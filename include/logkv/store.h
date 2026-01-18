#ifndef _LOGKV_STORE_H_
#define _LOGKV_STORE_H_

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <thread>
#include <vector>

#if defined(_WIN32) || defined(_WIN64)
#define LOGKV_WINDOWS 1
#else
#define LOGKV_WINDOWS 0
#endif

#if LOGKV_WINDOWS
#include <io.h>
#include <process.h>
#include <stdio.h>
#else
#include <sys/wait.h>
#include <unistd.h>
#endif

#include <logkv/autoser/bytes.h>
#include <logkv/crc.h>

#define IF_CONSTEXPR_REQUIRES_EXPR_EXPR(expr)                                  \
  if constexpr (requires { expr; }) {                                          \
    expr;                                                                      \
  }

namespace logkv {

/**
 * Store option flags.
 */
enum StoreFlags {
  none = 0,
  createDir = 1,  /// Creates directory if it doesn't exist.
  deleteData = 2, /// Deletes data in directory, if it exists.
  deferLoad = 4   /// Do not `load()` after setting the directory.
};

/**
 * Store save mode.
 */
enum StoreSaveMode {
  asyncClear = 0, // Write snapshot synchronously but clean up old files
                  // asynchronously (detached threads).
  syncSave = 1,   // Fully serial (synchronous) mode.
  forkSave = 2    // Fork the process to write new snapshot and clean up old
                  // files afterwards. If no POSIX, reverts to `threaded`.
};

/**
 * `logkv::Store` is a wrapper around any K,V container M that optionally logs
 * K,V mapping changes to an event log and knows how to load and save M
 * snapshots from and to persisted storage.
 *
 * Type V must have logkv::serializer<V> implemented; see `logkv/serializer.h`.
 * There is built-in serialization for several types; see `logkv/autoser.h`.
 *
 * When implementing a custom serializer, `logkv::Store` will notify the
 * serializer of snapshot events via `_logkvStoreSnapshot(bool)`; see
 * `logkv/partial.h` for a concrete example.
 *
 * NOTE: An absent key K is equivalent to a key K mapped to an empty value V.
 * `update()` keeps K,V mappings with an empty value V, but keys K with an empty
 * value V are _not_ stored in snapshots (`save()`).
 *
 * NOTE: To guarantee that a sequence of updates will be applied atomically,
 * they must all be written within the same frame (buffer flush cycle). There's
 * no transactional API to guarantee that, but `getBufferWriteRemaining()` plus
 * application knowledge of the serialization overhead of its own objects can be
 * used to decide when to `flush()` to start a new frame and guarantee the
 * following object writes will end up on the same frame and thus be atomic.
 */
template <template <typename...> class M, typename K, typename V> class Store {
public:
  using map_type = M<K, V>;
  using key_type = K;
  using mapped_type = V;
  using value_type = typename map_type::value_type;
  using iterator = typename map_type::iterator;
  using const_iterator = typename map_type::const_iterator;

  /**
   * Default internal buffer size in bytes (512KB).
   */
  static constexpr size_t DefaultBufferSize = 1 << 19;

  /**
   * Maximum size of a single object, frame, and internal buffer (512MB).
   */
  static constexpr size_t MaxBufferSize = 1 << 29;

  /**
   * Minimum payload size in bytes protected by CRC32 instead of CRC16.
   * CRC32 offers significantly better error protection than CRC16 (and faster
   * with hardware acceleration), but still it's 4 bytes of overhead vs 2.
   * For applications that flush tiny objects after each store change (e.g.
   * transactional systems) the extra bytes could be significant.
   * To instead force CRC32 on all frame writes, call `setForceCRC32(true)`.
   */
  static constexpr size_t MinCRC32PayloadSize = 512;

  /**
   * Constuct a Store that will operate in the given data directory.
   * If `StoreFlags::deferLoad` is specified without specifying both
   * `StoreFlags::createDir` and `StoreFlags::deleteData` as well, you must call
   * `load()` immediately afterwards.
   * @param dir Backing data directory for the store.
   * @param bufferSize Initial size of the I/O buffer (default: 1MB).
   * @param flags Store options (see `logkv::StoreFlags` enum).
   */
  Store(const std::string& dir, int flags = StoreFlags::none,
        size_t bufferSize = DefaultBufferSize)
      : flags_(flags), buffer_(bufferSize) {
    if (!logkv::serializer<mapped_type>::is_empty(emptyValue_)) {
      throw std::runtime_error(
        std::string("detected a non-empty default-constructed value for a "
                    "logkv::Store mapped type: ") +
        typeid(mapped_type).name());
    }
    IF_CONSTEXPR_REQUIRES_EXPR_EXPR(mapped_type::_logkvStoreSnapshot(false));
    setDirectory(dir);
  }

  /**
   * Destructor.
   * NOTE: Does NOT flush any pending events; `flush()` must be called
   * before the store object is destroyed to persist buffered events.
   */
  virtual ~Store() {
    if (events_) {
      closeFile(events_);
    }
  }

  /**
   * Get internal buffer size.
   * @return Buffer size.
   */
  size_t getBufferSize() const { return buffer_.size(); }

  /**
   * Resizes internal buffer.
   * @param size New size in bytes.
   */
  void setBufferSize(size_t size) {
    if (!size || size > MaxBufferSize) {
      throw std::runtime_error("invalid buffer size");
    }
    if (writeOffset_ > 0) {
      if (!events_) {
        throw std::runtime_error("event file handle is null");
      }
      writeFrame(events_);
    }
    buffer_.resize(size);
  }

  /**
   * Get internal buffer read offset.
   * @return Buffer read offset.
   */
  size_t getBufferReadOffset() const { return readOffset_; }

  /**
   * Get internal buffer write offset.
   * @return Buffer write offset.
   */
  size_t getBufferWriteOffset() const { return writeOffset_; }

  /**
   * Get remaining read capacity.
   * @return Remaining readable bytes in the buffer.
   */
  size_t getBufferReadRemaining() const { return writeOffset_ - readOffset_; }

  /**
   * Get remaining write capacity.
   * @return Remaining buffer space to write before it will auto-flush.
   */
  size_t getBufferWriteRemaining() const {
    return buffer_.size() - writeOffset_;
  }

  /**
   * Configure whether CRC32 should always be used.
   * @param force If true, all frames will use CRC32 regardless of size.
   * If false (default), CRC16 is used for frames smaller than 512 bytes.
   */
  void setForceCRC32(bool force) { forceCRC32_ = force; }

  /**
   * Get internal time counter.
   * @return Time counter.
   */
  uint64_t getTime() const { return time_; }

  /**
   * Get loaded status.
   * @return `true` if `load()` was already called for the current data
   * directory, `false` otherwise.
   */
  bool isLoaded() const { return loaded_; }

  /**
   * Get the data directory.
   * @return The data directory.
   */
  std::string getDirectory() const { return dir_; }

  /**
   * Change the current working directory. Loads the data in the directory
   * unless `StoreFlags::deferLoad` is set.
   * NOTE: If `StoreFlags::deferLoad` was set, you must call `load()`
   * immediately afterwards, unless both `StoreFlags::createDir` and
   * `StoreFlags::deleteData` were set.
   * @param dir Backing data directory for the store.
   */
  void setDirectory(const std::string& dir) {
    if (dir == dir_) {
      return;
    }
    bool exists = std::filesystem::exists(dir);
    bool isDir = std::filesystem::is_directory(dir);
    if (exists && !isDir) {
      throw std::runtime_error("directory path is not a directory");
    }
    if (exists && isDir) {
      closeEventsFile();
      dir_ = dir;
      if (flags_ & StoreFlags::deleteData) {
        for (const auto& entry : std::filesystem::directory_iterator(dir)) {
          if (entry.is_regular_file()) {
            const auto& path = entry.path();
            auto ext = path.extension();
            auto stem = path.stem().string();
            if ((ext == ".events" || ext == ".snapshot") &&
                std::all_of(stem.begin(), stem.end(), ::isdigit)) {
              std::filesystem::remove(path);
            }
          }
        }
        load();
      } else if (!(flags_ & StoreFlags::deferLoad)) {
        load();
      } else {
        loaded_ = false;
      }
    } else {
      if (flags_ & StoreFlags::createDir) {
        if (!std::filesystem::create_directories(dir)) {
          throw std::runtime_error("cannot create directory");
        }
        closeEventsFile();
        dir_ = dir;
        load();
      } else {
        throw std::runtime_error("directory not found");
      }
    }
  }

  /**
   * Get the underlying K,V map.
   * The store's `objects_` field can and should be modified directly
   * whenever it is not economical to write the object updates to disk.
   * To later persist all so far unlogged updates, call `save()`.
   * @return Reference to the underlying K,V map.
   */
  map_type& operator()() { return objects_; }

  /**
   * Get the underlying K,V map.
   * The store's `objects_` field can and should be modified directly
   * whenever it is not economical to write the object updates to disk.
   * To later persist all so far unlogged updates, call `save()`.
   * @return Reference to the underlying K,V map.
   */
  map_type& getObjects() { return objects_; }

  /**
   * Forward `operator[]` to backing K,V map for convenience.
   * @param key Key to access
   */
  mapped_type& operator[](const key_type& key) { return objects_[key]; }

  /**
   * Update a K,V mapping, writing an event to the events log.
   * @param key Key to set
   * @param value Value to associate with the given key
   */
  void update(const key_type& key, const mapped_type& value) {
    writeUpdate(events_, key, value);
    objects_[key] = value;
  }

  /**
   * Erase a K,V mapping, writing an event to the events log.
   * @param key Key to erase
   */
  void erase(const key_type& key) {
    auto it = objects_.find(key);
    if (it != objects_.end()) {
      writeErase(events_, key);
      objects_.erase(it);
    }
  }

  /**
   * Iterator support.
   */
  iterator find(const key_type& key) { return objects_.find(key); }
  const_iterator find(const key_type& key) const { return objects_.find(key); }
  iterator begin() { return objects_.begin(); }
  iterator end() { return objects_.end(); }
  const_iterator begin() const { return objects_.begin(); }
  const_iterator end() const { return objects_.end(); }
  const_iterator cbegin() const { return objects_.cbegin(); }
  const_iterator cend() const { return objects_.cend(); }

  /**
   * Update a K,V mapping, writing an event to the events log.
   * @param it Pointer to the entry to modify
   * @param value Value to write at the entry
   */
  void update(iterator it, const mapped_type& value) {
    writeUpdate(events_, it->first, value);
    it->second = value;
  }

  /**
   * Erase the key at the iterator and write an event in the events log.
   * @param it The entry to erase.
   */
  iterator erase(iterator it) {
    writeErase(events_, it->first);
    return objects_.erase(it);
  }

  /**
   * Write an event in the events log for the given K,V entry.
   * The caller has already modified the value for the key in place.
   * @param it The entry to persist.
   */
  void persist(iterator it) { writeUpdate(events_, it->first, it->second); }

  /**
   * Flush any buffered writes to the events file.
   * @param sync `true` to commit to disk, `false` otherwise.
   */
  void flush(bool sync = false) { flush(events_, sync); }

  /**
   * Clear the underlying K,V map, saving an empty snapshot.
   */
  void clear() {
    objects_.clear();
    save(StoreSaveMode::syncSave);
  }

  /**
   * Load state from the store's directory, discarding any current state.
   * Loads most recent snapshot, if any. Replays relevant event log, if any.
   * @return `false` if latest events file was corrupted, `true` otherwise.
   * @throws std::runtime_error if corrupted snapshot or a filesystem error.
   */
  bool load() {
    std::vector<std::filesystem::path> snapshots;
    for (const auto& entry : std::filesystem::directory_iterator(dir_)) {
      if (!entry.is_regular_file()) {
        continue;
      }
      auto stem = entry.path().stem().string();
      if (entry.path().extension() == ".snapshot" &&
          std::all_of(stem.begin(), stem.end(), ::isdigit)) {
        snapshots.push_back(entry.path());
      }
    }
    FILE* sf = nullptr;
    if (!snapshots.empty()) {
      std::sort(snapshots.begin(), snapshots.end());
      auto last = snapshots.back();
      time_ = std::stoull(last.stem().string());
      sf = fopen(last.string().c_str(), "rb");
      if (!sf) {
        throw std::runtime_error("cannot open snapshot file for reading");
      }
    }
    objects_.clear();
    if (sf) {
      IF_CONSTEXPR_REQUIRES_EXPR_EXPR(mapped_type::_logkvStoreSnapshot(true));
      bool ok = replay(sf);
      IF_CONSTEXPR_REQUIRES_EXPR_EXPR(mapped_type::_logkvStoreSnapshot(false));
      closeFile(sf);
      if (!ok) {
        throw std::runtime_error("corrupted snapshot");
      }
    } else {
      time_ = 0;
    }
    uint64_t expectedTime = time_;
    std::vector<uint64_t> eventTimes;
    for (const auto& entry : std::filesystem::directory_iterator(dir_)) {
      if (!entry.is_regular_file())
        continue;
      auto path = entry.path();
      if (path.extension() != ".events")
        continue;
      auto stem = path.stem().string();
      if (!std::all_of(stem.begin(), stem.end(), ::isdigit))
        continue;
      uint64_t fileNum = std::stoull(stem);
      if (fileNum >= time_) {
        eventTimes.push_back(fileNum);
      }
    }
    std::sort(eventTimes.begin(), eventTimes.end());
    bool corrupted = false;
    for (uint64_t eventTime : eventTimes) {
      if (eventTime != expectedTime) {
        corrupted = true;
      }
      auto eventsPath =
        std::filesystem::path(dir_) / (pad(eventTime) + ".events");
      FILE* ef = fopen(eventsPath.string().c_str(), "rb");
      if (!ef) {
        throw std::runtime_error("cannot open events file for reading");
      } else {
        bool replayOk = replay(ef);
        closeFile(ef);
        if (!replayOk) {
          std::filesystem::remove(eventsPath);
          corrupted = true;
        } else {
          time_ = eventTime;
        }
      }
      expectedTime = eventTime + 1;
    }
    loaded_ = true;
    if (corrupted) {
      save(StoreSaveMode::syncSave);
    }
    closeEventsFile();
    openEventsFile();
    return !corrupted;
  }

  /**
   * Save state to the store's directory.
   * Deletes any and all snapshots and event logs and writes a fresh snapshot.
   * @param mode Save mode (see `logkv::StoreSaveMode` enum).
   * @throws std::exception on any filesystem, write or serialization error.
   */
  int save(int mode = StoreSaveMode::syncSave) {
    if (!loaded_) {
      throw std::runtime_error("cannot save() without calling load() first");
    }
    if (events_) {
      fclose(events_);
      events_ = nullptr;
    }
#if !LOGKV_WINDOWS
    if (mode == StoreSaveMode::forkSave) {
      int status;
      while (waitpid(-1, &status, WNOHANG) > 0) {
      }
      uint64_t snapshotTime = time_ + 1;
      pid_t pid = fork();
      if (pid == -1) {
        throw std::runtime_error("fork() failed");
      } else if (pid == 0) { // Child
        try {
          writeSnapshot(snapshotTime);
          deleteOldSnapshotsAndEvents(snapshotTime);
        } catch (...) {
          _exit(1);
        }
        _exit(0);
      } else { // Parent
        time_ = snapshotTime;
        openEventsFile();
        return pid;
      }
    }
#endif

    uint64_t snapshotTime = time_ + 1;
    writeSnapshot(snapshotTime);
    time_ = snapshotTime;
    openEventsFile();
    if (mode == StoreSaveMode::syncSave) {
      deleteOldSnapshotsAndEvents(snapshotTime);
    } else {
      std::thread([this, snapshotTime]() {
        deleteOldSnapshotsAndEvents(snapshotTime);
      }).detach();
    }
    return 0;
  }

private:
  map_type objects_;
  FILE* events_ = nullptr;
  int flags_ = StoreFlags::none;
  std::vector<char> buffer_;
  size_t writeOffset_ = 0;
  size_t readOffset_ = 0;
  bool forceCRC32_ = false;
  bool loaded_ = false;
  uint64_t time_ = 0;
  std::string dir_;
  mapped_type emptyValue_{};

  enum ReadResult {
    RR_Success = 0,
    RR_Frame_EOF = 1,
    RR_Frame_Underflow = 2,
    RR_Frame_Corrupted = 3,
    RR_Object_Corrupted = 4
  };

  std::string pad(uint64_t n) {
    std::ostringstream oss;
    oss << std::setw(20) << std::setfill('0') << n;
    return oss.str();
  }

  void flush(FILE* f, bool sync = false) {
    writeFrame(f);
    if (sync) {
#if LOGKV_WINDOWS
      _commit(_fileno(f));
#else
      fsync(fileno(f));
#endif
    }
  }
  void closeFile(FILE* f) {
    if (fclose(f) != 0) {
      throw std::runtime_error("cannot close file");
    }
  }

  void closeEventsFile() {
    if (events_) {
      flush(events_, true);
      closeFile(events_);
      events_ = nullptr;
    }
  }

  void openEventsFile() {
    writeOffset_ = 0;
    auto eventsPath = std::filesystem::path(dir_) / (pad(time_) + ".events");
    events_ = fopen(eventsPath.string().c_str(), "ab+");
    if (!events_) {
      throw std::runtime_error("cannot open events file for writing");
    }
  }

  void writeSnapshot(uint64_t snapshotTime) {
    auto snapshotStem = pad(snapshotTime);
    auto snapshotName = snapshotStem + ".snapshot";
    auto snapshotPath = std::filesystem::path(dir_) / snapshotName;
    std::ostringstream tempNameStream;
    uint64_t nanosEpoch = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    int pid = 0;
#if LOGKV_WINDOWS
    pid = _getpid();
#else
    pid = getpid();
#endif
    tempNameStream << "tmp_snapshot_" << pid << "_" << nanosEpoch << "_"
                   << snapshotStem;
    auto tempPath = std::filesystem::path(dir_) / tempNameStream.str();
    FILE* sf = fopen(tempPath.string().c_str(), "wb");
    if (!sf) {
      throw std::runtime_error("cannot open temp snapshot file for writing");
    }
    writeOffset_ = 0;
    try {
      IF_CONSTEXPR_REQUIRES_EXPR_EXPR(mapped_type::_logkvStoreSnapshot(true));
      for (auto& [k, v] : objects_) {
        writeUpdate(sf, k, v);
      }
      IF_CONSTEXPR_REQUIRES_EXPR_EXPR(mapped_type::_logkvStoreSnapshot(false));
      flush(sf, true);
    } catch (std::exception& ex) {
      closeFile(sf);
      try {
        std::filesystem::remove(tempPath);
      } catch (...) {
      }
      throw ex;
    }
    closeFile(sf);
    try {
      std::filesystem::rename(tempPath, snapshotPath);
    } catch (const std::exception& e) {
      try {
        std::filesystem::remove(tempPath);
      } catch (...) {
      }
      throw std::runtime_error(std::string("failed to rename snapshot: ") +
                               e.what());
    }
  }

  void deleteOldSnapshotsAndEvents(uint64_t keepSnapshotTime) {
    auto snapshotStem = pad(keepSnapshotTime);
    std::vector<std::filesystem::path> toDelete;
    for (const auto& entry : std::filesystem::directory_iterator(dir_)) {
      if (!entry.is_regular_file())
        continue;
      const auto& path = entry.path();
      auto stem = path.stem().string();
      if (!std::all_of(stem.begin(), stem.end(), ::isdigit))
        continue;
      auto ext = path.extension().string();
      uint64_t fileNum = std::stoull(stem);
      if (ext == ".events") {
        if (fileNum < keepSnapshotTime) {
          toDelete.push_back(path);
        }
      } else if (ext == ".snapshot") {
        if (fileNum < keepSnapshotTime) {
          toDelete.push_back(path);
        }
      }
    }
    for (const auto& p : toDelete) {
      try {
        std::filesystem::remove(p);
      } catch (...) {
      }
    }
  }

  void writeFrame(FILE* f) {
    if (writeOffset_ == 0)
      return;
    const uint32_t payloadSize = static_cast<uint32_t>(writeOffset_);
    char headerBuf[8];
    size_t headerIdx = 1;
    /**
     * First byte is the control byte:
     * Bits 0-4: first 5 bits of the frame size.
     * Bit 5: 1 = CRC32 (4 bytes), 0 = CRC16 (2 bytes).
     * Bits 6-7: extra size bytes (0 to 3 bytes).
     */
    uint8_t control = payloadSize & 0x1F;
    uint32_t extra = payloadSize >> 5;
    bool isCRC32 = forceCRC32_ || (payloadSize >= MinCRC32PayloadSize);
    if (isCRC32) {
      control |= 0x20;
    }
    if (extra > 0) {
      if (extra <= 0xFF) {
        control |= 0x40; // 01 (1 extra byte)
        headerBuf[headerIdx++] = static_cast<char>(extra);
      } else if (extra <= 0xFFFF) {
        control |= 0x80; // 10 (2 extra bytes)
        uint16_t e = static_cast<uint16_t>(extra);
        std::memcpy(headerBuf + headerIdx, &e, 2);
        headerIdx += 2;
      } else {
        control |= 0xC0; // 11 (3 extra bytes)
        std::memcpy(headerBuf + headerIdx, &extra, 3);
        headerIdx += 3;
      }
    }
    headerBuf[0] = control;
    if (isCRC32) {
      uint32_t checksum = logkv::computeCRC32(buffer_.data(), payloadSize);
      std::memcpy(headerBuf + headerIdx, &checksum, 4);
      headerIdx += 4;
    } else {
      uint16_t checksum = logkv::computeCRC16(buffer_.data(), payloadSize);
      std::memcpy(headerBuf + headerIdx, &checksum, 2);
      headerIdx += 2;
    }
    if (fwrite(headerBuf, 1, headerIdx, f) != headerIdx ||
        fwrite(buffer_.data(), 1, payloadSize, f) != payloadSize ||
        fflush(f) != 0) {
      throw std::runtime_error("file write error");
    }
    writeOffset_ = 0;
  }

  /**
   * Reads a frame from disk into the buffer and verifies integrity.
   * Returns a `ReadResult` enum value, where 0 is success and any
   * non-zero indicates failure.
   */
  int readFrame(FILE* f) {
    uint8_t control = 0;
    if (fread(&control, 1, 1, f) != 1) {
      return RR_Frame_EOF;
    }
    const int extraLenBytes = (control >> 6) & 0x03;
    const bool isCRC32 = (control & 0x20);
    const int crcBytes = isCRC32 ? 4 : 2;
    const int remainingHeaderSize = extraLenBytes + crcBytes;
    char headerBuf[8];
    if (fread(headerBuf, 1, remainingHeaderSize, f) !=
        (size_t)remainingHeaderSize) {
      return RR_Frame_Underflow; // truncated frame header
    }
    uint32_t payloadSize = (control & 0x1F);
    if (extraLenBytes > 0) {
      uint32_t extraValue = 0;
      std::memcpy(&extraValue, headerBuf, extraLenBytes);
      payloadSize |= (extraValue << 5);
    }
    uint32_t diskCRC = 0;
    std::memcpy(&diskCRC, headerBuf + extraLenBytes, crcBytes);
    if (buffer_.size() < payloadSize) {
      buffer_.resize(payloadSize);
    }
    if (fread(buffer_.data(), 1, payloadSize, f) != payloadSize) {
      return RR_Frame_Underflow; // truncated frame payload
    }
    if (isCRC32) {
      uint32_t calcCRC = logkv::computeCRC32(buffer_.data(), payloadSize);
      if (calcCRC != diskCRC) {
        return RR_Frame_Corrupted;
      }
    } else {
      uint16_t calcCRC = logkv::computeCRC16(buffer_.data(), payloadSize);
      if (calcCRC != static_cast<uint16_t>(diskCRC)) {
        return RR_Frame_Corrupted;
      }
    }
    writeOffset_ = payloadSize;
    readOffset_ = 0;
    return RR_Success;
  }

  /**
   * Returns a `ReadResult` enum value, where 0 is success and any
   * non-zero indicates failure.
   * Throwing anywhere in here means a corrupted file, and depending
   * on the code returned it can also mean a corrupted file.
   */
  template <typename T> int readObject(FILE* f, T& out) {
    if (readOffset_ >= writeOffset_) {
      int rf = readFrame(f);
      if (rf != RR_Success) {
        return rf;
      }
    }
    const char* inptr = buffer_.data() + readOffset_;
    size_t avail = writeOffset_ - readOffset_;
    size_t used = logkv::serializer<T>::read(inptr, avail, out);
    if (used > avail) {
      // Broken object deserializer code trying to read beyond its frame
      return RR_Object_Corrupted;
    }
    readOffset_ += used;
    return RR_Success;
  }

  template <typename T> size_t writeObject(FILE* f, const T& obj) {
    size_t avail = buffer_.size() - writeOffset_;
    size_t used =
      logkv::serializer<T>::write(buffer_.data() + writeOffset_, avail, obj);
    if (used > avail) {
      writeFrame(f);
      if (buffer_.size() < used) {
        size_t targetSz = buffer_.size() * 2;
        while (targetSz < used) {
          targetSz *= 2;
        }
        buffer_.resize(targetSz);
      }
      return writeObject(f, obj);
    }
    writeOffset_ += used;
    return used;
  }

  bool replay(FILE* f) {
    writeOffset_ = 0;
    readOffset_ = 0;
    try {
      if (fseek(f, 0, SEEK_SET) != 0) {
        return false;
      }
      while (true) {
        if (readOffset_ >= writeOffset_) {
          // buffer empty; read next frame
          int rf = readFrame(f);
          if (rf == RR_Frame_EOF) {
            break; // EOF reached cleanly between objects; replay done.
          } else if (rf != RR_Success) {
            return false;
          }
        }
        key_type key;
        if (readObject(f, key) != RR_Success) {
          return false;
        }
        auto it = objects_.find(key);
        if (it != objects_.end()) {
          if (readObject(f, it->second) != RR_Success) {
            return false;
          }
          if (logkv::serializer<mapped_type>::is_empty(it->second)) {
            objects_.erase(it);
          }
        } else {
          mapped_type value;
          if (readObject(f, value) != RR_Success) {
            return false;
          }
          if (!logkv::serializer<mapped_type>::is_empty(value)) {
            objects_[std::move(key)] = std::move(value);
          }
        }
      }
    } catch (...) {
      return false;
    }
    return true;
  }

  void writeUpdate(FILE* f, const key_type& key, const mapped_type& value) {
    writeObject(f, key);
    writeObject(f, value);
  }

  void writeErase(FILE* f, const key_type& key) {
    writeUpdate(f, key, emptyValue_);
  }
};

} // namespace logkv

#endif