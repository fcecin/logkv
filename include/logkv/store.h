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
 * NOTE: An absent key K is equivalent to a key K mapped to an empty value V.
 * `update()` keeps K,V mappings with an empty value V, but keys K with an empty
 * value V are _not_ stored in snapshots (`save()`).
 */
template <template <typename, typename> class M, typename K, typename V>
class Store {
private:
  M<K, V> objects_;
  FILE* events_ = nullptr;
  int flags_ = StoreFlags::none;
  Bytes buffer_;
  size_t writeOffset_ = 0;
  size_t readOffset_ = 0;
  bool loaded_ = false;
  uint64_t time_ = 0;
  std::string dir_;
  V emptyValue_{};

  static constexpr size_t defaultBufferSize = 1 << 19;

  std::string pad(uint64_t n) {
    std::ostringstream oss;
    oss << std::setw(20) << std::setfill('0') << n;
    return oss.str();
  }

  void flush(FILE* f, bool sync = false) {
    if (writeOffset_ > 0) {
      bool err = fwrite(buffer_.data(), 1, writeOffset_, f) != writeOffset_ ||
                 fflush(f) != 0;
      writeOffset_ = 0;
      if (err) {
        throw std::runtime_error("file write error");
      }
    }
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
      IF_CONSTEXPR_REQUIRES_EXPR_EXPR(V::_logkvStoreSnapshot(true));
      for (auto& [k, v] : objects_) {
        writeUpdate(sf, k, v);
      }
      IF_CONSTEXPR_REQUIRES_EXPR_EXPR(V::_logkvStoreSnapshot(false));
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

  template <typename T> size_t readObject(FILE* f, T& out) {
    const char* inptr = buffer_.data() + readOffset_;
    size_t avail = writeOffset_ - readOffset_;
    size_t used = logkv::serializer<T>::read(inptr, avail, out);
    bool underflow = used > avail;
    if (underflow) {
      if (buffer_.size() < used) {
        size_t targetSz = buffer_.size() * 2;
        while (targetSz < used) {
          targetSz *= 2;
        }
        Bytes newbuf(targetSz);
        std::memmove(newbuf.data(), buffer_.data() + readOffset_, avail);
        buffer_ = std::move(newbuf);
      } else {
        if (avail > 0) {
          std::memmove(buffer_.data(), buffer_.data() + readOffset_, avail);
        }
      }
      readOffset_ = 0;
      writeOffset_ = avail;
      size_t r = fread(buffer_.data() + avail, 1, buffer_.size() - avail, f);
      if (r == 0) {
        throw std::runtime_error("file read error");
      }
      writeOffset_ += r;
      return readObject(f, out);
    }
    readOffset_ += used;
    return used;
  }

  template <typename T> size_t writeObject(FILE* f, const T& obj) {
    size_t avail = buffer_.size() - writeOffset_;
    size_t used =
      logkv::serializer<T>::write(buffer_.data() + writeOffset_, avail, obj);
    bool overflow = used > avail;
    if (overflow) {
      if (writeOffset_ > 0) {
        bool err = fwrite(buffer_.data(), 1, writeOffset_, f) != writeOffset_;
        writeOffset_ = 0;
        if (err) {
          throw std::runtime_error("file write error");
        }
      }
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
      long totalBytesRead = 0;
      long fsize;
      if (fseek(f, 0, SEEK_END) != 0 || (fsize = ftell(f)) == -1 ||
          fseek(f, 0, SEEK_SET) != 0) {
        return false;
      }
      while (totalBytesRead < fsize) {
        K key;
        totalBytesRead += readObject(f, key);
        V value;
        totalBytesRead += readObject(f, value);
        if (logkv::serializer<V>::is_empty(value)) {
          objects_.erase(std::move(key));
        } else {
          objects_[std::move(key)] = std::move(value);
        }
      }
    } catch (...) {
      return false;
    }
    return true;
  }

  void writeUpdate(FILE* f, const K& key, const V& value) {
    writeObject(f, key);
    writeObject(f, value);
  }

  void writeErase(FILE* f, const K& key) { writeUpdate(f, key, emptyValue_); }

public:
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
        size_t bufferSize = defaultBufferSize)
      : flags_(flags), buffer_(bufferSize) {
    if (!logkv::serializer<V>::is_empty(emptyValue_)) {
      throw std::runtime_error(
        std::string("detected a non-empty default-constructed value for a "
                    "logkv::Store mapped type: ") +
        typeid(V).name());
    }
    IF_CONSTEXPR_REQUIRES_EXPR_EXPR(V::_logkvStoreSnapshot(false));
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
  M<K, V>& operator()() { return objects_; }

  /**
   * Get the underlying K,V map.
   * The store's `objects_` field can and should be modified directly
   * whenever it is not economical to write the object updates to disk.
   * To later persist all so far unlogged updates, call `save()`.
   * @return Reference to the underlying K,V map.
   */
  M<K, V>& getObjects() { return objects_; }

  /**
   * Forward `operator[]` to backing K,V map for convenience.
   * @param key Key to access
   */
  V& operator[](const K& key) { return objects_[key]; }

  /**
   * Update a K,V mapping, writing an event to the events log.
   * @param key Key to set
   * @param value Value to associate with the given key
   */
  void update(const K& key, const V& value) {
    writeUpdate(events_, key, value);
    objects_[key] = value;
  }

  /**
   * Erase a K,V mapping, writing an event to the events log.
   * @param key Key to erase
   */
  void erase(const K& key) {
    auto it = objects_.find(key);
    if (it != objects_.end()) {
      writeErase(events_, key);
      objects_.erase(it);
    }
  }

  /**
   * Iterator support.
   */
  using iterator = typename M<K, V>::iterator;
  using const_iterator = typename M<K, V>::const_iterator;
  iterator find(const K& key) { return objects_.find(key); }
  const_iterator find(const K& key) const { return objects_.find(key); }
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
  void update(iterator it, const V& value) {
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
      if (!replay(sf)) {
        closeFile(sf);
        throw std::runtime_error("corrupted snapshot");
      }
      closeFile(sf);
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
        if (!replay(ef)) {
          closeFile(ef);
          std::filesystem::remove(eventsPath);
          corrupted = true;
        } else {
          time_ = eventTime;
        }
        closeFile(ef);
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
        if (events_) {
          fclose(events_);
          events_ = nullptr;
        }
        try {
          writeSnapshot(snapshotTime);
          deleteOldSnapshotsAndEvents(snapshotTime);
        } catch (...) {
          _exit(1);
        }
        _exit(0);
      } else { // Parent
        time_ = snapshotTime;
        closeFile(events_);
        events_ = nullptr;
        openEventsFile();
        return pid;
      }
    }
#endif

    uint64_t snapshotTime = time_ + 1;
    writeSnapshot(snapshotTime);
    time_ = snapshotTime;
    closeFile(events_);
    events_ = nullptr;
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
};

} // namespace logkv

#endif