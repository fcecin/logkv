#ifndef _LOGKV_H_
#define _LOGKV_H_

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <thread>
#include <vector>

#include "bytes.h"

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
 * `logkv::Store` is a wrapper around any K,V container M that optionally logs
 * K,V mapping changes to an event log and knows how to load and save M
 * snapshots from and to persisted storage.
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
  V emptyValue_;

  static constexpr size_t defaultBufferSize = 1 << 19;

  std::string pad(uint64_t n) {
    std::ostringstream oss;
    oss << std::setw(20) << std::setfill('0') << n;
    return oss.str();
  }

  void flush(FILE* f) {
    if (writeOffset_ > 0) {
      bool err = fwrite(buffer_.data(), 1, writeOffset_, f) != writeOffset_ ||
                 fflush(f) != 0;
      writeOffset_ = 0;
      if (err) {
        throw std::runtime_error("file write error");
      }
    }
  }

  void closeFile(FILE* f) {
    if (fclose(f) != 0) {
      throw std::runtime_error("cannot close file");
    }
  }

  void closeEventsFile() {
    if (events_) {
      flush(events_);
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

  template <typename T> size_t readObject(FILE* f, T& out) {
    const char* inptr = buffer_.data() + readOffset_;
    size_t avail = writeOffset_ - readOffset_;
    size_t used = out.deserialize(inptr, avail);
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
    size_t used = obj.serialize(buffer_.data() + writeOffset_, avail);
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
        if (value.empty()) {
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
    if (!emptyValue_.empty()) {
      throw std::runtime_error("default-constructed value type is not empty()");
    }
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
    objects_[key] = value;
    writeUpdate(events_, key, value);
  }

  /**
   * Erase a K,V mapping, writing an event to the events log.
   * @param key Key to erase
   */
  void erase(const K& key) {
    auto it = objects_.find(key);
    if (it != objects_.end()) {
      objects_.erase(it);
      writeErase(events_, key);
    }
  }

  /**
   * Flush any buffered writes to the events file.
   */
  void flush() { flush(events_); }

  /**
   * Clear the underlying K,V map, saving an empty snapshot.
   */
  void clear() {
    objects_.clear();
    save();
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
    if (sf) {
      objects_.clear();
      if (!replay(sf)) {
        closeFile(sf);
        throw std::runtime_error("corrupted snapshot");
      }
      closeFile(sf);
    } else {
      time_ = 0;
    }
    auto eventsPath = std::filesystem::path(dir_) / (pad(time_) + ".events");
    FILE* ef = nullptr;
    if (std::filesystem::exists(eventsPath) &&
        std::filesystem::is_regular_file(eventsPath)) {
      ef = fopen(eventsPath.string().c_str(), "rb");
      if (!ef) {
        throw std::runtime_error("cannot open events file for reading");
      }
    }
    if (snapshots.empty()) {
      objects_.clear();
    }
    loaded_ = true;
    if (ef) {
      if (!replay(ef)) {
        closeFile(ef);
        std::filesystem::remove(eventsPath);
        save();
        return false;
      } else {
        closeFile(ef);
      }
    }
    closeEventsFile();
    openEventsFile();
    return true;
  }

  /**
   * Save state to the store's directory.
   * Deletes any and all snapshots and event logs and writes a fresh snapshot.
   * @param sync `true` for synchronous file deletion, `false` otherwise.
   * @throws std::exception on any filesystem, write or serialization error.
   */
  void save(bool sync = true) {
    if (!loaded_) {
      throw std::runtime_error("cannot save() without calling load() first");
    }
    auto snapshotStem = pad(time_ + 1);
    auto snapshotName = snapshotStem + ".snapshot";
    auto snapshotPath = std::filesystem::path(dir_) / snapshotName;
    FILE* sf = fopen(snapshotPath.string().c_str(), "wb");
    if (!sf) {
      throw std::runtime_error("cannot open snapshot file for writing");
    }
    writeOffset_ = 0;
    try {
      for (auto& [k, v] : objects_) {
        writeUpdate(sf, k, v);
      }
      flush(sf);
    } catch (std::exception& ex) {
      closeFile(sf);
      try {
        std::filesystem::remove(snapshotPath);
      } catch (...) {
      }
      throw ex;
    }
    ++time_;
    closeFile(sf);
    closeFile(events_);
    events_ = nullptr;
    std::vector<std::filesystem::path> toDelete;
    for (const auto& entry : std::filesystem::directory_iterator(dir_)) {
      if (!entry.is_regular_file()) {
        continue;
      }
      const auto& path = entry.path();
      auto stem = path.stem().string();
      if (!std::all_of(stem.begin(), stem.end(), ::isdigit)) {
        continue;
      }
      auto ext = path.extension().string();
      if (ext == ".events") {
        toDelete.push_back(path);
      } else if (ext == ".snapshot" && stem != snapshotStem) {
        toDelete.push_back(path);
      }
    }
    if (!toDelete.empty()) {
      auto deleteFiles =
        [](const std::vector<std::filesystem::path>& pathsToDelete) {
          for (const auto& p : pathsToDelete) {
            try {
              std::filesystem::remove(p);
            } catch (...) {
            }
          }
        };
      if (sync) {
        deleteFiles(toDelete);
      } else {
        std::thread(deleteFiles, std::move(toDelete)).detach();
      }
    }
    openEventsFile();
  }
};

} // namespace logkv

#endif