#include <atomic>
#include <cassert>
#include <filesystem>
#include <iostream>
#include <map>
#include <thread>
#include <vector>

#include "logkv/autoser.h"
#include "logkv/store.h"

namespace fs = std::filesystem;

std::atomic<bool> g_thread_inside_serialization{false};
std::atomic<bool> g_main_thread_verification_done{false};
std::atomic<bool> g_test_failed{false};

struct SerializationHook {
  int dummy = 0;
};

struct MyObject : public logkv::AutoSerializableObject<MyObject> {
  int id = 0;
  std::string name;
  SerializationHook hook;

  static thread_local bool is_snapshotting;

  static void _logkvStoreSnapshot(bool active) { is_snapshotting = active; }

  AUTO_SERIALIZABLE_MEMBERS(id, name, hook)
};

thread_local bool MyObject::is_snapshotting = false;

namespace logkv {
template <> struct serializer<SerializationHook> {
  static size_t get_size(const SerializationHook&) { return 1; }

  static bool is_empty(const SerializationHook& v) { return v.dummy == 0; }

  static size_t read(const char*, size_t, SerializationHook&) { return 1; }

  static size_t write(char* dest, size_t size, const SerializationHook&) {
    if (size < 1)
      return 1;

    if (MyObject::is_snapshotting) {
      g_thread_inside_serialization = true;

      std::cout << "[Thread " << std::this_thread::get_id()
                << "] Pausing inside serializer... waiting for verification.\n";

      while (!g_main_thread_verification_done) {
        std::this_thread::yield();
      }
    }

    dest[0] = 0xAA;
    return 1;
  }
};
} // namespace logkv

int main() {
  const std::string storeDir = "test_data_store";

  if (fs::exists(storeDir)) {
    fs::remove_all(storeDir);
  }

  std::cout << ">>> Starting Thread-Local Snapshot Isolation Test <<<\n";

  std::thread worker([&]() {
    try {
      logkv::Store<std::map, int, MyObject> store(storeDir,
                                                  logkv::StoreFlags::createDir);

      MyObject obj;
      obj.id = 1;
      obj.name = "TestObject";
      store.update(1, obj);

      std::cout << "[Worker] Calling save()...\n";
      store.save(logkv::StoreSaveMode::syncSave);
      std::cout << "[Worker] Save completed.\n";

      if (MyObject::is_snapshotting) {
        std::cerr << "ERROR: Worker thread still thinks it is snapshotting!\n";
        g_test_failed = true;
      }

    } catch (const std::exception& e) {
      std::cerr << "Worker Exception: " << e.what() << "\n";
      g_test_failed = true;
    }
  });

  std::cout << "[Main] Waiting for worker to enter serialization...\n";

  while (!g_thread_inside_serialization && !g_test_failed) {
    std::this_thread::yield();
  }

  if (g_test_failed) {
    std::cout << "[Main] Worker failed before serialization. Aborting.\n";
    g_main_thread_verification_done = true;
    worker.join();
    return 1;
  }

  std::cout
    << "[Main] Worker is inside save(). Checking Main thread isolation...\n";

  if (MyObject::is_snapshotting == true) {
    std::cerr
      << "FAIL: Main thread sees is_snapshotting == true! Isolation broken.\n";
    g_test_failed = true;
  } else {
    std::cout << "[Main] SUCCESS: Main thread sees is_snapshotting == false.\n";
  }

  g_main_thread_verification_done = true;
  worker.join();

  if (fs::exists(storeDir)) {
    fs::remove_all(storeDir);
  }

  if (g_test_failed) {
    std::cout << ">>> TEST FAILED <<<\n";
    return 1;
  }

  std::cout << ">>> TEST PASSED <<<\n";
  return 0;
}