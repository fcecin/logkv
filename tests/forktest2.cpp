#include <chrono>
#include <filesystem>
#include <iostream>
#include <logkv/store.h>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

// Define a simple map alias
template <typename K, typename V> using StdMap = std::unordered_map<K, V>;

using namespace logkv;
namespace fs = std::filesystem;

std::string pad_num(uint64_t n) {
  std::ostringstream oss;
  oss << std::setw(20) << std::setfill('0') << n;
  return oss.str();
}

void backup_event_file(const std::string& dir, uint64_t time) {
  auto filename = pad_num(time) + ".events";
  auto src = fs::path(dir) / filename;
  auto dst = fs::path(dir) / (filename + ".bak");

  if (fs::exists(src)) {
    fs::copy_file(src, dst, fs::copy_options::overwrite_existing);
    std::cout << "    [Backup] Preserved " << filename << std::endl;
  } else {
    std::cerr << "    [Error] Could not find " << filename << " to backup!"
              << std::endl;
  }
}

int main() {
  std::cout << "--- LogKV Multi-Fork & Chain Replay Test ---" << std::endl;

#if LOGKV_WINDOWS
  std::cout << "[SKIP] Test targets Linux/Fork chain replay behavior."
            << std::endl;
  return 0;
#else

  const std::string dirStr = "./forktest2data";

  // Cleanup previous run
  if (fs::exists(dirStr))
    fs::remove_all(dirStr);

  std::cout << "[1] Starting Store..." << std::endl;
  {
    Store<StdMap, std::string, std::string> store(
      dirStr, StoreFlags::createDir | StoreFlags::deleteData);

    // --- CYCLE 0 ---
    std::cout << "[2] Cycle 0: Writing Key 0..." << std::endl;
    store.update("key0", "val0");

    // IMPORTANT: Flush to disk so we can back up the file before save() (which
    // forks and deletes it)
    store.flush(true);
    backup_event_file(dirStr, 0);

    std::cout << "    Calling save() (0 -> 1)..." << std::endl;
    int pid;
    pid =
      store.save(StoreSaveMode::forkSave); // Forks. Child tries to snapshot
                                           // 0->1. Parent moves to 1.events.
    std::cout << "    save() forked child pid is " << pid << std::endl;
    if (pid <= 0) {
      std::cerr << "FAIL: save() pid <= 0; not fork." << std::endl;
      return 1;
    }
    // --- CYCLE 1 ---
    // Sleep briefly to ensure parent has rotated files, though save() is
    // immediate.
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    std::cout << "[3] Cycle 1: Writing Key 1..." << std::endl;
    store.update("key1", "val1");

    store.flush(true);
    backup_event_file(dirStr, 1);

    std::cout << "    Calling save() (1 -> 2)..." << std::endl;
    pid = store.save(
      StoreSaveMode::forkSave); // Forks. Child tries to snapshot 1->2.
    std::cout << "    save() forked child pid is " << pid << std::endl;
    if (pid <= 0) {
      std::cerr << "FAIL: save() pid <= 0; not fork." << std::endl;
      return 1;
    }

    // --- CYCLE 2 ---
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    std::cout << "[4] Cycle 2: Writing Key 2..." << std::endl;
    store.update("key2", "val2");

    store.flush(true);
    backup_event_file(dirStr, 2);

    std::cout << "    Calling save() (2 -> 3)..." << std::endl;
    pid = store.save(StoreSaveMode::forkSave);
    std::cout << "    save() forked child pid is " << pid << std::endl;
    if (pid <= 0) {
      std::cerr << "FAIL: save() pid <= 0; not fork." << std::endl;
      return 1;
    }

    // --- CYCLE 3 ---
    std::cout << "[5] Cycle 3: Writing Key 3 (Final active log)..."
              << std::endl;
    store.update("key3", "val3");
    // No save here, this stays in 3.events
    store.flush(true);
  }

  std::cout << "[6] Wait 2s for background processes to finish..." << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // --- SIMULATE FAILURE ---
  std::cout << "[7] Deleting all snapshots (simulate failure)..." << std::endl;

  // 1. Delete ALL snapshots. Use file iterator.
  int snapsDeleted = 0;
  for (const auto& entry : fs::directory_iterator(dirStr)) {
    if (entry.path().extension() == ".snapshot") {
      fs::remove(entry.path());
      snapsDeleted++;
    }
  }
  std::cout << "    Deleted " << snapsDeleted << " snapshot(s)." << std::endl;

  // 2. Restore backed up events (simulating that cleanup failed or events were
  // recovered) We expect the store to have deleted 0.events, 1.events, 2.events
  // by now. We put them back.
  int eventsRestored = 0;
  for (const auto& entry : fs::directory_iterator(dirStr)) {
    if (entry.path().extension() == ".bak") {
      std::string stem = entry.path().stem().string(); // e.g., 00...01.events
      fs::copy_file(entry.path(), fs::path(dirStr) / stem,
                    fs::copy_options::overwrite_existing);
      eventsRestored++;
    }
  }
  std::cout << "    Restored " << eventsRestored << " event file(s)."
            << std::endl;

  // --- RELOAD AND VERIFY ---
  std::cout << "[8] Reloading Store..." << std::endl;
  std::cout << "    Expectation: Store finds NO snapshots, starts at 0, and "
               "replays 0, 1, 2, 3 events."
            << std::endl;

  {
    Store<StdMap, std::string, std::string> store(dirStr);

    // Verify Data
    bool allPass = true;
    for (int i = 0; i <= 3; ++i) {
      std::string key = "key" + std::to_string(i);
      std::string val = "val" + std::to_string(i);

      if (store().find(key) == store().end()) {
        std::cout << "    [FAIL] Missing " << key << std::endl;
        allPass = false;
      } else if (store[key] != val) {
        std::cout << "    [FAIL] Wrong value for " << key << ": " << store[key]
                  << std::endl;
        allPass = false;
      } else {
        std::cout << "    [PASS] Found " << key << std::endl;
      }
    }

    if (allPass) {
      std::cout << "--- Test Complete: SUCCESS ---" << std::endl;
    } else {
      std::cout << "--- Test Complete: FAILED ---" << std::endl;
      return 1;
    }
  }

  return 0;
#endif
}