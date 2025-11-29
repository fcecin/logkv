#include <chrono>
#include <iostream>
#include <logkv/store.h>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

// Define a simple map alias to use std::unordered_map
template <typename K, typename V> using StdMap = std::unordered_map<K, V>;

using namespace logkv;

// Simple string wrapper to act as K/V if needed,
// though we will use std::string directly for this test.

int main(int argc, char* argv[]) {
  std::cout << "--- LogKV Parallel Fork Test ---" << std::endl;

#if LOGKV_WINDOWS
  std::cout << "[SKIP] Test undefined for Windows or Serial mode." << std::endl;
  std::cout
    << "       This test specifically targets the Linux fork() behavior."
    << std::endl;
  return 0;
#else

  const std::string dirStr = "./forktestdata";

  // 1. SETUP: Clean start
  std::cout << "[1] Setting up store in " << dirStr << "..." << std::endl;
  {
    Store<StdMap, std::string, std::string> store(
      dirStr, StoreFlags::createDir | StoreFlags::deleteData);

    // 2. PHASE 1: Base Data
    std::cout << "[2] Writing 'base_key'..." << std::endl;
    store.update("base_key", "base_value");

    // 3. PHASE 2: Parallel Save
    // If logic is correct, this forks.
    // Child writes Snapshot containing "base_key".
    // Parent returns immediately, increments time, and opens new event file.
    std::cout << "[3] Calling save() (Expect Fork)..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();
    int pid = store.save(StoreSaveMode::forkSave);
    if (pid <= 0) {
      std::cerr << "FAIL: save() pid <= 0; not fork." << std::endl;
      return 1;
    }
    std::cout << "    save() forked child pid is " << pid << std::endl;
    auto end = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double, std::milli> elapsed = end - start;
    std::cout << "    save() returned in " << elapsed.count() << "ms."
              << std::endl;

    // 4. PHASE 3: Immediate Write to New Log
    // This key should end up in the NEW .events file, not the snapshot being
    // written by the child.
    std::cout << "[4] Immediately writing 'fork_key'..." << std::endl;
    store.update("fork_key", "fork_value");

    // Destructor checks
    std::cout << "[5] Closing store (Parent)..." << std::endl;
    store.flush();
  }

  // 5. PHASE 4: Synchronization
  // Since the child process might still be writing the snapshot file
  // (since save() returned immediately), we sleep briefly to ensure
  // the filesystem is settled before we try to reload.
  std::cout << "[6] Sleeping 2s to allow child process to finish snapshot..."
            << std::endl;
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  // 6. PHASE 5: Verification (Reload)
  std::cout << "[7] Reloading store..." << std::endl;
  {
    Store<StdMap, std::string, std::string> store(dirStr);

    if (!store.isLoaded()) {
      std::cerr << "FAIL: Store failed to load." << std::endl;
      return 1;
    }

    std::cout << "    Store loaded. Current Time: " << store.getTime()
              << std::endl;

    // Verify Key A (Should come from Snapshot)
    if (store().find("base_key") != store().end() &&
        store["base_key"] == "base_value") {
      std::cout
        << "    [PASS] 'base_key' found (Persisted via Forked Snapshot)."
        << std::endl;
    } else {
      std::cerr << "    [FAIL] 'base_key' missing or incorrect!" << std::endl;
      return 1;
    }

    // Verify Key B (Should come from Event Log N+1)
    if (store().find("fork_key") != store().end() &&
        store["fork_key"] == "fork_value") {
      std::cout
        << "    [PASS] 'fork_key' found (Persisted via Event Log post-fork)."
        << std::endl;
    } else {
      std::cerr << "    [FAIL] 'fork_key' missing or incorrect!" << std::endl;
      return 1;
    }
  }

  std::cout << "--- Test Complete: SUCCESS ---" << std::endl;
  return 0;
#endif
}