#include <boost/unordered/unordered_flat_map.hpp>

#include <logkv/store.h>
using namespace logkv;

#include <logkv/autoser/bytes.h>

#include <chrono>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <random>

constexpr size_t NUM_PRECOMPUTED_KEYS = 100'000;
constexpr size_t NUM_PRECOMPUTED_VALUES = 65536;
constexpr size_t MAX_VAL_SIZE = 4096;
constexpr size_t NUM_UPDATE_OPS = 1'000'000;
constexpr size_t KEY_SIZE = 32;
constexpr size_t IMPORTANCE_RATE = 1;
constexpr size_t LOG_UPDATE_OPS = NUM_UPDATE_OPS / 100;

Bytes randomBytes(size_t len, std::mt19937_64& rng) {
  Bytes b(len);
  for (size_t i = 0; i < len; ++i) {
    b[i] = static_cast<char>(rng() % 256);
  }
  return b;
}

int main(int argc, char* argv[]) {
  const std::string dirStr = "./benchdata";

  std::cout << "Benchmark: " << NUM_UPDATE_OPS << " updates, logging 1 in "
            << IMPORTANCE_RATE << std::endl;

  Store<boost::unordered_flat_map, Hash, Bytes> store(
    dirStr, StoreFlags::createDir | StoreFlags::deleteData);

  std::mt19937_64 rng(42);

  std::cout << "Making " << NUM_PRECOMPUTED_KEYS << " keys of size " << KEY_SIZE
            << "..." << std::endl;
  std::vector<Hash> key_cache(NUM_PRECOMPUTED_KEYS);
  for (size_t i = 0; i < NUM_PRECOMPUTED_KEYS; ++i) {
    key_cache[i] = bytesToHash(randomBytes(KEY_SIZE, rng));
  }

  std::cout << "Making " << NUM_PRECOMPUTED_VALUES
            << " values with random sizes (0 to " << MAX_VAL_SIZE
            << " bytes)..." << std::endl;
  std::vector<Bytes> value_cache(NUM_PRECOMPUTED_VALUES);
  for (size_t i = 0; i < NUM_PRECOMPUTED_VALUES; ++i) {
    size_t current_value_size = rng() % (MAX_VAL_SIZE + 1);
    value_cache[i] = randomBytes(current_value_size, rng);
  }

  std::cout << "Performing " << NUM_UPDATE_OPS << " update operations..."
            << std::endl;

  uint64_t event_writes = 0;

  auto overall_start_time = std::chrono::steady_clock::now();
  for (size_t i = 0; i < NUM_UPDATE_OPS; ++i) {
    size_t key_idx = i % NUM_PRECOMPUTED_KEYS;
    const Hash& k = key_cache[key_idx];

    size_t value_idx = i % NUM_PRECOMPUTED_VALUES;
    const Bytes& current_val = value_cache[value_idx];

    if (i % IMPORTANCE_RATE == 0) {
      ++event_writes;
      if (current_val.size() == 0) {
        store.erase(k);
      } else {
        store.update(k, current_val);
      }
    } else {
      if (current_val.size() == 0) {
        store.getObjects().erase(k);
      } else {
        store.getObjects()[k] = current_val;
      }
    }
  }
  auto overall_end_time = std::chrono::steady_clock::now();
  std::chrono::duration<double> total_elapsed_time =
    overall_end_time - overall_start_time;

  std::cout << "Update operations complete." << std::endl;

  store.flush();
  std::filesystem::path event_file_path =
    std::filesystem::path(dirStr) / "00000000000000000000.events";
  uintmax_t event_file_size = std::filesystem::file_size(event_file_path);
  std::cout << "Events file (" << event_file_path.filename().string()
            << ") size: " << event_file_size << " bytes." << std::endl;

  std::cout << "Saving final state..." << std::endl;
  auto save_start_time = std::chrono::steady_clock::now();
  store.save(false);
  auto save_end_time = std::chrono::steady_clock::now();
  std::chrono::duration<double> snapshot_write_time =
    save_end_time - save_start_time;
  std::filesystem::path snapshot_file_path =
    std::filesystem::path(dirStr) / "00000000000000000001.snapshot";
  uintmax_t snapshot_file_size = std::filesystem::file_size(snapshot_file_path);
  std::cout << "Snapshot file (" << snapshot_file_path.filename().string()
            << ") size: " << snapshot_file_size << " bytes." << std::endl;

  std::cout << std::fixed << std::setprecision(6);
  std::cout << "------------------------------------------" << std::endl;
  std::cout << "Event writes:                    " << event_writes << std::endl;
  std::cout << "Total elapsed time (updates):    " << total_elapsed_time.count()
            << " seconds." << std::endl;
  std::cout << "Time in store.save() (snapshot): "
            << snapshot_write_time.count() << " seconds." << std::endl;
  std::cout << "------------------------------------------" << std::endl;
  std::cout << "Data stored in: " << dirStr << std::endl;

  std::cout << "Testing load..." << std::endl;
  auto load_start_time = std::chrono::steady_clock::now();
  Store<boost::unordered_flat_map, Hash, Bytes> store2(dirStr,
                                                       StoreFlags::deferLoad);
  if (!store2.load()) {
    std::cout << "ERROR: store2.load() returned false (corrupted events file)."
              << std::endl;
  }
  auto load_end_time = std::chrono::steady_clock::now();
  std::chrono::duration<double> snapshot_read_time =
    load_end_time - load_start_time;
  std::cout << "Load complete." << std::endl;
  std::cout << "------------------------------------------" << std::endl;
  std::cout << "Time in store2.load() (snapshot): "
            << snapshot_read_time.count() << " seconds." << std::endl;
  std::cout << "------------------------------------------" << std::endl;

  bool identical = true;
  const auto& map1 = store.getObjects();
  const auto& map2 = store2.getObjects();

  if (map1.size() != map2.size()) {
    identical = false;
    std::cout << "Test failed: Different sizes! store size: " << map1.size()
              << ", store2 size: " << map2.size() << std::endl;
  } else {
    std::cout << "Testing " << map1.size() << " elements..." << std::endl;
    size_t compared_count = 0;
    for (const auto& pair1 : map1) {
      const Hash& key1 = pair1.first;
      const Bytes& val1 = pair1.second;
      auto it2 = map2.find(key1);
      if (it2 == map2.end()) {
        identical = false;
        std::cout << "Test failed: Key from store not found in store2."
                  << std::endl;
        break;
      }
      const Bytes& val2 = it2->second;
      if (!(val1 == val2)) {
        identical = false;
        std::cout << "Test failed: Values for a key differ." << std::endl;
        break;
      }
      compared_count++;
    }
  }
  if (identical && map1.size() == map2.size()) {
    std::cout << "Test passed: store and store2 objects are identical."
              << std::endl;
  }
  return 0;
}
