#include <filesystem>
#include <fstream>
#include <iostream>
#include <logkv/store.h>
#include <string>
#include <unordered_map>

#if !LOGKV_WINDOWS
#include <sys/resource.h>
#endif

template <typename K, typename V> using StdMap = std::unordered_map<K, V>;

using namespace logkv;

// A garbage frame header at the end of an events file must not make load()
// allocate the payload size that the header claims.
int main() {
#if LOGKV_WINDOWS
  std::cout << "[SKIP] getrusage() test." << std::endl;
  return 0;
#else
  const std::string dir = "./garbagetaildata";
  {
    Store<StdMap, std::string, std::string> store(
      dir, StoreFlags::createDir | StoreFlags::deleteData);
    store.update("good", "1");
    store.flush();
  }
  std::filesystem::path events;
  for (const auto& e : std::filesystem::directory_iterator(dir))
    if (e.path().extension() == ".events" &&
        (events.empty() || e.path().filename() > events.filename()))
      events = e.path();
  if (events.empty()) {
    std::cerr << "FAIL: no events file" << std::endl;
    return 1;
  }
  {
    std::ofstream f(events, std::ios::binary | std::ios::app);
    const char garbage[] = {char(0xDF), char(0xFF), char(0xFF), char(0xFF), 0, 0, 0};
    f.write(garbage, sizeof garbage);
  }
  {
    Store<StdMap, std::string, std::string> store(dir);
    if (store().count("good") != 1) {
      std::cerr << "FAIL: the good record was lost" << std::endl;
      return 1;
    }
  }
  struct rusage ru {};
  getrusage(RUSAGE_SELF, &ru);
  // macOS gives `ru_maxrss` in bytes. Linux gives it in KiB.
#if defined(__APPLE__)
  const long maxrssKiB = long(ru.ru_maxrss / 1024);
#else
  const long maxrssKiB = long(ru.ru_maxrss);
#endif
  if (maxrssKiB > 256 * 1024) {
    std::cerr << "FAIL: load() reached " << maxrssKiB << " KiB resident" << std::endl;
    return 1;
  }
  std::filesystem::remove_all(dir);
  std::cout << "PASS" << std::endl;
  return 0;
#endif
}
