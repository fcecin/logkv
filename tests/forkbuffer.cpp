#include <filesystem>
#include <iostream>
#include <logkv/store.h>
#include <string>
#include <unordered_map>

template <typename K, typename V> using StdMap = std::unordered_map<K, V>;

using namespace logkv;

// An update buffered at forkSave time survives a fork child that fails.
int main() {
#if LOGKV_WINDOWS
  std::cout << "[SKIP] fork() test." << std::endl;
  return 0;
#else
  const std::string dir = "./forkbufferdata";
  const std::filesystem::path blocker =
    std::filesystem::path(dir) / "00000000000000000001.snapshot";
  {
    Store<StdMap, std::string, std::string> store(
      dir, StoreFlags::createDir | StoreFlags::deleteData);
    store.update("flushed", "1");
    store.flush();
    store.update("buffered", "2");

    std::filesystem::create_directory(blocker);
    std::filesystem::create_directory(blocker / "busy");
    int pid = store.save(StoreSaveMode::forkSave);
    if (pid <= 0) {
      std::cerr << "FAIL: save() did not fork" << std::endl;
      return 1;
    }
    int status = 0;
    waitpid(pid, &status, 0);
    if (!WIFEXITED(status) || WEXITSTATUS(status) == 0) {
      std::cerr << "FAIL: the fork child did not fail as planned" << std::endl;
      return 1;
    }
    store.flush();
  }
  std::filesystem::remove_all(blocker);
  {
    Store<StdMap, std::string, std::string> store(dir);
    if (store().count("flushed") != 1 || store().count("buffered") != 1) {
      std::cerr << "FAIL: the buffered update was lost" << std::endl;
      return 1;
    }
  }
  std::filesystem::remove_all(dir);
  std::cout << "PASS" << std::endl;
  return 0;
#endif
}
