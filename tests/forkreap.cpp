#include <filesystem>
#include <iostream>
#include <logkv/store.h>
#include <string>
#include <unordered_map>

template <typename K, typename V> using StdMap = std::unordered_map<K, V>;

using namespace logkv;

// forkSave must reap only the children it forked.
int main() {
#if LOGKV_WINDOWS
  std::cout << "[SKIP] fork() test." << std::endl;
  return 0;
#else
  const std::string dir = "./forkreapdata";
  int status = 0;
  {
    Store<StdMap, std::string, std::string> store(
      dir, StoreFlags::createDir | StoreFlags::deleteData);
    store.update("key", "value");
    store.flush();

    pid_t other = fork();
    if (other == -1) {
      std::cerr << "FAIL: fork() failed" << std::endl;
      return 1;
    }
    if (other == 0) {
      _exit(7);
    }
    siginfo_t info{};
    if (waitid(P_PID, other, &info, WEXITED | WNOWAIT) != 0) {
      std::cerr << "FAIL: waitid() failed" << std::endl;
      return 1;
    }

    int pid = store.save(StoreSaveMode::forkSave);
    if (pid <= 0) {
      std::cerr << "FAIL: save() did not fork" << std::endl;
      return 1;
    }
    pid_t r = waitpid(other, &status, 0);
    if (r != other || !WIFEXITED(status) || WEXITSTATUS(status) != 7) {
      std::cerr << "FAIL: save() reaped a child that it did not fork"
                << std::endl;
      return 1;
    }
    waitpid(pid, &status, 0);
    store.flush();
  }
  std::filesystem::remove_all(dir);
  std::cout << "PASS" << std::endl;
  return 0;
#endif
}
