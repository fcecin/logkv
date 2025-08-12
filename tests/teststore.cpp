#include <logkv/store.h>

#include <logkv/autoser/asio.h>
#include <logkv/autoser/associative.h>
#include <logkv/autoser/bytes.h>
#include <logkv/autoser/pushback.h>

#include <iostream>

#include <boost/unordered/unordered_flat_map.hpp>

using TestStore = logkv::Store<std::map, logkv::Bytes, logkv::Bytes>;

const std::string TEST_BASE_DIR = "logkv_store_test_run_data";

std::string setup_test_directory(const std::string& test_sub_path) {
  std::filesystem::path base_path = TEST_BASE_DIR;
  std::filesystem::path test_dir_path = base_path / test_sub_path;

  if (std::filesystem::exists(test_dir_path)) {
    std::filesystem::remove_all(test_dir_path);
  }

  std::filesystem::create_directories(test_dir_path);
  return test_dir_path.string();
}

void cleanup_test_directory(const std::string& full_dir_path_str) {
  std::filesystem::path dir_path = full_dir_path_str;
  if (std::filesystem::exists(dir_path)) {
    try {
      std::filesystem::remove_all(dir_path);
    } catch (const std::filesystem::filesystem_error& e) {
      std::cerr
        << "Warning: Filesystem error during cleanup_test_directory for "
        << full_dir_path_str << ": " << e.what() << " (code: " << e.code()
        << ")." << std::endl;
    }
  }
}

void cleanup_base_test_directory() {
  std::filesystem::path base_path = TEST_BASE_DIR;
  if (std::filesystem::exists(base_path)) {
    std::filesystem::remove_all(base_path);
  }
}

std::string test_pad_filename(uint64_t n) {
  std::ostringstream oss;
  oss << std::setw(20) << std::setfill('0') << n;
  return oss.str();
}

void test_store_constructor_and_directory_handling() {
  std::cout << "Running test_store_constructor_and_directory_handling..."
            << std::endl;
  std::string test_name = "constructor_dir_handling";

  std::string main_test_subdir = setup_test_directory(test_name);

  {
    std::cout << "  Subtest: StoreFlags::none (dir exists)..." << std::endl;
    std::string dir1 =
      (std::filesystem::path(main_test_subdir) / "dir1_exists").string();
    std::filesystem::create_directories(dir1);
    TestStore store(dir1, logkv::StoreFlags::none);
    assert(std::filesystem::exists(dir1));
    assert(store.getObjects().empty());
  }
  {
    std::cout
      << "  Subtest: StoreFlags::none (dir does not exist - should throw)..."
      << std::endl;
    std::string dir_non_existent =
      (std::filesystem::path(main_test_subdir) / "dir_non_existent").string();
    bool caught = false;
    try {
      TestStore store(dir_non_existent, logkv::StoreFlags::none);
    } catch (const std::runtime_error& e) {
      std::string msg = e.what();
      assert(msg.find("directory not found") != std::string::npos);
      caught = true;
    }
    assert(caught);
  }

  {
    std::cout << "  Subtest: StoreFlags::createDir (dir does not exist)..."
              << std::endl;
    std::string dir_create =
      (std::filesystem::path(main_test_subdir) / "dir_create").string();

    if (std::filesystem::exists(dir_create))
      std::filesystem::remove_all(dir_create);
    TestStore store(dir_create, logkv::StoreFlags::createDir);
    assert(std::filesystem::exists(dir_create));
    assert(std::filesystem::is_directory(dir_create));
  }
  {
    std::cout << "  Subtest: StoreFlags::createDir (dir already exists)..."
              << std::endl;
    std::string dir_create_exists =
      (std::filesystem::path(main_test_subdir) / "dir_create_exists").string();
    std::filesystem::create_directories(dir_create_exists);
    TestStore store(dir_create_exists, logkv::StoreFlags::createDir);
    assert(std::filesystem::exists(dir_create_exists));
  }

  {
    std::cout << "  Subtest: StoreFlags::deleteData..." << std::endl;
    std::string dir_delete =
      (std::filesystem::path(main_test_subdir) / "dir_delete_data").string();
    std::filesystem::create_directories(dir_delete);

    std::ofstream((std::filesystem::path(dir_delete) /
                   (test_pad_filename(1) + ".snapshot")))
      .put('s');
    std::ofstream(
      (std::filesystem::path(dir_delete) / (test_pad_filename(1) + ".events")))
      .put('e');
    std::ofstream((std::filesystem::path(dir_delete) / "some_other_file.txt"))
      .put('o');
    std::ofstream((std::filesystem::path(dir_delete) / "notdigits.snapshot"))
      .put('n');
    std::ofstream((std::filesystem::path(dir_delete) / "123.txt")).put('x');

    TestStore store(dir_delete, logkv::StoreFlags::createDir |
                                  logkv::StoreFlags::deleteData);
    assert(std::filesystem::exists(dir_delete));

    assert(!std::filesystem::exists(std::filesystem::path(dir_delete) /
                                    (test_pad_filename(1) + ".snapshot")));
    assert(!std::filesystem::exists(std::filesystem::path(dir_delete) /
                                    (test_pad_filename(1) + ".events")));
    assert(std::filesystem::exists(std::filesystem::path(dir_delete) /
                                   "some_other_file.txt"));
    assert(std::filesystem::exists(std::filesystem::path(dir_delete) /
                                   "notdigits.snapshot"));
    assert(
      std::filesystem::exists(std::filesystem::path(dir_delete) / "123.txt"));
  }

  {
    std::cout
      << "  Subtest: StoreFlags::deferLoad (save should throw if not loaded)..."
      << std::endl;
    std::string dir_defer =
      (std::filesystem::path(main_test_subdir) / "dir_defer").string();
    std::filesystem::create_directories(dir_defer);

    TestStore store(dir_defer, logkv::StoreFlags::deferLoad);
    bool caught = false;
    try {
      store.save();
    } catch (const std::runtime_error& e) {
      std::string msg = e.what();
      assert(msg.find("cannot save() without calling load() first") !=
             std::string::npos);
      caught = true;
    }
    assert(caught);

    std::cout << "  Subtest: StoreFlags::deferLoad (load then save)..."
              << std::endl;
    store.load();
    store.save();

    assert(std::filesystem::exists(std::filesystem::path(dir_defer) /
                                   (test_pad_filename(1) + ".snapshot")));
  }

  {
    std::cout << "  Subtest: Path is a file (should throw)..." << std::endl;
    std::string file_path_str =
      (std::filesystem::path(main_test_subdir) / "path_is_a_file.txt").string();
    std::ofstream(file_path_str).put('f');
    bool caught = false;
    try {
      TestStore store(file_path_str, logkv::StoreFlags::none);
    } catch (const std::runtime_error& e) {
      std::string msg = e.what();
      assert(msg.find("directory path is not a directory") !=
             std::string::npos);
      caught = true;
    }
    assert(caught);
    std::filesystem::remove(file_path_str);
  }

  cleanup_test_directory(main_test_subdir);
  std::cout << "test_store_constructor_and_directory_handling PASSED."
            << std::endl;
}

void test_store_save_and_load_empty() {
  std::cout << "Running test_store_save_and_load_empty..." << std::endl;
  std::string test_name = "save_load_empty";
  std::string dir_path = setup_test_directory(test_name);

  {
    TestStore store(dir_path, logkv::StoreFlags::createDir);
    assert(store.getObjects().empty());
    store.save();

    std::string snapshot_name = test_pad_filename(1) + ".snapshot";
    assert(
      std::filesystem::exists(std::filesystem::path(dir_path) / snapshot_name));
  }

  {
    TestStore store_load(dir_path, logkv::StoreFlags::none);
    assert(store_load.getObjects().empty());

    store_load.save();
    std::string new_snapshot_name = test_pad_filename(2) + ".snapshot";
    assert(std::filesystem::exists(std::filesystem::path(dir_path) /
                                   new_snapshot_name));
  }

  cleanup_test_directory(dir_path);
  std::cout << "test_store_save_and_load_empty PASSED." << std::endl;
}

void test_store_update_erase_flush_events() {
  std::cout << "Running test_store_update_erase_flush_events..." << std::endl;
  std::string test_name = "update_erase_events";
  std::string dir_path = setup_test_directory(test_name);

  logkv::Bytes key1("keyOne"), val1("valueOne");
  logkv::Bytes key2("keyTwo"), val2("valueTwo");

  {
    TestStore store(dir_path, logkv::StoreFlags::createDir);
    store.update(key1, val1);
    store.update(key2, val2);
    store.flush();

    std::string events_file_name = test_pad_filename(0) + ".events";
    assert(std::filesystem::exists(std::filesystem::path(dir_path) /
                                   events_file_name));
    assert(std::filesystem::file_size(std::filesystem::path(dir_path) /
                                      events_file_name) > 0);
    assert(store.getObjects().size() == 2);
    assert(store.getObjects().at(key1) == val1);
  }

  {
    TestStore store_load(dir_path, logkv::StoreFlags::none);
    assert(store_load.getObjects().size() == 2);
    assert(store_load.getObjects().at(key1) == val1);
    assert(store_load.getObjects().at(key2) == val2);

    store_load.erase(key1);
    store_load.flush();
    assert(store_load.getObjects().size() == 1);
    assert(store_load.getObjects().count(key1) == 0);
  }

  {
    TestStore store_load2(dir_path, logkv::StoreFlags::none);
    assert(store_load2.getObjects().size() == 1);
    assert(store_load2.getObjects().count(key1) == 0);
    assert(store_load2.getObjects().at(key2) == val2);
  }

  cleanup_test_directory(dir_path);
  std::cout << "test_store_update_erase_flush_events PASSED." << std::endl;
}

void test_store_save_snapshot_with_data() {
  std::cout << "Running test_store_save_snapshot_with_data..." << std::endl;
  std::string test_name = "save_load_data";
  std::string dir_path = setup_test_directory(test_name);

  logkv::Bytes key1("s_key1"), val1("s_val1");
  logkv::Bytes key2("s_key2"), val2("s_val2");

  {
    TestStore store(dir_path, logkv::StoreFlags::createDir);
    store.update(key1, val1);
    store.update(key2, val2);

    store.save();

    std::string snapshot_name = test_pad_filename(1) + ".snapshot";
    std::string old_events_name = test_pad_filename(0) + ".events";

    assert(
      std::filesystem::exists(std::filesystem::path(dir_path) / snapshot_name));

    assert(!std::filesystem::exists(std::filesystem::path(dir_path) /
                                    old_events_name));
  }

  {
    TestStore store_load(dir_path, logkv::StoreFlags::none);
    assert(store_load.getObjects().size() == 2);
    assert(store_load.getObjects().at(key1) == val1);
    assert(store_load.getObjects().at(key2) == val2);

    store_load.save();
    std::string new_snapshot_name = test_pad_filename(2) + ".snapshot";
    assert(std::filesystem::exists(std::filesystem::path(dir_path) /
                                   new_snapshot_name));
  }

  cleanup_test_directory(dir_path);
  std::cout << "test_store_save_snapshot_with_data PASSED." << std::endl;
}

void test_store_clear_method() {
  std::cout << "Running test_store_clear_method..." << std::endl;
  std::string test_name = "clear_method";
  std::string dir_path = setup_test_directory(test_name);

  logkv::Bytes key1("c_key1"), val1("c_val1");

  {
    TestStore store(dir_path, logkv::StoreFlags::createDir);
    store.update(key1, val1);
    store.flush();
    assert(!store.getObjects().empty());

    store.clear();
    assert(store.getObjects().empty());

    std::string snapshot_name = test_pad_filename(1) + ".snapshot";
    assert(
      std::filesystem::exists(std::filesystem::path(dir_path) / snapshot_name));

    assert(!std::filesystem::exists(std::filesystem::path(dir_path) /
                                    (test_pad_filename(0) + ".events")));
  }

  {
    TestStore store_load(dir_path, logkv::StoreFlags::none);
    assert(store_load.getObjects().empty());
  }

  cleanup_test_directory(dir_path);
  std::cout << "test_store_clear_method PASSED." << std::endl;
}

void test_store_load_snapshot_then_newer_events() {
  std::cout << "Running test_store_load_snapshot_then_newer_events..."
            << std::endl;
  std::string test_name = "load_snap_then_events";
  std::string dir_path = setup_test_directory(test_name);

  logkv::Bytes key_snap("key_snap"), val_snap("val_snap");
  logkv::Bytes key_event("key_event"), val_event("val_event");
  logkv::Bytes val_snap_updated("val_snap_MODIFIED");

  {
    TestStore store(dir_path, logkv::StoreFlags::createDir);
    store.update(key_snap, val_snap);
    store.save();
  }

  {
    TestStore store(dir_path, logkv::StoreFlags::none);
    assert(store.getObjects().size() == 1);
    assert(store.getObjects().at(key_snap) == val_snap);

    store.update(key_event, val_event);
    store.update(key_snap, val_snap_updated);
    store.flush();
  }

  {
    TestStore store_load(dir_path, logkv::StoreFlags::none);
    assert(store_load.getObjects().size() == 2);
    assert(store_load.getObjects().at(key_snap) == val_snap_updated);
    assert(store_load.getObjects().at(key_event) == val_event);
  }

  cleanup_test_directory(dir_path);
  std::cout << "test_store_load_snapshot_then_newer_events PASSED."
            << std::endl;
}

void test_store_operator_access() {
  std::cout << "Running test_store_operator_access..." << std::endl;
  std::string test_name = "operator_access";
  std::string dir_path = setup_test_directory(test_name);

  logkv::Bytes key1("op_key1"), val1("op_val1");
  logkv::Bytes key2("op_key2"), val2_initial("op_val2_initial"),
    val2_modified("op_val2_modified");

  TestStore store(dir_path, logkv::StoreFlags::createDir);

  store[key1] = val1;
  assert(store.getObjects().count(key1) == 1);
  assert(store.getObjects().at(key1) == val1);
  assert(store[key1] == val1);

  store[key1] = val2_initial;
  assert(store[key1] == val2_initial);

  store.getObjects()[key2] = val2_initial;
  assert(store.getObjects().count(key2) == 1);
  assert(store[key2] == val2_initial);

  store.getObjects().at(key2) = val2_modified;
  assert(store[key2] == val2_modified);

  store().clear();
  store()[key1] = logkv::Bytes("final_val");
  assert(store.getObjects().size() == 1);
  assert(store[key1] == logkv::Bytes("final_val"));

  store.save();

  TestStore store_load(dir_path, logkv::StoreFlags::none);
  assert(store_load.getObjects().size() == 1);
  assert(store_load[key1] == logkv::Bytes("final_val"));

  cleanup_test_directory(dir_path);
  std::cout << "test_store_operator_access PASSED." << std::endl;
}

void test_store_buffer_resizing() {
  std::cout << "Running test_store_buffer_resizing..." << std::endl;
  std::string suite_parent_dir_name = "buffer_resizing_suite";

  logkv::Bytes k1("k"), v1("v");

  logkv::Bytes k2("key_long"), v2("value_very_long_indeed");

  std::vector<size_t> buffer_sizes_to_test = {5,  9, 16, 20,
                                              30, 50

  };

  int iteration_counter = 0;
  for (size_t buffer_size : buffer_sizes_to_test) {

    std::string iter_subdir_name = "bs_" + std::to_string(buffer_size) +
                                   "_iter_" + std::to_string(iteration_counter);
    std::string dir_path =
      setup_test_directory(suite_parent_dir_name + "/" + iter_subdir_name);

    std::cout << "  Subtest: Buffer size = " << buffer_size
              << " in dir: " << dir_path << "..." << std::endl;

    {
      TestStore store(dir_path, logkv::StoreFlags::createDir, buffer_size);
      store.update(k1, v1);
      store.update(k2, v2);
      store.save();

      assert(std::filesystem::exists(std::filesystem::path(dir_path) /
                                     (test_pad_filename(1) + ".snapshot")));
    }

    {
      TestStore store_load(dir_path, logkv::StoreFlags::none, buffer_size);

      assert(store_load.getObjects().size() == 2);
      assert(store_load.getObjects().count(k1) == 1 &&
             store_load.getObjects().at(k1) == v1);
      assert(store_load.getObjects().count(k2) == 1 &&
             store_load.getObjects().at(k2) == v2);
    }

    cleanup_test_directory(dir_path);
    iteration_counter++;
  }

  std::string full_suite_parent_path =
    (std::filesystem::path(TEST_BASE_DIR) / suite_parent_dir_name).string();
  cleanup_test_directory(full_suite_parent_path);

  std::cout << "test_store_buffer_resizing PASSED." << std::endl;
}
void test_store_key_types() {
  std::cout << "Running test_store_key_types..." << std::endl;
  std::string test_name = "key_value_types";
  std::string dir_path = setup_test_directory(test_name);

  try {
    {
      std::cout << "  Subtest: std::string keys and values..." << std::endl;
      using StringStore = logkv::Store<std::map, std::string, std::string>;

      std::string key1 = "A";
      std::string val1 = "B";
      std::string key2 = "Some larger key";
      std::string val2 = "Some larger value";
      std::string key3 = "E";
      std::string val3 = "";
      {
        StringStore store(dir_path, logkv::createDir | logkv::deleteData);

        store.update(key1, val1);
        store.update(key2, val2);
        store.update(key3, val3);
        store.flush();

        auto& objects = store.getObjects();
        assert(objects.size() == 3);
        assert(objects.at(key1) == val1);
        assert(objects.at(key2) == val2);
        assert(objects.at(key3) == val3);

        // writes the snapshot, which will exclude key3,val3
        // only two keypairs will be in the snapshot
        store.save();
      }
    }

    {
      std::cout << "  Subtest: unordered_map logkv::Bytes keys and values..." << std::endl;
      using StringStore = logkv::Store<std::unordered_map, logkv::Bytes, logkv::Bytes>;

      logkv::Bytes key1("key 1");
      logkv::Bytes key2("value 1");
      logkv::Bytes val1("key 2");
      logkv::Bytes val2("value 2");
      {
        StringStore store(dir_path, logkv::createDir | logkv::deleteData);

        store.update(key1, val1);
        store.update(key2, val2);
        store.flush();

        auto& objects = store.getObjects();
        assert(objects.size() == 2);
        assert(objects.at(key1) == val1);
        assert(objects.at(key2) == val2);
        store.save();
      }

      {
        StringStore reloaded_store(dir_path);
        auto& objects = reloaded_store.getObjects();
        assert(objects.size() == 2);
        assert(objects.at(key1) == val1);
        assert(objects.at(key2) == val2);
      }
    }

    {
      std::cout << "  Subtest: boost_unordered_flat_map logkv::Bytes keys and values..." << std::endl;
      using StringStore = logkv::Store<boost::unordered_flat_map, logkv::Bytes, logkv::Bytes>;

      logkv::Bytes key1("key 1");
      logkv::Bytes key2("value 1");
      logkv::Bytes val1("key 2");
      logkv::Bytes val2("value 2");
      {
        StringStore store(dir_path, logkv::createDir | logkv::deleteData);

        store.update(key1, val1);
        store.update(key2, val2);
        store.flush();

        auto& objects = store.getObjects();
        assert(objects.size() == 2);
        assert(objects.at(key1) == val1);
        assert(objects.at(key2) == val2);
        store.save();
      }

      {
        StringStore reloaded_store(dir_path);
        auto& objects = reloaded_store.getObjects();
        assert(objects.size() == 2);
        assert(objects.at(key1) == val1);
        assert(objects.at(key2) == val2);
      }
    }

    {
      std::cout
        << "  Subtest: std::string keys and std::vector<std::string> values..."
        << std::endl;
      using VecStore =
        logkv::Store<std::map, std::string, std::vector<std::string>>;

      std::string key1 = "user:123:permissions";
      std::vector<std::string> val1 = {"read", "write", "execute"};
      std::string key2 = "user:456:aliases";
      std::vector<std::string> val2 = {"Big John", "Johnny"};
      std::string key3 = "user:789:history";
      std::vector<std::string> val3 = {}; // Empty vector

      {
        VecStore store(dir_path, logkv::createDir | logkv::deleteData);

        store.update(key1, val1);
        store.update(key2, val2);
        store.update(key3, val3);

        auto& objects = store.getObjects();
        assert(objects.size() == 3);
        assert(objects.at(key1) == val1);
        assert(objects.at(key2) == val2);
        assert(objects.at(key3) == val3);

        store.save();
      }

      {
        VecStore reloaded_store(dir_path);
        auto& objects = reloaded_store.getObjects();
        assert(objects.size() == 2);
        assert(objects.at(key1).size() == 3);
        assert(objects.at(key1)[1] == "write");
        assert(objects.at(key2)[0] == "Big John");
      }
    }

    {
      std::cout << "  Subtest: unordered_map boost::asio::ip::udp::endpoint keys..."
                << std::endl;
      using Socket = boost::asio::ip::udp::endpoint;
      using SocketStore = logkv::Store<std::unordered_map, Socket, std::string>;

      Socket key1(boost::asio::ip::make_address("1.2.3.4"), 5);
      Socket key2(boost::asio::ip::make_address("6.7.8.9"), 10);
      std::string val1 = "server-alpha";
      std::string val2 = "server-beta";
      {
        SocketStore store(dir_path, logkv::createDir | logkv::deleteData);

        store.update(key1, val1);
        store.update(key2, val2);

        assert(store.getObjects().size() == 2);
        store.save();
      }

      {
        SocketStore reloaded_store(dir_path);
        auto& objects = reloaded_store.getObjects();
        assert(objects.size() == 2);

        Socket key1(boost::asio::ip::make_address("1.2.3.4"), 5);
        Socket key2(boost::asio::ip::make_address("6.7.8.9"), 10);

        assert(objects.at(key1) == "server-alpha");
        assert(objects.at(key2) == "server-beta");
      }
    }

    {
      std::cout << "  Subtest: boost::unordered_flat_map boost::asio::ip::udp::endpoint keys..."
                << std::endl;
      using Socket = boost::asio::ip::udp::endpoint;
      using SocketStore = logkv::Store<boost::unordered_flat_map, Socket, std::string>;

      Socket key1(boost::asio::ip::make_address("1.2.3.4"), 5);
      Socket key2(boost::asio::ip::make_address("6.7.8.9"), 10);
      std::string val1 = "server-alpha";
      std::string val2 = "server-beta";
      {
        SocketStore store(dir_path, logkv::createDir | logkv::deleteData);

        store.update(key1, val1);
        store.update(key2, val2);

        assert(store.getObjects().size() == 2);
        store.save();
      }

      {
        SocketStore reloaded_store(dir_path);
        auto& objects = reloaded_store.getObjects();
        assert(objects.size() == 2);

        Socket key1(boost::asio::ip::make_address("1.2.3.4"), 5);
        Socket key2(boost::asio::ip::make_address("6.7.8.9"), 10);

        assert(objects.at(key1) == "server-alpha");
        assert(objects.at(key2) == "server-beta");
      }
    }

  } catch (...) {
    cleanup_test_directory(dir_path);
    throw;
  }

  cleanup_test_directory(dir_path);
  std::cout << "test_store_key_types PASSED." << std::endl;
}
int main() {

  if (!std::filesystem::exists(TEST_BASE_DIR)) {
    std::filesystem::create_directories(TEST_BASE_DIR);
  }

  try {
    test_store_constructor_and_directory_handling();
    test_store_save_and_load_empty();
    test_store_update_erase_flush_events();
    test_store_save_snapshot_with_data();
    test_store_clear_method();
    test_store_load_snapshot_then_newer_events();
    test_store_operator_access();
    test_store_buffer_resizing();
    test_store_key_types();

    std::cout << "\nALL Store tests PASSED successfully!" << std::endl;

  } catch (const std::exception& e) {
    std::cerr << "A Store test FAILED with exception: " << e.what()
              << std::endl;

    return 1;
  } catch (...) {
    std::cerr << "A Store test FAILED with an unknown exception." << std::endl;

    return 1;
  }

  cleanup_base_test_directory();
  return 0;
}
