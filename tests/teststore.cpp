#include <logkv/store.h>

#include <logkv/autoser/asio.h>
#include <logkv/autoser/associative.h>
#include <logkv/autoser/bytes.h>
#include <logkv/autoser/partial.h>
#include <logkv/autoser/pushback.h>

#include <logkv/bytes.h>

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

  logkv::Bytes key1 = logkv::makeBytes("keyOne");
  logkv::Bytes val1 = logkv::makeBytes("valueOne");
  logkv::Bytes key2 = logkv::makeBytes("keyTwo");
  logkv::Bytes val2 = logkv::makeBytes("valueTwo");

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

  logkv::Bytes key1 = logkv::makeBytes("s_key1");
  logkv::Bytes val1 = logkv::makeBytes("s_val1");
  logkv::Bytes key2 = logkv::makeBytes("s_key2");
  logkv::Bytes val2 = logkv::makeBytes("s_val2");

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

  logkv::Bytes key1 = logkv::makeBytes("c_key1");
  logkv::Bytes val1 = logkv::makeBytes("c_val1");

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

  logkv::Bytes key_snap = logkv::makeBytes("key_snap");
  logkv::Bytes val_snap = logkv::makeBytes("val_snap");
  logkv::Bytes key_event = logkv::makeBytes("key_event");
  logkv::Bytes val_event = logkv::makeBytes("val_event");
  logkv::Bytes val_snap_updated = logkv::makeBytes("val_snap_MODIFIED");

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

  logkv::Bytes key1 = logkv::makeBytes("op_key1");
  logkv::Bytes val1 = logkv::makeBytes("op_val1");
  logkv::Bytes key2 = logkv::makeBytes("op_key2");
  logkv::Bytes val2_initial = logkv::makeBytes("op_val2_initial");
  logkv::Bytes val2_modified = logkv::makeBytes("op_val2_modified");

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
  store()[key1] = logkv::makeBytes("final_val");
  assert(store.getObjects().size() == 1);
  assert(store[key1] == logkv::makeBytes("final_val"));

  store.save();

  TestStore store_load(dir_path, logkv::StoreFlags::none);
  assert(store_load.getObjects().size() == 1);
  assert(store_load[key1] == logkv::makeBytes("final_val"));

  cleanup_test_directory(dir_path);
  std::cout << "test_store_operator_access PASSED." << std::endl;
}

void test_store_buffer_resizing() {
  std::cout << "Running test_store_buffer_resizing..." << std::endl;
  std::string suite_parent_dir_name = "buffer_resizing_suite";

  logkv::Bytes k1 = logkv::makeBytes("k");
  logkv::Bytes v1 = logkv::makeBytes("v");
  logkv::Bytes k2 = logkv::makeBytes("key_long");
  logkv::Bytes v2 = logkv::makeBytes("value_very_long_indeed");

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

        store.save();
      }
    }

    {
      std::cout << "  Subtest: unordered_map logkv::Bytes keys and values..."
                << std::endl;
      using StringStore =
        logkv::Store<std::unordered_map, logkv::Bytes, logkv::Bytes>;

      logkv::Bytes key1 = logkv::makeBytes("key 1");
      logkv::Bytes key2 = logkv::makeBytes("value 1");
      logkv::Bytes val1 = logkv::makeBytes("key 2");
      logkv::Bytes val2 = logkv::makeBytes("value 2");
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
        << "  Subtest: boost_unordered_flat_map logkv::Bytes keys and values..."
        << std::endl;
      using StringStore =
        logkv::Store<boost::unordered_flat_map, logkv::Bytes, logkv::Bytes>;

      logkv::Bytes key1 = logkv::makeBytes("key 1");
      logkv::Bytes key2 = logkv::makeBytes("value 1");
      logkv::Bytes val1 = logkv::makeBytes("key 2");
      logkv::Bytes val2 = logkv::makeBytes("value 2");
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
      std::vector<std::string> val3 = {};

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
      std::cout
        << "  Subtest: unordered_map boost::asio::ip::udp::endpoint keys..."
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
      std::cout << "  Subtest: boost::unordered_flat_map "
                   "boost::asio::ip::udp::endpoint keys..."
                << std::endl;
      using Socket = boost::asio::ip::udp::endpoint;
      using SocketStore =
        logkv::Store<boost::unordered_flat_map, Socket, std::string>;

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
      using ArrayHash = std::array<uint8_t, 32>;
      std::cout << "  Subtest: array<uint8_t> store" << std::endl;
      using ArrStore =
        logkv::Store<boost::unordered_flat_map, ArrayHash, ArrayHash>;

      ArrayHash key1, key2, val1, val2;
      key1.fill(1);
      key2.fill(2);
      val1.fill(3);
      val2.fill(0);
      {
        ArrStore store(dir_path, logkv::createDir | logkv::deleteData);
        store.update(key1, val1);
        store.update(key2, val2);
        assert(store.getObjects().size() == 2);
        store.save();
      }

      {
        ArrStore reloaded_store(dir_path);
        auto& objects = reloaded_store.getObjects();
        assert(objects.size() == 1);
        assert(objects.at(key1) == val1);
      }
    }

    {
      std::cout << "  Subtest: uint64_t store" << std::endl;
      using IntStore =
        logkv::Store<boost::unordered_flat_map, uint64_t, uint64_t>;
      uint64_t key1 = 100;
      uint64_t key2 = 200;
      uint64_t val1 = 300;
      uint64_t val2 = 0;
      {
        IntStore store(dir_path, logkv::createDir | logkv::deleteData);
        store.update(key1, val1);
        store.update(key2, val2);
        assert(store.getObjects().size() == 2);
        store.save();
      }

      {
        IntStore reloaded_store(dir_path);
        auto& objects = reloaded_store.getObjects();
        assert(objects.size() == 1);
        assert(objects.at(key1) == val1);
      }
    }

  } catch (...) {
    cleanup_test_directory(dir_path);
    throw;
  }

  cleanup_test_directory(dir_path);
  std::cout << "test_store_key_types PASSED." << std::endl;
}

void test_store_iterators() {
  std::cout << "Running test_store_iterators..." << std::endl;
  std::string test_name = "iterators";
  std::string dir_path = setup_test_directory(test_name);

  logkv::Bytes key1 = logkv::makeBytes("i_key1");
  logkv::Bytes val1 = logkv::makeBytes("i_val1");
  logkv::Bytes key2 = logkv::makeBytes("i_key2");
  logkv::Bytes val2 = logkv::makeBytes("i_val2");

  {
    TestStore store(dir_path, logkv::StoreFlags::createDir);
    store.update(key1, val1);
    store.update(key2, val2);

    auto it1 = store.find(key1);
    assert(it1 != store.end());
    assert(it1->second == val1);

    auto it_none = store.find(logkv::makeBytes("nonexistent"));
    assert(it_none == store.end());

    size_t count = 0;
    for (auto it = store.begin(); it != store.end(); ++it) {
      count++;
    }
    assert(count == 2);

    it1->second = logkv::makeBytes("i_val1_modified");
    store.persist(it1);
    store.flush();
  }

  {
    TestStore store_load(dir_path, logkv::StoreFlags::none);
    assert(store_load.getObjects().at(key1) ==
           logkv::makeBytes("i_val1_modified"));

    auto it2 = store_load.find(key2);
    assert(it2 != store_load.end());
    store_load.erase(it2);

    assert(store_load.find(key2) == store_load.end());
    store_load.flush();
  }

  {
    TestStore store_load2(dir_path, logkv::StoreFlags::none);
    assert(store_load2.getObjects().count(key2) == 0);
    assert(store_load2.getObjects().count(key1) == 1);
  }

  cleanup_test_directory(dir_path);
  std::cout << "test_store_iterators PASSED." << std::endl;
}

struct PartialObj : public logkv::AutoSerializableObject<PartialObj> {
  uint64_t id;
  std::string heavyData;
  uint64_t counter;

  PartialObj() : id(0), counter(0) {}
  PartialObj(uint64_t i, std::string h, uint64_t c)
      : id(i), heavyData(h), counter(c) {}

  bool operator==(const PartialObj& other) const {
    return id == other.id && heavyData == other.heavyData &&
           counter == other.counter;
  }

  static void setFullSerialization(bool full) { fullMode_ = full; }
  static bool getFullSerialization() { return fullMode_; }

  static void _logkvStoreSnapshot(bool snapshot) { isSnapshot_ = snapshot; }
  static bool _logkvStoreSnapshot() { return isSnapshot_; }

  auto _full_tie() const { return std::tie(id, heavyData, counter); }
  auto _partial_tie() const { return std::tie(id, counter); }

  auto _full_tie_mut() { return std::tie(id, heavyData, counter); }
  auto _partial_tie_mut() { return std::tie(id, counter); }

private:
  static thread_local bool fullMode_;
  static thread_local bool isSnapshot_;
};

thread_local bool PartialObj::fullMode_ = false;
thread_local bool PartialObj::isSnapshot_ = false;

namespace logkv {
template <> struct serializer<PartialObj> {
  static size_t get_size(const PartialObj& obj) {
    bool isSnapshot = PartialObj::_logkvStoreSnapshot();
    bool full = isSnapshot || PartialObj::getFullSerialization();

    if (full)
      return logkv::get_size_for_members(obj._full_tie());

    return logkv::get_size_for_members(obj._partial_tie()) +
           (isSnapshot ? 0 : 1);
  }

  static bool is_empty(const PartialObj& obj) { return obj.id == 0; }

  static size_t write(char* dest, size_t size, const PartialObj& obj) {
    Writer writer(dest, size);
    bool isSnapshot = PartialObj::_logkvStoreSnapshot();
    bool full = isSnapshot || PartialObj::getFullSerialization();

    try {
      if (!isSnapshot) {

        if (is_empty(obj)) {
          writer.write((uint8_t)0x02);
        } else if (full) {
          writer.write((uint8_t)0x00);
        } else {
          writer.write((uint8_t)0x01);
        }
      }

      if (is_empty(obj) && !isSnapshot) {

      } else if (full) {
        logkv::write_members(writer, obj._full_tie());
      } else {
        logkv::write_members(writer, obj._partial_tie());
      }
    } catch (const insufficient_buffer& e) {
      return writer.bytes_processed() + e.get_required_bytes();
    }
    return writer.bytes_processed();
  }

  static size_t read(const char* src, size_t size, PartialObj& obj) {
    Reader reader(src, size);
    bool isSnapshot = PartialObj::_logkvStoreSnapshot();
    bool full = isSnapshot;
    bool objectIsEmpty = false;

    try {
      if (!isSnapshot) {
        uint8_t header;
        reader.read(header);
        if (header == 0x02)
          objectIsEmpty = true;
        else if (header == 0x00)
          full = true;
        else if (header == 0x01)
          full = false;
        else
          throw std::runtime_error("Invalid header");
      }

      if (objectIsEmpty) {
        obj = PartialObj();
      } else if (full) {
        auto members = obj._full_tie_mut();
        logkv::read_members(reader, members);
      } else {
        auto members = obj._partial_tie_mut();
        logkv::read_members(reader, members);
      }
    } catch (const insufficient_buffer& e) {
      return reader.bytes_processed() + e.get_required_bytes();
    }
    return reader.bytes_processed();
  }
};
} // namespace logkv

void test_store_partial_serialization() {
  std::cout << "Running test_store_partial_serialization..." << std::endl;
  std::string test_name = "partial_serialization";
  std::string dir_path = setup_test_directory(test_name);

  using PartialStore = logkv::Store<std::map, uint64_t, PartialObj>;

  uint64_t k1 = 100;

  std::string heavyOriginal = "ORIGINAL_HEAVY_DATA_THAT_MUST_SURVIVE";
  std::string heavyTransient = "TRANSIENT_DATA_THAT_MUST_NOT_PERSIST";

  {
    PartialStore store(dir_path, logkv::createDir | logkv::deleteData);

    PartialObj p1(1, heavyOriginal, 10);
    store.update(k1, p1);

    store.save();
  }

  {
    PartialStore store(dir_path);

    assert(store.getObjects().at(k1).heavyData == heavyOriginal);

    PartialObj p = store.getObjects().at(k1);
    p.counter = 20;
    p.heavyData = heavyTransient;

    store.update(k1, p);
    store.flush();
  }

  {
    PartialStore store(dir_path);
    const auto& obj = store.getObjects().at(k1);

    if (obj.counter != 20) {
      throw std::runtime_error("Partial update failed: counter not updated.");
    }

    if (obj.heavyData == heavyTransient) {
      throw std::runtime_error("FAILURE: The partial update persisted the "
                               "heavy data! Serialization is not porous.");
    }

    if (obj.heavyData != heavyOriginal) {
      throw std::runtime_error(
        "FAILURE: Heavy data corrupted/lost. Expected '" + heavyOriginal +
        "' but found '" + obj.heavyData + "'");
    }
  }

  cleanup_test_directory(dir_path);
  std::cout << "test_store_partial_serialization PASSED." << std::endl;
}

struct MacroPartialObj
    : public logkv::AutoPartialSerializableObject<MacroPartialObj> {
  uint64_t id;
  std::string heavyData;
  uint64_t counter;

  MacroPartialObj() : id(0), counter(0) {}
  MacroPartialObj(uint64_t i, std::string h, uint64_t c)
      : id(i), heavyData(h), counter(c) {}

  auto operator<=>(const MacroPartialObj&) const = default;

  AUTO_SERIALIZABLE_MEMBERS(id, heavyData, counter)

  AUTO_PARTIAL_SERIALIZABLE_MEMBERS(id, counter)
};

void test_store_macro_partial_serialization() {
  std::cout << "Running test_store_macro_partial_serialization..." << std::endl;
  std::string test_name = "macro_partial_serialization";
  std::string dir_path = setup_test_directory(test_name);

  using MacroStore = logkv::Store<std::map, uint64_t, MacroPartialObj>;

  uint64_t k1 = 555;

  std::string heavyOriginal = "MACRO_ORIGINAL_HEAVY_DATA";
  std::string heavyTransient = "MACRO_TRANSIENT_SHOULD_DISAPPEAR";

  {
    MacroStore store(dir_path, logkv::createDir | logkv::deleteData);

    MacroPartialObj p1(1, heavyOriginal, 100);
    store.update(k1, p1);

    store.save();
  }

  {
    MacroStore store(dir_path);

    assert(store.getObjects().at(k1).heavyData == heavyOriginal);

    MacroPartialObj p = store.getObjects().at(k1);
    p.counter = 200;
    p.heavyData = heavyTransient;

    store.update(k1, p);
    store.flush();
  }

  {
    MacroStore store(dir_path);
    const auto& obj = store.getObjects().at(k1);

    if (obj.counter != 200) {
      throw std::runtime_error(
        "Macro partial update failed: counter not updated.");
    }

    if (obj.heavyData == heavyTransient) {
      throw std::runtime_error(
        "FAILURE: The macro partial update persisted the "
        "heavy data! Serialization is not porous.");
    }

    if (obj.heavyData != heavyOriginal) {
      throw std::runtime_error(
        "FAILURE: Heavy data corrupted/lost. Expected '" + heavyOriginal +
        "' but found '" + obj.heavyData + "'");
    }

    {
      std::cout << "  Subtest: Binary Inspection of _setFullSerialization..."
                << std::endl;

      std::string probeHeavy = "BINARY_PROBE_HEAVY_STRING";
      MacroPartialObj probeObj(999, probeHeavy, 777);

      std::vector<char> buffer(1024);

      {
        std::fill(buffer.begin(), buffer.end(), 0);
        size_t written = logkv::serializer<MacroPartialObj>::write(
          buffer.data(), buffer.size(), probeObj);

        uint8_t header = static_cast<uint8_t>(buffer[0]);

        if (header != 0x01) {
          throw std::runtime_error(
            "Binary Probe: Expected header 0x01 (Partial), got " +
            std::to_string(header));
        }

        std::string rawData(buffer.data(), written);
        if (rawData.find(probeHeavy) != std::string::npos) {
          throw std::runtime_error(
            "Binary Probe: Found heavy data in default partial mode!");
        }
      }

      {
        MacroPartialObj::_setFullSerialization(true);

        std::fill(buffer.begin(), buffer.end(), 0);
        size_t written = logkv::serializer<MacroPartialObj>::write(
          buffer.data(), buffer.size(), probeObj);

        uint8_t header = static_cast<uint8_t>(buffer[0]);

        if (header != 0x00) {
          throw std::runtime_error(
            "Binary Probe: Expected header 0x00 (Full), got " +
            std::to_string(header));
        }

        std::string rawData(buffer.data(), written);
        if (rawData.find(probeHeavy) == std::string::npos) {
          throw std::runtime_error(
            "Binary Probe: Heavy data missing in Forced Full mode!");
        }

        MacroPartialObj::_setFullSerialization(false);
      }

      {
        std::fill(buffer.begin(), buffer.end(), 0);
        size_t written [[maybe_unused]] = logkv::serializer<MacroPartialObj>::write(
          buffer.data(), buffer.size(), probeObj);

        uint8_t header = static_cast<uint8_t>(buffer[0]);
        if (header != 0x01) {
          throw std::runtime_error(
            "Binary Probe: Flag reset failed. Expected 0x01, got " +
            std::to_string(header));
        }
      }
    }
  }

  cleanup_test_directory(dir_path);
  std::cout << "test_store_macro_partial_serialization PASSED." << std::endl;
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
    test_store_iterators();
    test_store_partial_serialization();
    test_store_macro_partial_serialization();

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
