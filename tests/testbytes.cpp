#include <logkv/autoser/bytes.h>

#include <cassert>
#include <iomanip>
#include <iostream>

void printBytes(const logkv::Bytes& b, const std::string& label = "") {
  if (!label.empty()) {
    std::cout << label << ": ";
  }
  if (b.empty()) {
    std::cout << "<empty>" << std::endl;
    return;
  }
  std::cout << "Size: " << b.size() << ", Data: [";
  for (size_t i = 0; i < b.size(); ++i) {
    std::cout << std::hex << std::setw(2) << std::setfill('0')
              << static_cast<int>(static_cast<unsigned char>(b[i]));
    if (i < b.size() - 1) {
      std::cout << " ";
    }
  }
  std::cout << std::dec << "]" << std::endl;
}

void test_default_constructor() {
  std::cout << "Running test_default_constructor..." << std::endl;
  logkv::Bytes b;
  assert(b.empty());
  assert(b.size() == 0);
  assert(b.data() == nullptr);
  std::cout << "test_default_constructor PASSED." << std::endl;
}

void test_size_constructor() {
  std::cout << "Running test_size_constructor..." << std::endl;
  logkv::Bytes b0(0);
  assert(b0.empty());
  assert(b0.size() == 0);
  assert(b0.data() == nullptr);

  logkv::Bytes b5(5);
  assert(!b5.empty());
  assert(b5.size() == 5);
  assert(b5.data() != nullptr);
  std::cout << "test_size_constructor PASSED." << std::endl;
}

void test_data_size_constructor() {
  std::cout << "Running test_data_size_constructor..." << std::endl;
  logkv::Bytes b_null(nullptr, 5);
  assert(b_null.empty());
  assert(b_null.size() == 0);
  assert(b_null.data() == nullptr);

  char data[] = "hello";
  size_t data_size = std::strlen(data);

  logkv::Bytes b(data, data_size);
  assert(!b.empty());
  assert(b.size() == data_size);
  assert(b.data() != nullptr);
  assert(std::memcmp(b.data(), data, data_size) == 0);

  logkv::Bytes b_empty_data(data, 0);
  assert(b_empty_data.empty());
  assert(b_empty_data.size() == 0);
  assert(b_empty_data.data() == nullptr);

  std::cout << "test_data_size_constructor PASSED." << std::endl;
}

void test_string_constructor() {
  std::cout << "Running test_string_constructor..." << std::endl;
  std::string s = "world";
  logkv::Bytes b(s);
  assert(!b.empty());
  assert(b.size() == s.size());
  assert(b.data() != nullptr);
  assert(std::memcmp(b.data(), s.data(), s.size()) == 0);

  std::string empty_s = "";
  logkv::Bytes b_empty(empty_s);
  assert(b_empty.empty());
  assert(b_empty.size() == 0);
  assert(b_empty.data() == nullptr);
  std::cout << "test_string_constructor PASSED." << std::endl;
}

void test_vector_constructor() {
  std::cout << "Running test_vector_constructor..." << std::endl;
  std::vector<uint8_t> v = {'a', 'b', 'c', 'd'};
  logkv::Bytes b(v);
  assert(!b.empty());
  assert(b.size() == v.size());
  assert(b.data() != nullptr);
  assert(std::memcmp(b.data(), v.data(), v.size()) == 0);

  std::vector<uint8_t> empty_v;
  logkv::Bytes b_empty(empty_v);
  assert(b_empty.empty());
  assert(b_empty.size() == 0);
  assert(b_empty.data() == nullptr);
  std::cout << "test_vector_constructor PASSED." << std::endl;
}

void test_copy_constructor() {
  std::cout << "Running test_copy_constructor..." << std::endl;
  logkv::Bytes original("original");
  logkv::Bytes copy(original);

  assert(copy.size() == original.size());
  assert(copy.data() != original.data());
  assert(std::memcmp(copy.data(), original.data(), original.size()) == 0);

  logkv::Bytes empty_original;
  logkv::Bytes empty_copy(empty_original);
  assert(empty_copy.empty());
  assert(empty_copy.data() == nullptr);
  std::cout << "test_copy_constructor PASSED." << std::endl;
}

void test_move_constructor() {
  std::cout << "Running test_move_constructor..." << std::endl;
  logkv::Bytes original("move_me");
  char* orig_data_ptr = original.data();
  size_t orig_size = original.size();

  logkv::Bytes moved(std::move(original));

  assert(moved.size() == orig_size);
  assert(moved.data() == orig_data_ptr);
  assert(original.empty());
  assert(original.data() == nullptr);
  assert(original.size() == 0);

  logkv::Bytes empty_original;
  logkv::Bytes empty_moved(std::move(empty_original));
  assert(empty_moved.empty());
  assert(empty_moved.data() == nullptr);
  assert(empty_original.empty());
  assert(empty_original.data() == nullptr);

  std::cout << "test_move_constructor PASSED." << std::endl;
}

void test_copy_assignment() {
  std::cout << "Running test_copy_assignment..." << std::endl;
  logkv::Bytes b1("one");
  logkv::Bytes b2("two_long");

  b1 = b2;
  assert(b1.size() == b2.size());
  assert(b1.data() != b2.data());
  assert(std::memcmp(b1.data(), b2.data(), b1.size()) == 0);

  logkv::Bytes b3("three_long");
  logkv::Bytes b4("four");
  b3 = b4;
  assert(b3.size() == b4.size());
  assert(b3.data() != b4.data());
  assert(std::memcmp(b3.data(), b4.data(), b3.size()) == 0);

  logkv::Bytes b5("five");
  b5 = b5;
  assert(b5.size() == 4);
  assert(std::memcmp(b5.data(), "five", 4) == 0);

  logkv::Bytes b6("six");
  logkv::Bytes empty_b;
  b6 = empty_b;
  assert(b6.empty());
  assert(b6.data() == nullptr);

  logkv::Bytes b7;
  logkv::Bytes non_empty_b("not_empty");
  b7 = non_empty_b;
  assert(b7.size() == non_empty_b.size());
  assert(b7.data() != non_empty_b.data());
  assert(std::memcmp(b7.data(), non_empty_b.data(), b7.size()) == 0);

  std::cout << "test_copy_assignment PASSED." << std::endl;
}

void test_string_assignment() {
  std::cout << "Running test_string_assignment..." << std::endl;
  logkv::Bytes b;
  std::string s = "assign_string";
  b = s;
  assert(b.size() == s.size());
  assert(std::memcmp(b.data(), s.data(), s.size()) == 0);

  std::string empty_s = "";
  b = empty_s;
  assert(b.empty());
  assert(b.data() == nullptr);
  std::cout << "test_string_assignment PASSED." << std::endl;
}

void test_vector_assignment() {
  std::cout << "Running test_vector_assignment..." << std::endl;
  logkv::Bytes b;
  std::vector<uint8_t> v = {'v', 'e', 'c'};
  b = v;
  assert(b.size() == v.size());
  assert(std::memcmp(b.data(), v.data(), v.size()) == 0);

  std::vector<uint8_t> empty_v;
  b = empty_v;
  assert(b.empty());
  assert(b.data() == nullptr);
  std::cout << "test_vector_assignment PASSED." << std::endl;
}

void test_move_assignment() {
  std::cout << "Running test_move_assignment..." << std::endl;
  logkv::Bytes b1("initial_b1");
  logkv::Bytes b2("move_source");
  char* b2_data_ptr = b2.data();
  size_t b2_size = b2.size();

  b1 = std::move(b2);
  assert(b1.size() == b2_size);
  assert(b1.data() == b2_data_ptr);
  assert(b2.empty());
  assert(b2.data() == nullptr);

  logkv::Bytes b4;
  logkv::Bytes b5("move_to_empty");
  char* b5_data_ptr = b5.data();
  size_t b5_size = b5.size();
  b4 = std::move(b5);
  assert(b4.size() == b5_size);
  assert(b4.data() == b5_data_ptr);
  assert(b5.empty());

  logkv::Bytes b6("move_empty_source");
  logkv::Bytes b7;
  b6 = std::move(b7);
  assert(b6.empty());
  assert(b7.empty());

  std::cout << "test_move_assignment PASSED." << std::endl;
}

void test_empty_size_data_accessors() {
  std::cout << "Running test_empty_size_data_accessors..." << std::endl;
  logkv::Bytes b;
  assert(b.empty());
  assert(b.size() == 0);
  assert(b.data() == nullptr);

  logkv::Bytes b_content("content");
  assert(!b_content.empty());
  assert(b_content.size() == 7);
  assert(b_content.data() != nullptr);
  assert(std::memcmp(b_content.data(), "content", 7) == 0);
  std::cout << "test_empty_size_data_accessors PASSED." << std::endl;
}

void test_clear() {
  std::cout << "Running test_clear..." << std::endl;
  logkv::Bytes b("not_empty");
  assert(!b.empty());
  b.clear();
  assert(b.empty());
  assert(b.size() == 0);
  assert(b.data() == nullptr);

  b.clear();
  assert(b.empty());
  std::cout << "test_clear PASSED." << std::endl;
}

void test_resize() {
  std::cout << "Running test_resize..." << std::endl;
  logkv::Bytes b("hello");

  b.resize(10);
  assert(b.size() == 10);
  assert(std::memcmp(b.data(), "hello", 5) == 0);

  b.resize(3);
  assert(b.size() == 3);
  assert(std::memcmp(b.data(), "hel", 3) == 0);

  b.resize(0);
  assert(b.empty());
  assert(b.data() == nullptr);

  logkv::Bytes b2;
  b2.resize(5);
  assert(b2.size() == 5);
  assert(b2.data() != nullptr);

  b2.resize(5);
  assert(b2.size() == 5);

  std::cout << "test_resize PASSED." << std::endl;
}

void test_wrap() {
  std::cout << "Running test_wrap..." << std::endl;

  logkv::Bytes b("initial");
  char* new_data = new char[4];
  std::memcpy(new_data, "wrap", 4);
  b.wrap(new_data, 4);
  assert(b.size() == 4);
  assert(b.data() == new_data);
  assert(std::memcmp(b.data(), "wrap", 4) == 0);

  logkv::Bytes b2;
  b2.wrap(nullptr, 0);
  assert(b2.empty());
  assert(b2.size() == 0);
  assert(b2.data() == nullptr);

  std::cout << "test_wrap PASSED." << std::endl;
}

void test_to_string_to_vector() {
  std::cout << "Running test_to_string_to_vector..." << std::endl;
  std::string s_orig = "test_data";
  logkv::Bytes b(s_orig);

  std::string s_conv = b.toString();
  assert(s_conv == s_orig);

  std::vector<uint8_t> v_orig(s_orig.begin(), s_orig.end());
  std::vector<uint8_t> v_conv = b.toVector();
  assert(v_conv.size() == v_orig.size());
  assert(std::equal(v_conv.begin(), v_conv.end(), v_orig.begin()));

  logkv::Bytes empty_b;
  assert(empty_b.toString() == "");
  assert(empty_b.toVector().empty());
  std::cout << "test_to_string_to_vector PASSED." << std::endl;
}

void test_comparison_operators() {
  std::cout << "Running test_comparison_operators..." << std::endl;
  logkv::Bytes b1("abc");
  logkv::Bytes b2("abc");
  logkv::Bytes b3("abd");
  logkv::Bytes b4("ab");
  logkv::Bytes b5("abcd");
  logkv::Bytes b6("bbc");
  logkv::Bytes empty1, empty2;

  assert(b1 < b6);
  assert((b1 <=> b6) == std::strong_ordering::less);

  assert(b1 == b2);
  assert(!(b1 == b3));
  assert((b1 <=> b2) == std::strong_ordering::equal);

  assert(b1 != b3);
  assert((b1 <=> b3) == std::strong_ordering::less);
  assert((b3 <=> b1) == std::strong_ordering::greater);

  assert(b4 < b1);
  assert((b4 <=> b1) == std::strong_ordering::less);

  assert(b5 > b1);
  assert((b5 <=> b1) == std::strong_ordering::greater);

  assert(empty1 == empty2);
  assert((empty1 <=> empty2) == std::strong_ordering::equal);
  assert(empty1 < b1);
  assert((empty1 <=> b1) == std::strong_ordering::less);
  assert(b1 > empty1);
  assert((b1 <=> empty1) == std::strong_ordering::greater);

  assert(b1 == b1);
  assert((b1 <=> b1) == std::strong_ordering::equal);

  std::cout << "test_comparison_operators PASSED." << std::endl;
}

void test_element_access() {
  std::cout << "Running test_element_access..." << std::endl;
  logkv::Bytes b("hello");
  assert(b[0] == 'h');
  assert(b[4] == 'o');

  b[0] = 'J';
  assert(b[0] == 'J');
  assert(b.toString() == "Jello");

  const logkv::Bytes cb("const");
  assert(cb[0] == 'c');

  std::cout << "test_element_access PASSED." << std::endl;
}

void test_hex_encode_decode() {
  std::cout << "Running test_hex_encode_decode..." << std::endl;
  char data1[] = "\x01\x23\x45\x67\x89\xab\xcd\xef";
  size_t size1 = 8;
  std::string hex_lower1 = "0123456789abcdef";
  std::string hex_upper1 = "0123456789ABCDEF";

  logkv::Bytes b_data1(data1, size1);

  logkv::Bytes encoded_lower1 = logkv::Bytes::encodeHex(b_data1, false);
  assert(encoded_lower1.toString() == hex_lower1);
  logkv::Bytes decoded1 = logkv::Bytes::decodeHex(encoded_lower1);
  assert(decoded1 == b_data1);

  logkv::Bytes encoded_upper1 = logkv::Bytes::encodeHex(b_data1, true);
  assert(encoded_upper1.toString() == hex_upper1);
  decoded1 = logkv::Bytes::decodeHex(encoded_upper1);
  assert(decoded1 == b_data1);

  std::string str_data = "Hello World!";
  std::string hex_str_data_lower = "48656c6c6f20576f726c6421";
  std::string hex_str_data_upper = "48656C6C6F20576F726C6421";

  logkv::Bytes encoded_s_lower = logkv::Bytes::encodeHex(str_data, false);
  assert(encoded_s_lower.toString() == hex_str_data_lower);
  logkv::Bytes decoded_s = logkv::Bytes::decodeHex(hex_str_data_lower);
  assert(decoded_s.toString() == str_data);

  logkv::Bytes encoded_s_upper = logkv::Bytes::encodeHex(str_data, true);
  assert(encoded_s_upper.toString() == hex_str_data_upper);
  decoded_s = logkv::Bytes::decodeHex(hex_str_data_upper);
  assert(decoded_s.toString() == str_data);

  logkv::Bytes empty_b;
  logkv::Bytes encoded_empty = logkv::Bytes::encodeHex(empty_b);
  assert(encoded_empty.empty());
  logkv::Bytes decoded_empty = logkv::Bytes::decodeHex(encoded_empty);
  assert(decoded_empty.empty());

  decoded_empty = logkv::Bytes::decodeHex("", 0);
  assert(decoded_empty.empty());

  bool caught = false;
  try {
    logkv::Bytes::decodeHex("123", 3);
  } catch (const std::invalid_argument&) {
    caught = true;
  }
  assert(caught);

  caught = false;
  try {
    logkv::Bytes::decodeHex("1G", 2);
  } catch (const std::invalid_argument&) {
    caught = true;
  }
  assert(caught);

  logkv::Bytes decoded_from_str_hex = logkv::Bytes::decodeHex(hex_lower1);
  assert(decoded_from_str_hex == b_data1);

  logkv::Bytes hex_bytes_obj(hex_lower1);
  logkv::Bytes decoded_from_bytes_hex = logkv::Bytes::decodeHex(hex_bytes_obj);
  assert(decoded_from_bytes_hex == b_data1);

  std::cout << "test_hex_encode_decode PASSED." << std::endl;
}

#include <logkv/autoser.h> // Make sure this is included for VarUint

void test_serialize_deserialize() {
  std::cout << "Running test_serialize_deserialize..." << std::endl;

  using BytesSerializer = logkv::serializer<logkv::Bytes>;
  using VarUintSerializer = logkv::serializer<logkv::VarUint<uint64_t>>;

  logkv::Bytes b_orig("serialize_me");

  size_t req_size = BytesSerializer::get_size(b_orig);
  size_t expected_size =
    b_orig.size() + VarUintSerializer::get_size(b_orig.size());
  assert(req_size == expected_size);

  std::vector<char> buffer(req_size);
  size_t written_size =
    BytesSerializer::write(buffer.data(), buffer.size(), b_orig);
  assert(written_size == req_size);

  logkv::Bytes b_deserial;
  size_t read_size =
    BytesSerializer::read(buffer.data(), buffer.size(), b_deserial);
  assert(read_size == req_size);
  assert(b_deserial == b_orig);

  logkv::Bytes b_empty_orig;
  req_size = BytesSerializer::get_size(b_empty_orig);
  expected_size = VarUintSerializer::get_size(0);
  assert(req_size == expected_size);
  assert(req_size == 1);

  std::vector<char> empty_buffer(req_size);
  written_size = BytesSerializer::write(empty_buffer.data(),
                                        empty_buffer.size(), b_empty_orig);
  assert(written_size == req_size);

  logkv::Bytes b_empty_deserial;
  read_size = BytesSerializer::read(empty_buffer.data(), empty_buffer.size(),
                                    b_empty_deserial);
  assert(read_size == req_size);
  assert(b_empty_deserial.empty());
  assert(b_empty_deserial == b_empty_orig);

  logkv::Bytes b_orig_long(
    "a_very_long_string_for_serialization_to_test_varuint");
  req_size = BytesSerializer::get_size(b_orig_long);
  std::vector<char> full_buffer_long(req_size);
  BytesSerializer::write(full_buffer_long.data(), full_buffer_long.size(),
                         b_orig_long);

  logkv::Bytes b_partial_deserial;
  const size_t len_prefix_size =
    VarUintSerializer::get_size(b_orig_long.size());

  read_size = BytesSerializer::read(full_buffer_long.data(), len_prefix_size,
                                    b_partial_deserial);
  assert(read_size == req_size);
  assert(b_partial_deserial.empty());

  if (len_prefix_size > 0) {
    read_size = BytesSerializer::read(full_buffer_long.data(),
                                      len_prefix_size - 1, b_partial_deserial);
    assert(read_size == len_prefix_size);
    assert(b_partial_deserial.empty());
  }

  logkv::Bytes b_reuse("short");
  BytesSerializer::read(full_buffer_long.data(), full_buffer_long.size(),
                        b_reuse);
  assert(b_reuse == b_orig_long);

  std::cout << "test_serialize_deserialize PASSED." << std::endl;
}

void test_hash_class() {
  std::cout << "Running test_hash_class..." << std::endl;
  logkv::Bytes b("some_data_for_hash");
  logkv::Hash h_from_bytes(b);
  assert(h_from_bytes == b);

  logkv::Hash h_default;
  assert(h_default.empty());

  logkv::Hash h_copy_construct(h_from_bytes);
  assert(h_copy_construct == h_from_bytes);

  logkv::Hash h_move_construct(std::move(h_copy_construct));
  assert(h_move_construct == h_from_bytes);
  assert(h_copy_construct.empty());

  logkv::Hash h_assign;
  h_assign = h_from_bytes;
  assert(h_assign == h_from_bytes);

  logkv::Hash h_move_assign;
  h_move_assign = std::move(h_assign);
  assert(h_move_assign == h_from_bytes);
  assert(h_assign.empty());

  std::cout << "test_hash_class PASSED." << std::endl;
}

void test_hash_value_hash() {
  std::cout << "Running test_hash_value_hash..." << std::endl;

  char data_long[] = {0x01, 0x02, 0x03, 0x04, 0x05,
                      0x06, 0x07, 0x08, 0x09, 0x0A};
  logkv::Bytes b_long(data_long, sizeof(data_long));
  logkv::Hash h_long(b_long);
  size_t hv_long = logkv::hash_value(h_long);

  size_t expected_hv_long = 0;
  std::memcpy(&expected_hv_long, data_long,
              std::min(sizeof(data_long), sizeof(size_t)));
  assert(hv_long == expected_hv_long);

  char data_short[] = {0x11, 0x22, 0x33};
  logkv::Bytes b_short(data_short, sizeof(data_short));

  logkv::Hash h_short(b_short);
  size_t hv_short = logkv::hash_value(h_short);

  size_t expected_hv_short = 0;
  std::memcpy(&expected_hv_short, data_short, sizeof(data_short));
  assert(hv_short == expected_hv_short);

  logkv::Hash h_empty;
  size_t hv_empty = logkv::hash_value(h_empty);
  assert(hv_empty == 0);

  std::cout << "test_hash_value_hash PASSED." << std::endl;
}

void test_std_hash_specializations() {
  std::cout << "Running test_std_hash_specializations..." << std::endl;
  std::hash<logkv::Bytes> bytes_hasher;
  std::hash<logkv::Hash> hash_hasher;

  logkv::Bytes b("test_std_hash_bytes");
  logkv::Hash h(logkv::Bytes("test_std_hash_hash"));

  assert(bytes_hasher(b) == logkv::hash_value(b));
  assert(hash_hasher(h) == logkv::hash_value(h));

  std::cout << "test_std_hash_specializations PASSED." << std::endl;
}

int main() {
  try {
    test_default_constructor();
    test_size_constructor();
    test_data_size_constructor();
    test_string_constructor();
    test_vector_constructor();
    test_copy_constructor();
    test_move_constructor();

    test_copy_assignment();
    test_string_assignment();
    test_vector_assignment();
    test_move_assignment();

    test_empty_size_data_accessors();
    test_clear();
    test_resize();
    test_wrap();

    test_to_string_to_vector();
    test_comparison_operators();
    test_element_access();

    test_hex_encode_decode();
    test_serialize_deserialize();

    test_hash_class();
    test_hash_value_hash();
    test_std_hash_specializations();

    std::cout << "All tests PASSED." << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "A test FAILED with exception: " << e.what() << std::endl;
    return 1;
  } catch (...) {
    std::cerr << "A test FAILED with an unknown exception." << std::endl;
    return 1;
  }

  return 0;
}
