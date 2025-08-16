#include <logkv/autoser/asio.h>
#include <logkv/autoser/associative.h>
#include <logkv/autoser/bytes.h>
#include <logkv/autoser/pfr.h>
#include <logkv/autoser/pushback.h>

#include <iostream>
#include <sstream>

static int tests_passed = 0;
static int tests_failed = 0;

#define RUN_TEST(test_func)                                                    \
  std::cout << "--- Running " << #test_func << "..." << std::endl;             \
  try {                                                                        \
    test_func();                                                               \
    std::cout << "--- PASSED: " << #test_func << std::endl << std::endl;       \
    tests_passed++;                                                            \
  } catch (const std::exception& e) {                                          \
    std::cerr << "--- FAILED: " << #test_func << " with exception:\n    "      \
              << e.what() << std::endl                                         \
              << std::endl;                                                    \
    tests_failed++;                                                            \
  } catch (...) {                                                              \
    std::cerr << "--- FAILED: " << #test_func << " with an unknown exception." \
              << std::endl                                                     \
              << std::endl;                                                    \
    tests_failed++;                                                            \
  }

#define ASSERT_TRUE(condition)                                                 \
  if (!(condition)) {                                                          \
    throw std::runtime_error("Assertion failed at " + std::string(__FILE__) +  \
                             ":" + std::to_string(__LINE__) +                  \
                             ": " #condition);                                 \
  }

#define ASSERT_EQ(a, b)                                                        \
  if (!((a) == (b))) {                                                         \
    std::stringstream ss;                                                      \
    ss << "Equality assertion failed at " << __FILE__ << ":" << __LINE__       \
       << ": " << #a << " == " << #b;                                          \
    throw std::runtime_error(ss.str());                                        \
  }

#define ASSERT_THROW(expression, ExceptionType)                                \
  {                                                                            \
    bool thrown = false;                                                       \
    try {                                                                      \
      expression;                                                              \
    } catch (const ExceptionType&) {                                           \
      thrown = true;                                                           \
    } catch (...) {                                                            \
    }                                                                          \
    if (!thrown) {                                                             \
      throw std::runtime_error("Assertion failed at " +                        \
                               std::string(__FILE__) + ":" +                   \
                               std::to_string(__LINE__) +                      \
                               ": Expected exception " #ExceptionType          \
                               " was not thrown for " #expression);            \
    }                                                                          \
  }

namespace logkv {
template <typename T>
std::ostream& operator<<(std::ostream& os, const VarUint<T>& v) {
  os << v.value;
  return os;
}
} // namespace logkv

// -----------------------------------------------------------------------------
// Test classes
// -----------------------------------------------------------------------------

class OpaqueComposite {
public:
  OpaqueComposite() : i_(0) {};

  // comment-out this line to see a compile-time serialization error
  OpaqueComposite(const uint16_t i, const std::string& s) : i_(i), s_(s) {};

  uint16_t i_;
  std::string s_;
  auto operator<=>(const OpaqueComposite&) const = default;
};

namespace logkv {
template <> struct composite_traits<OpaqueComposite> {
  using member_types = std::tuple<uint16_t, std::string>;
  // The methods below also work, but defeats the point of a test with an
  // "opaque" object that we don't control. For those types (T) that you own,
  // you should instead just subclass AutoSerializableObject<T> which is easier.
  //
  // static auto get_members_by_reference(OpaqueComposite& o) {
  //   return std::tie(o.i_, o.s_);
  // }
  // static auto get_members_by_const_reference(const OpaqueComposite& o) {
  //   return std::tie(o.i_, o.s_);
  // }
  //
  static auto get_members_by_value(const OpaqueComposite& o) {
    return std::make_tuple(o.i_, o.s_);
  }
};
} // namespace logkv

class MyTestObject : public logkv::AutoSerializableObject<MyTestObject> {
public:
  int32_t a = 0;
  logkv::VarUint<uint64_t> b = 0;
  std::string c;
  OpaqueComposite d;

  MyTestObject() = default;
  MyTestObject(int32_t a_v, uint64_t b_v, std::string c_v, OpaqueComposite d_v)
      : a(a_v), b(b_v), c(std::move(c_v)), d(std::move(d_v)) {}

  AUTO_SERIALIZABLE_MEMBERS(a, b, c, d)
  auto operator<=>(const MyTestObject&) const = default;
};

using LeafTuple = std::tuple<uint32_t, boost::asio::ip::address, logkv::Bytes,
                             OpaqueComposite, std::string>;
using Level1Container = std::vector<LeafTuple>;
using Level2Container = std::map<std::string, Level1Container>;
using DeeplyNestedContainer = std::list<Level2Container>;

class MonsterObject : public logkv::AutoSerializableObject<MonsterObject> {
public:
  uint64_t id_{};
  std::string name_;
  DeeplyNestedContainer nested_data_;
  MonsterObject() = default;
  MonsterObject(uint64_t id, std::string name, DeeplyNestedContainer data)
      : id_(id), name_(std::move(name)), nested_data_(std::move(data)) {}
  AUTO_SERIALIZABLE_MEMBERS(id_, name_, nested_data_)
  auto operator<=>(const MonsterObject&) const = default;
};

std::ostream& operator<<(std::ostream& os, const OpaqueComposite& obj) {
  os << "OpaqueComposite{i_: " << obj.i_ << ", s_: \"" << obj.s_ << "\"}";
  return os;
}

std::ostream& operator<<(std::ostream& os, const MyTestObject& obj) {
  os << "MyTestObject {\n"
     << "  a: " << obj.a << ",\n"
     << "  b: " << obj.b << ",\n"
     << "  c: \"" << obj.c << "\",\n"
     << "  d: " << obj.d << "\n"
     << "}";
  return os;
}

std::ostream& operator<<(std::ostream& os, const MonsterObject& obj) {
  os << "MonsterObject {\n"
     << "  id_: " << obj.id_ << ",\n"
     << "  name_: \"" << obj.name_ << "\",\n"
     << "  nested_data_: [";
  for (const auto& map_item : obj.nested_data_) {
    os << "\n    {\n";
    for (const auto& pair : map_item) {
      os << "      \"" << pair.first << "\": " << pair.second.size()
         << " elements,\n";
    }
    os << "    },";
  }
  os << "\n  ]\n}";
  return os;
}

struct SimplePfrAggregate {
  int32_t a;
  std::string b;
  auto operator<=>(const SimplePfrAggregate&) const = default;
};

struct NestedPfrAggregate {
  uint64_t id;
  SimplePfrAggregate simple;
  logkv::Bytes data;
  std::vector<int> numbers;
  auto operator<=>(const NestedPfrAggregate&) const = default;
};

struct PfrWithMethod {
  int x;
  int y;
  int sum() const { return x + y; }
  auto operator<=>(const PfrWithMethod&) const = default;
};

static_assert(std::is_aggregate_v<PfrWithMethod>);

struct EmptyPfrAggregate {
  auto operator<=>(const EmptyPfrAggregate&) const = default;
};

// -----------------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------------

template <typename T> void test_type_serialization(const T& original) {
  size_t required_size = logkv::serializer<T>::get_size(original);

  if (required_size > 1) {
    std::vector<char> small_buffer(required_size - 1);
    size_t needed = logkv::serializer<T>::write(small_buffer.data(),
                                                small_buffer.size(), original);
    ASSERT_EQ(needed, required_size);
  }

  std::vector<char> buffer(required_size);
  size_t bytes_written =
    logkv::serializer<T>::write(buffer.data(), buffer.size(), original);
  ASSERT_EQ(bytes_written, required_size);

  if (required_size > 1) {
    T temp_obj{};
    std::vector<char> small_buffer_read(required_size - 1);
    memcpy(small_buffer_read.data(), buffer.data(), small_buffer_read.size());
    size_t needed = logkv::serializer<T>::read(
      small_buffer_read.data(), small_buffer_read.size(), temp_obj);
    ASSERT_EQ(needed, required_size);
  }

  T deserialized{};
  size_t bytes_read =
    logkv::serializer<T>::read(buffer.data(), buffer.size(), deserialized);
  ASSERT_EQ(bytes_read, required_size);

  ASSERT_EQ(original, deserialized);
}

std::vector<char> encode_varuint(uint64_t val) {
  std::vector<char> result;
  if (val == 0) {
    result.push_back(0);
    return result;
  }
  while (val > 0) {
    uint8_t byte = val & 0x7F;
    val >>= 7;
    if (val > 0) {
      byte |= 0x80;
    }
    result.push_back(static_cast<char>(byte));
  }
  return result;
}

// -----------------------------------------------------------------------------
// Test definitions
// -----------------------------------------------------------------------------

void test_arithmetic_integers() {
  test_type_serialization<int32_t>(123456);
  test_type_serialization<int32_t>(-123456);
  test_type_serialization<uint64_t>(0);
  test_type_serialization<uint64_t>(9876543210ULL);
  test_type_serialization<uint64_t>(std::numeric_limits<uint64_t>::max());
}

void test_arithmetic_big_endian_check() {
  int32_t original = 0x01020304;
  if (std::endian::native == std::endian::big) {
    original = 0x04030201;
  }
  std::vector<char> buffer(sizeof(original));
  logkv::serializer<int32_t>::write(buffer.data(), buffer.size(), original);

  ASSERT_EQ(static_cast<uint8_t>(buffer[0]), 0x01);
  ASSERT_EQ(static_cast<uint8_t>(buffer[1]), 0x02);
  ASSERT_EQ(static_cast<uint8_t>(buffer[2]), 0x03);
  ASSERT_EQ(static_cast<uint8_t>(buffer[3]), 0x04);
}

void test_varuint_basic_values() {
  test_type_serialization<logkv::VarUint<uint32_t>>(0);
  test_type_serialization<logkv::VarUint<uint32_t>>(127);
  test_type_serialization<logkv::VarUint<uint32_t>>(128);
  test_type_serialization<logkv::VarUint<uint32_t>>(16383);
  test_type_serialization<logkv::VarUint<uint32_t>>(16384);
}

void test_varuint_max_value() {
  test_type_serialization<logkv::VarUint<uint64_t>>(
    std::numeric_limits<uint64_t>::max());
}

void test_varuint_read_overflow() {
  std::vector<char> overflow_buffer(6, 0x80);
  overflow_buffer[5] = 0x01;
  logkv::VarUint<uint32_t> val32;
  ASSERT_THROW(logkv::serializer<logkv::VarUint<uint32_t>>::read(
                 overflow_buffer.data(), overflow_buffer.size(), val32),
               std::runtime_error);
}

void test_varuint_read_value_overflow() {
  std::vector<char> buffer = {(char)0x80, (char)0x80, (char)0x80, (char)0x80,
                              (char)0x10};
  logkv::VarUint<uint32_t> val32;
  ASSERT_THROW(logkv::serializer<logkv::VarUint<uint32_t>>::read(
                 buffer.data(), buffer.size(), val32),
               std::runtime_error);
}

void test_container_std_string() {
  test_type_serialization<std::string>("");
  test_type_serialization<std::string>("hello world");
  test_type_serialization<std::string>(std::string(250, 'a'));
  test_type_serialization<std::string>(std::string("hello\0world", 11));
}

void test_container_read_size_limits() {
  {
    uint64_t large_size = logkv::MAX_AUTOSER_BYTES + 1;
    std::vector<char> buffer = encode_varuint(large_size);
    std::string s;
    ASSERT_THROW(
      logkv::serializer<std::string>::read(buffer.data(), buffer.size(), s),
      std::runtime_error);
  }
  {
    uint64_t large_size = logkv::MAX_AUTOSER_BYTES + 1;
    std::vector<char> buffer = encode_varuint(large_size);
    std::vector<uint8_t> v;
    ASSERT_THROW(logkv::serializer<std::vector<uint8_t>>::read(
                   buffer.data(), buffer.size(), v),
                 std::runtime_error);
  }
  {
    uint64_t large_count = logkv::MAX_AUTOSER_ITEMS + 1;
    std::vector<char> buffer = encode_varuint(large_count);
    std::vector<int> v;
    ASSERT_THROW(logkv::serializer<std::vector<int>>::read(buffer.data(),
                                                           buffer.size(), v),
                 std::runtime_error);
  }
}

void test_container_std_array() {
  std::array<uint8_t, 16> arr;
  for (size_t i = 0; i < arr.size(); ++i) {
    arr[i] = static_cast<uint8_t>(i);
  }
  test_type_serialization<std::array<uint8_t, 16>>(arr);
}

void test_container_std_array_complex() {
  std::array<uint64_t, 4> arr_ints = {1, 2, 9999999999ULL, 0};
  test_type_serialization(arr_ints);

  std::array<std::string, 3> arr_strs = {"first", "second string", ""};
  test_type_serialization(arr_strs);

  std::array<std::string, 0> arr_empty = {};
  test_type_serialization(arr_empty);
}

void test_container_std_vector_complex() {
  std::vector<std::string> vec_strs = {"alpha", "beta", "gamma", "", "delta"};
  test_type_serialization(vec_strs);

  std::vector<OpaqueComposite> vec_agg;
  vec_agg.push_back(OpaqueComposite(10, "ten"));
  vec_agg.push_back(OpaqueComposite(20, "twenty"));
  test_type_serialization(vec_agg);

  std::vector<int32_t> empty_vec;
  test_type_serialization(empty_vec);

  std::vector<std::vector<std::string>> nested_vec = {
    {"a", "b"}, {"c"}, {}, {"d", "e", "f"}};
  test_type_serialization(nested_vec);
}

void test_container_associative() {
  std::map<std::string, int> m = {{"a", 1}, {"b", 2}};
  test_type_serialization(m);
  std::set<std::string> s = {"x", "y", "z"};
  test_type_serialization(s);

  std::unordered_map<int, std::string> um = {{10, "ten"}, {20, "twenty"}};
  test_type_serialization(um);
  std::unordered_set<int> us = {100, 200, 300};
  test_type_serialization(us);

  test_type_serialization(std::map<int, int>());
  test_type_serialization(std::set<int>());
  test_type_serialization(std::unordered_map<int, int>());
  test_type_serialization(std::unordered_set<int>());
}

void test_container_sequential_other() {
  std::list<int> l = {1, 2, 3, 4, 5};
  test_type_serialization(l);
  std::list<OpaqueComposite> l_complex;
  l_complex.push_back(OpaqueComposite(1, "one"));
  l_complex.push_back(OpaqueComposite(2, "two"));
  test_type_serialization(l_complex);

  std::deque<std::string> d = {"a", "b", "c"};
  test_type_serialization(d);

  test_type_serialization(std::list<int>());
  test_type_serialization(std::deque<int>());
}

void test_container_std_span() {
  {
    std::vector<uint8_t> original_vec = {1, 2, 3, 4, 5, 6, 7, 8};
    std::span<const uint8_t> src_span(original_vec);
    std::vector<char> buffer(
      logkv::serializer<decltype(src_span)>::get_size(src_span));
    logkv::serializer<decltype(src_span)>::write(buffer.data(), buffer.size(),
                                                 src_span);
    std::vector<uint8_t> dest_vec(original_vec.size(), 0);
    std::span<uint8_t> dest_span(dest_vec);
    logkv::serializer<decltype(dest_span)>::read(buffer.data(), buffer.size(),
                                                 dest_span);
    ASSERT_EQ(original_vec, dest_vec);
  }
  {
    uint8_t original_arr[] = {10, 20, 30, 40};
    std::span<const uint8_t> src_span(original_arr);
    std::vector<char> buffer(
      logkv::serializer<decltype(src_span)>::get_size(src_span));
    logkv::serializer<decltype(src_span)>::write(buffer.data(), buffer.size(),
                                                 src_span);
    uint8_t dest_arr[4] = {0};
    std::span<uint8_t> dest_span(dest_arr);
    logkv::serializer<decltype(dest_span)>::read(buffer.data(), buffer.size(),
                                                 dest_span);
    ASSERT_TRUE(std::equal(std::begin(original_arr), std::end(original_arr),
                           std::begin(dest_arr)));
  }
  {
    logkv::Bytes original_bytes("some_raw_data");
    std::span<const uint8_t> src_span(
      reinterpret_cast<const uint8_t*>(original_bytes.data()),
      original_bytes.size());
    std::vector<char> buffer(
      logkv::serializer<decltype(src_span)>::get_size(src_span));
    logkv::serializer<decltype(src_span)>::write(buffer.data(), buffer.size(),
                                                 src_span);
    logkv::Bytes dest_bytes(original_bytes.size());
    std::span<uint8_t> dest_span(reinterpret_cast<uint8_t*>(dest_bytes.data()),
                                 dest_bytes.size());
    logkv::serializer<decltype(dest_span)>::read(buffer.data(), buffer.size(),
                                                 dest_span);
    ASSERT_EQ(original_bytes, dest_bytes);
  }
}

void test_asio_ip_address() {
  test_type_serialization<boost::asio::ip::address>(
    boost::asio::ip::make_address("192.168.1.1"));
  test_type_serialization<boost::asio::ip::address>(
    boost::asio::ip::make_address("2001:0db8:85a3::8a2e:0370:7334"));
  test_type_serialization<boost::asio::ip::address>(boost::asio::ip::address());
}

void test_asio_tcp_endpoint() {
  using boost::asio::ip::tcp;
  tcp::endpoint ep_v4(boost::asio::ip::make_address("8.8.8.8"), 53);
  test_type_serialization<tcp::endpoint>(ep_v4);

  tcp::endpoint ep_v6(boost::asio::ip::make_address("2001:4860:4860::8888"),
                      53);
  test_type_serialization<tcp::endpoint>(ep_v6);
}

void test_writer_reader_sequential_ops() {
  char buffer[1024];
  logkv::Writer writer(buffer, sizeof(buffer));
  int32_t i_orig = -500;
  std::string s_orig = "test string";
  uint16_t u_orig = 8080;
  writer.write(i_orig);
  writer.write(s_orig);
  writer.write(u_orig);
  size_t total_written = writer.bytes_processed();
  ASSERT_TRUE(total_written > 0);

  logkv::Reader reader(buffer, total_written);
  int32_t i_read;
  std::string s_read;
  uint16_t u_read;
  reader.read(i_read);
  reader.read(s_read);
  reader.read(u_read);

  ASSERT_EQ(reader.bytes_processed(), total_written);
  ASSERT_EQ(i_orig, i_read);
  ASSERT_EQ(s_orig, s_read);
  ASSERT_EQ(u_orig, u_read);
}

void test_writer_reader_insufficient_buffer() {
  char buffer[10];
  logkv::Writer writer(buffer, sizeof(buffer));
  writer.write<int32_t>(1);
  writer.write<int32_t>(2);
  ASSERT_THROW(writer.write(std::string("this is too long")),
               logkv::insufficient_buffer);
}

void test_composite_opaque_type() {
  test_type_serialization(OpaqueComposite{1234, "opaque string"});
  test_type_serialization(OpaqueComposite{});
}

void test_autoserializable_object() {
  MyTestObject original(-123, 999999999, "this is a test",
                        {5678, "nested opaque"});

  size_t required_size = logkv::serializer<MyTestObject>::get_size(original);
  ASSERT_TRUE(required_size > 0);

  std::vector<char> buffer(required_size);
  size_t bytes_written = logkv::serializer<MyTestObject>::write(
    buffer.data(), buffer.size(), original);
  ASSERT_EQ(bytes_written, required_size);

  MyTestObject deserialized;
  size_t bytes_read = logkv::serializer<MyTestObject>::read(
    buffer.data(), buffer.size(), deserialized);
  ASSERT_EQ(bytes_read, required_size);

  if (original != deserialized) {
    std::cerr << "Equality failed in test_autoserializable_object:\n"
              << "Original:\n"
              << original << "\n"
              << "Deserialized:\n"
              << deserialized << std::endl;
  }

  ASSERT_EQ(original, deserialized);
}

void test_autoserializable_empty_object() {
  MyTestObject empty_obj;
  ASSERT_TRUE(logkv::serializer<MyTestObject>::is_empty(empty_obj));
}

void test_deeply_nested_object() {
  DeeplyNestedContainer sample_data;
  {
    Level2Container map1;
    {
      Level1Container vec1;
      vec1.emplace_back(101, boost::asio::ip::make_address("1.1.1.1"),
                        logkv::Bytes("bytes1"), OpaqueComposite(1, "agg1"),
                        "leaf_str1");
      vec1.emplace_back(102, boost::asio::ip::make_address("2001:db8::1"),
                        logkv::Bytes("bytes2"), OpaqueComposite(2, "agg2"),
                        "leaf_str2");
      map1["vec_A"] = vec1;
      Level1Container vec2;
      vec2.emplace_back(201, boost::asio::ip::make_address("3.3.3.3"),
                        logkv::Bytes("bytes3"), OpaqueComposite(3, "agg3"),
                        "leaf_str3");
      map1["vec_B"] = vec2;
    }
    sample_data.push_back(map1);

    Level2Container map2;
    {
      Level1Container vec_empty;
      map2["vec_C_empty"] = vec_empty;
    }
    sample_data.push_back(map2);

    sample_data.emplace_back();
  }

  MonsterObject original(9001, "The Kraken", sample_data);

  size_t required_size = logkv::serializer<MonsterObject>::get_size(original);
  ASSERT_TRUE(required_size > 0);

  std::vector<char> buffer(required_size);
  size_t bytes_written = logkv::serializer<MonsterObject>::write(
    buffer.data(), buffer.size(), original);
  ASSERT_EQ(bytes_written, required_size);

  MonsterObject deserialized;
  size_t bytes_read = logkv::serializer<MonsterObject>::read(
    buffer.data(), buffer.size(), deserialized);

  ASSERT_EQ(bytes_read, required_size);

  if (original != deserialized) {
    std::cerr << "Equality failed in test_deeply_nested_object:\n"
              << "Original:\n"
              << original << "\n"
              << "Deserialized:\n"
              << deserialized << std::endl;
  }

  ASSERT_EQ(original, deserialized);
}

void test_value_vs_reference_semantics() {
  using TestTraits = logkv::composite_traits<MyTestObject>;
  {
    MyTestObject obj(10, 20, "original", {30, "thirty"});
    auto value_tuple = TestTraits::get_members_by_value(obj);
    std::get<0>(value_tuple) = 999;
    ASSERT_EQ(obj.a, 10);
  }
  {
    MyTestObject obj(10, 20, "original", {30, "thirty"});
    auto ref_tuple = TestTraits::get_members_by_reference(obj);
    std::get<0>(ref_tuple) = 999;
    ASSERT_EQ(obj.a, 999);
  }
}

void test_pfr_automatic_serialization() {
  test_type_serialization(SimplePfrAggregate{-123, "Hello PFR!"});

  test_type_serialization(NestedPfrAggregate{
    9001, {-456, "I am nested"}, logkv::Bytes("raw data"), {10, 20, 30}});

  auto obj_with_method = PfrWithMethod{5, 7};
  ASSERT_EQ(obj_with_method.sum(), 12);
  test_type_serialization(obj_with_method);

  test_type_serialization(EmptyPfrAggregate{});
}

// -----------------------------------------------------------------------------
// Test runner
// -----------------------------------------------------------------------------

int main(int argc, char* argv[]) {
  RUN_TEST(test_arithmetic_integers);
  RUN_TEST(test_arithmetic_big_endian_check);

  RUN_TEST(test_varuint_basic_values);
  RUN_TEST(test_varuint_max_value);
  RUN_TEST(test_varuint_read_overflow);
  RUN_TEST(test_varuint_read_value_overflow);

  RUN_TEST(test_container_std_string);
  RUN_TEST(test_container_read_size_limits);
  RUN_TEST(test_container_std_array);
  RUN_TEST(test_container_std_array_complex);
  RUN_TEST(test_container_std_vector_complex);
  RUN_TEST(test_container_associative);
  RUN_TEST(test_container_sequential_other);
  RUN_TEST(test_container_std_span);

  RUN_TEST(test_asio_ip_address);
  RUN_TEST(test_asio_tcp_endpoint);

  RUN_TEST(test_writer_reader_sequential_ops);
  RUN_TEST(test_writer_reader_insufficient_buffer);

  RUN_TEST(test_composite_opaque_type);
  RUN_TEST(test_autoserializable_object);
  RUN_TEST(test_autoserializable_empty_object);

  RUN_TEST(test_deeply_nested_object);

  RUN_TEST(test_value_vs_reference_semantics);

  RUN_TEST(test_pfr_automatic_serialization);

  std::cout << "========================================\n"
            << "Test Summary:\n"
            << "  PASSED: " << tests_passed << "\n"
            << "  FAILED: " << tests_failed << "\n"
            << "========================================" << std::endl;

  return (tests_failed == 0) ? 0 : 1;
}
