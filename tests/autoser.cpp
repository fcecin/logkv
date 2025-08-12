/**
 * Hello-world demo of the optional auto-serialization helpers.
 */

#include <logkv/autoser/asio.h>
using SockAddr = boost::asio::ip::udp::endpoint;

#include <logkv/autoser/bytes.h>

#include <iostream>

using ArrayHash = std::array<uint8_t, 32>;
using namespace logkv;

// =============================================================================
// extending the serialization framework for a new type: boost::uuids::uuid.
// this is extending the autoser library of serializable types by telling
// it directly how to read and write the bytes in and out of a buffer.
// =============================================================================

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

// Add support for logkv autoser for this type
namespace logkv {
template <> struct serializer<boost::uuids::uuid> {
  // still works if this function is ommittted
  static bool is_empty(const boost::uuids::uuid& u) {
    // return false; // force failure to test custom is_default() dispatch
    return u.is_nil();
  }
  // still works if this function is ommittted
  static size_t
  get_size(const boost::uuids::uuid& u) { // works, but unnecessary
    assert(u.static_size() == sizeof(u));
    return u.static_size();
  }
  static size_t write(char* dest, size_t size, const boost::uuids::uuid& u) {
    if (size < u.static_size())
      return u.static_size();
    std::memcpy(dest, u.data, u.static_size());
    return u.static_size();
  }
  static size_t read(const char* src, size_t size, boost::uuids::uuid& u) {
    if (size < u.static_size())
      return u.static_size();
    std::memcpy(u.data, src, u.static_size());
    return u.static_size();
  }
};
} // namespace logkv

// =============================================================================
// extending the serialization framework for a custom composite type.
// composite types are types that can be described entirely as a list of
// other types that already have logkv serializer<> templates.
// =============================================================================

// Pretend that this is some class that maybe we can't alter (third-party, e.g.
// some boost class)
class OpaqueCompositeDemo {
public:
  OpaqueCompositeDemo() : i_(0) {};
  OpaqueCompositeDemo(const uint64_t i, const std::string& s) : i_(i), s_(s) {};
  uint64_t i_;
  std::string s_;
  auto operator<=>(const OpaqueCompositeDemo& other) const = default;
};

// Add support to logkv autoser for the preexisting opaque type
namespace logkv {
template <> struct composite_traits<OpaqueCompositeDemo> {
  using member_types = std::tuple<uint64_t, std::string>;
  static auto get_members_by_value(const OpaqueCompositeDemo& o) {
    return std::make_tuple(o.i_, o.s_);
  }
};
} // namespace logkv

// =============================================================================
// serialization test class (extends SerializableObject)
// =============================================================================

class TestObject : public logkv::AutoSerializableObject<TestObject> {
public:
  uint64_t uint_field_;
  ArrayHash hash_field_;
  std::string string_field_;
  SockAddr endpoint_field_;
  boost::uuids::uuid uuid_field_; // not provided by <logkv/autoser.h>
  Bytes bytes_field_;
  OpaqueCompositeDemo oad_field_; // not provided by <logkv/autoser.h>

  TestObject() : uint_field_(0) {
    hash_field_.fill(0);
    uuid_field_ = boost::uuids::nil_uuid();
  }
  TestObject(uint64_t u, const ArrayHash& h, const std::string& s,
             const SockAddr& ep, const boost::uuids::uuid& id, const Bytes& b,
             const OpaqueCompositeDemo& o)
      : uint_field_(u), hash_field_(h), string_field_(s), endpoint_field_(ep),
        uuid_field_(id), bytes_field_(b), oad_field_(o) {}

  AUTO_SERIALIZABLE_MEMBERS(uint_field_, hash_field_, string_field_,
                            endpoint_field_, uuid_field_, bytes_field_,
                            oad_field_)

  auto operator<=>(const TestObject& other) const = default;
};

// =============================================================================
// generic serialization test function.
// shows logkv::Store would be satisfied with the tested types.
// =============================================================================

template <typename T>
void test_serialization(const T& original_obj, const std::string& test_name) {
  std::cout << "--- Running test: " << test_name << " ---" << std::endl;
  size_t required_size = logkv::serializer<T>::get_size(original_obj);
  std::cout << "Calculated size: " << required_size << " bytes" << std::endl;
  if (!logkv::serializer<T>::is_empty(original_obj)) {
    assert(required_size > 0);
  }
  std::vector<char> buffer(required_size);
  if (required_size > 0) {
    size_t bytes_written = logkv::serializer<T>::write(buffer.data(), buffer.size(), original_obj);
    std::cout << "Bytes written: " << bytes_written << std::endl;
    assert(bytes_written == required_size);
  }
  T deserialized_obj;
  if (required_size > 0) {
    size_t bytes_read = logkv::serializer<T>::read(buffer.data(), buffer.size(), deserialized_obj);
    std::cout << "Bytes read: " << bytes_read << std::endl;
    assert(bytes_read == required_size);
  }
  assert(original_obj == deserialized_obj);
  std::cout << "SUCCESS: Original and deserialized objects match." << std::endl;
  std::cout << std::endl;
}

// =============================================================================
// main
// =============================================================================

int main(int argc, char* argv[]) {
  {
    // 1. Test a populated TestObject
    ArrayHash test_hash;
    std::fill(test_hash.begin(), test_hash.end(), 0xFE);
    boost::uuids::random_generator gen;
    boost::uuids::uuid test_uuid = gen();
    Bytes test_bytes("testing bytes");

    TestObject populated_obj(
      uint64_t(999888777), test_hash, "Hello, serialization!",
      SockAddr(boost::asio::ip::make_address("8.8.4.4"), 53), test_uuid,
      test_bytes, {5678, "nested str"});
    test_serialization(populated_obj, "Populated TestObject");

    // 2. Test a default (empty) TestObject
    TestObject empty_obj;
    assert(logkv::serializer<TestObject>::is_empty(empty_obj));
    test_serialization(empty_obj, "Empty TestObject");
  }

  std::cout << "All autoser tests passed!\n" << std::endl;
  return 0;
}