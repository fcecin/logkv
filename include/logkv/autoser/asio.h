#ifndef _LOGKV_AUTOSER_ASIO_H_
#define _LOGKV_AUTOSER_ASIO_H_

#include <logkv/autoser.h>

#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/basic_endpoint.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ip/udp.hpp>

namespace logkv {

// ----------------------------------------------------------------------------
// boost::asio::ip::address
// boost::asio::ip::basic_endpoint<Protocol>
// ----------------------------------------------------------------------------

template <> struct serializer<boost::asio::ip::address> {
  enum {
    TYPE_SIZE = 1,
    IPV4_SIZE = 4,
    IPV6_SIZE = 16
  };
  enum {
    UNKNOWN_TYPE = 0,
    IPV4_TYPE = 1,
    IPV6_TYPE = 2
  };
  static size_t get_size(const boost::asio::ip::address& addr) {
    size_t sz = TYPE_SIZE;
    if (addr.is_v4())
      sz += IPV4_SIZE;
    else if (addr.is_v6())
      sz += IPV6_SIZE;
    return sz;
  }
  static bool is_empty(const boost::asio::ip::address& addr) {
    return addr.is_unspecified();
  }
  static size_t write(char* dest, size_t size,
                      const boost::asio::ip::address& addr) {
    size_t required = get_size(addr);
    if (size < required)
      return required;
    if (addr.is_v4()) {
      dest[0] = IPV4_TYPE;
      auto b = addr.to_v4().to_bytes();
      std::memcpy(dest + TYPE_SIZE, b.data(), b.size());
    } else if (addr.is_v6()) {
      dest[0] = IPV6_TYPE;
      auto b = addr.to_v6().to_bytes();
      std::memcpy(dest + TYPE_SIZE, b.data(), b.size());
    } else {
      dest[0] = UNKNOWN_TYPE;
    }
    return required;
  }
  static size_t read(const char* src, size_t size,
                     boost::asio::ip::address& addr) {
    if (size < TYPE_SIZE) {
      return TYPE_SIZE;
    }
    const uint8_t type = src[0];
    const char* data = src + TYPE_SIZE;
    switch (type) {
    case IPV4_TYPE: {
      const size_t required_size = TYPE_SIZE + IPV4_SIZE;
      if (size < required_size) {
        return required_size;
      }
      boost::asio::ip::address_v4::bytes_type b;
      std::memcpy(b.data(), data, IPV4_SIZE);
      addr = boost::asio::ip::address_v4(b);
      return required_size;
    }
    case IPV6_TYPE: {
      const size_t required_size = TYPE_SIZE + IPV6_SIZE;
      if (size < required_size) {
        return required_size;
      }
      boost::asio::ip::address_v6::bytes_type b;
      std::memcpy(b.data(), data, IPV6_SIZE);
      addr = boost::asio::ip::address_v6(b);
      return required_size;
    }
    default: {
      addr = boost::asio::ip::address();
      return TYPE_SIZE;
    }
    }
  }
};

template <typename Protocol>
struct composite_traits<boost::asio::ip::basic_endpoint<Protocol>> {
  using member_types = std::tuple<boost::asio::ip::address, uint16_t>;
  static auto get_members_by_value(const boost::asio::ip::basic_endpoint<Protocol>& ep) {
    return std::make_tuple(ep.address(), ep.port());
  }
};

}

// ----------------------------------------------------------------------------
// Support for boost::asio::ip types as boost unordered container keys
// (e.g. logkv::Store<boost::unordered_flat_map, boost::asio::ip::address, V>)
// ----------------------------------------------------------------------------

#include <boost/container_hash/hash.hpp>
namespace boost::asio::ip {
inline std::size_t hash_value(const address& addr) {
  std::size_t seed = 0;
  if (addr.is_v4()) {
    auto bytes = addr.to_v4().to_bytes();
    boost::hash_range(seed, bytes.begin(), bytes.end());
  } else { // if addr.is_v6()
    auto bytes = addr.to_v6().to_bytes();
    boost::hash_range(seed, bytes.begin(), bytes.end());
  }
  return seed;
}
template <typename Protocol>
inline std::size_t hash_value(const basic_endpoint<Protocol>& ep) {
  std::size_t seed = hash_value(ep.address());
  boost::hash_combine(seed, ep.port());
  return seed;
}
}

#endif