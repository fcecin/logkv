#ifndef _LOGKV_AUTOSER_H_
#define _LOGKV_AUTOSER_H_

#include <boost/endian/conversion.hpp>

#include <logkv/serializer.h>

#include <algorithm>
#include <array>
#include <bit>
#include <limits>
#include <string>
#include <tuple>

/**
 * Automatic serialization support.
 *
 * struct MyClass : public AutoSerializableObject<MyClass> {
 *  int i_;
 *  std::string j_;
 *  AUTO_SERIALIZABLE_MEMBERS(i_, j_)
 *  char notSerialized_;
 * }
 *
 * Default supported types:
 *
 * #include <logkv/autoser.h> (this header)
 * - std integral types (intX_t/uintX_t)
 * - std::array<T,N> of raw byte data and of serializable<T> types
 * - logkv::VarUint
 * - std::tuple of various serializable<T> types
 *
 * Additional supported types:
 *
 * #include <logkv/autoser/bytes.h>
 * - std::string
 * - std::vector<T> of raw byte data (sizeof(T) == 1 && std::is_trivial_v(T))
 * - logkv::Bytes
 * - logkv::Hash
 *
 * #include <logkv/autoser/pushback.h>
 * - std::vector<T> of serializable<T> types (excluding raw byte data)
 * - std::list<T> of serializable<T> types
 * - std::deque<T> of serializable<T> types
 *
 * #include <logkv/autoser/associative.h>
 * - std::map<K,V> of serializable<K> & serializable<V> types
 * - std::set<T> of serializable<T> types
 * - std::unordered_map<K,V> of serializable<K> & serializable<V> types
 * - std::unordered_set<T> of serializable<T> types
 *
 * #include <logkv/autoser/asio.h>
 * - boost::asio::ip::address
 * - boost::asio::ip::basic_endpoint<Protocol>
 *
 * #include <logkv/pfr.h>
 * - Any type that satisfies std::is_aggregate_v (uses Boost::PFR)
 *
 * Support for other types T can be added via serializable<T> or
 * composite_traits<T> template specializations.
 *
 * Examples in `tests/autoser.cpp` and `tests/testautoser.cpp`.
 */

namespace logkv {

// ----------------------------------------------------------------------------
// Config
// ----------------------------------------------------------------------------

// Protect against reading corrupted byte size fields
const size_t MAX_AUTOSER_BYTES = 1024 * 1024 * 1024;

// Protect against reading corrupted element count fields
const size_t MAX_AUTOSER_ITEMS = 256 * 1024 * 1024;

// ----------------------------------------------------------------------------
// Template implemented for T types that are composites of serializable types.
// The concept of a composite type is more general than std::is_aggregate_v.
// ----------------------------------------------------------------------------

template <typename T, typename Enable = void> struct composite_traits;

// ----------------------------------------------------------------------------
// Helpers to deser multiple objects to the same underlying char* buffer
// ----------------------------------------------------------------------------

class insufficient_buffer : public std::exception {
private:
  size_t required_bytes_;

public:
  insufficient_buffer(size_t required_bytes)
      : required_bytes_(required_bytes) {}
  const char* what() const noexcept override {
    return "logkv::insufficient_buffer";
  }
  size_t get_required_bytes() const { return required_bytes_; }
};

class Writer {
public:
  Writer(char* dest, size_t size)
      : ptr_(dest), initial_ptr_(dest), remaining_(size) {}
  template <typename T> void write(const T& val) {
    size_t bytes_written = serializer<T>::write(ptr_, remaining_, val);
    if (bytes_written > remaining_) {
      throw insufficient_buffer(bytes_written);
    }
    ptr_ += bytes_written;
    remaining_ -= bytes_written;
  }
  size_t bytes_processed() const { return ptr_ - initial_ptr_; }

private:
  char* ptr_;
  const char* const initial_ptr_;
  size_t remaining_;
};

class Reader {
public:
  Reader(const char* src, size_t size)
      : ptr_(src), initial_ptr_(src), remaining_(size) {}
  template <typename T> void read(T& val) {
    size_t bytes_read = serializer<T>::read(ptr_, remaining_, val);
    if (bytes_read > remaining_) {
      throw insufficient_buffer(bytes_read);
    }
    ptr_ += bytes_read;
    remaining_ -= bytes_read;
  }
  size_t bytes_processed() const { return ptr_ - initial_ptr_; }

private:
  const char* ptr_;
  const char* const initial_ptr_;
  size_t remaining_;
};

// ----------------------------------------------------------------------------
// Template serializer for all integral types (big-endian byte order)
// ----------------------------------------------------------------------------

template <typename T>
struct serializer<T, std::enable_if_t<std::is_integral_v<T>>> {
  static size_t get_size(const T& val) { return sizeof(T); }
  static bool is_empty(const T& val) { return val == T(); }
  static size_t write(char* dest, size_t size, const T& val) {
    constexpr size_t required_size = sizeof(T);
    if (size < required_size) {
      return required_size;
    }
    T be = boost::endian::native_to_big(val);
    std::memcpy(dest, &be, required_size);
    return required_size;
  }
  static size_t read(const char* src, size_t size, T& val) {
    constexpr size_t required_size = sizeof(T);
    if (size < required_size) {
      return required_size;
    }
    T be;
    std::memcpy(&be, src, required_size);
    val = boost::endian::big_to_native(be);
    return required_size;
  }
};

// ----------------------------------------------------------------------------
// logkv::VarUint
// ----------------------------------------------------------------------------

template <typename T> struct VarUint {
  static_assert(std::is_unsigned_v<T>, "VarUint must wrap an unsigned type");
  T value;
  VarUint(const T& val = 0)
      : value(val) {} // Forces `std::is_aggregate_v<T> == false`
  operator T() const { return value; }
  bool operator==(const VarUint<T>& other) const {
    return value == other.value;
  }
  bool operator!=(const VarUint<T>& other) const {
    return value != other.value;
  }
  bool operator==(const T& other) const { return value == other; }
  bool operator!=(const T& other) const { return value != other; }
};

template <typename T> struct serializer<VarUint<T>> {
  static size_t get_size(const VarUint<T>& val) {
    if (val.value == 0) {
      return 1;
    }
    return (std::bit_width(val.value) + 6) / 7;
  }
  static bool is_empty(const VarUint<T>& val) { return val.value == T(); }
  static size_t write(char* dest, size_t size, const VarUint<T>& val) {
    const size_t required_size = get_size(val);
    if (size < required_size) {
      return required_size;
    }
    T v = val.value;
    char* ptr = dest;
    while (v >= 0x80) {
      *ptr++ = static_cast<char>((v & 0x7F) | 0x80);
      v >>= 7;
    }
    *ptr++ = static_cast<char>(v & 0x7F);
    return required_size;
  }
  static size_t read(const char* src, size_t size, VarUint<T>& val) {
    T result = 0;
    unsigned int shift = 0;
    constexpr size_t max_bytes_for_type = (sizeof(T) * 8 + 6) / 7;
    for (size_t i = 0; i < size; ++i) {
      if (i >= max_bytes_for_type) {
        throw std::runtime_error("VarUint overflow: too many input bytes.");
      }
      const unsigned char byte = src[i];
      const T part = static_cast<T>(byte & 0x7F);
      if (part > (std::numeric_limits<T>::max() >> shift)) {
        throw std::runtime_error("VarUint overflow: decoded value too large.");
      }
      result |= part << shift;
      if ((byte & 0x80) == 0) {
        val.value = result;
        return i + 1;
      }
      shift += 7;
    }
    return size + 1;
  }
};

template <typename T, size_t N>
struct serializer<std::array<T, N>,
                  std::enable_if_t<sizeof(T) == 1 && std::is_trivial_v<T>>> {
  static size_t get_size(const std::array<T, N>& arr) { return N; }
  static bool is_empty(const std::array<T, N>& arr) {
    return std::all_of(arr.begin(), arr.end(),
                       [](const T& val) { return val == T{}; });
  }
  static size_t write(char* dest, size_t size, const std::array<T, N>& arr) {
    if (size < N) {
      return N;
    }
    std::memcpy(dest, arr.data(), N);
    return N;
  }
  static size_t read(const char* src, size_t size, std::array<T, N>& arr) {
    if (size < N) {
      return N;
    }
    std::memcpy(arr.data(), src, N);
    return N;
  }
};

// ----------------------------------------------------------------------------
// std::array<T, N> for any type T with a logkv::serializer<T>
// ----------------------------------------------------------------------------

template <typename T, size_t N>
struct serializer<std::array<T, N>,
                  std::enable_if_t<!(sizeof(T) == 1 && std::is_trivial_v<T>)>> {
  static size_t get_size(const std::array<T, N>& arr) {
    size_t total_size = 0;
    for (const auto& elem : arr) {
      total_size += serializer<T>::get_size(elem);
    }
    return total_size;
  }
  static bool is_empty(const std::array<T, N>& arr) {
    return std::all_of(arr.begin(), arr.end(), [](const T& val) {
      return serializer<T>::is_empty(val);
    });
  }
  static size_t write(char* dest, size_t size, const std::array<T, N>& arr) {
    Writer writer(dest, size);
    try {
      for (const auto& elem : arr) {
        writer.write(elem);
      }
    } catch (const insufficient_buffer& e) {
      return writer.bytes_processed() + e.get_required_bytes();
    }
    return writer.bytes_processed();
  }
  static size_t read(const char* src, size_t size, std::array<T, N>& arr) {
    Reader reader(src, size);
    try {
      for (auto& elem : arr) {
        reader.read(elem);
      }
    } catch (const insufficient_buffer& e) {
      return reader.bytes_processed() + e.get_required_bytes();
    }
    return reader.bytes_processed();
  }
};

// ----------------------------------------------------------------------------
// Serializer implementation templates
// ----------------------------------------------------------------------------

template <typename... Args>
inline size_t get_size_for_members(const std::tuple<Args...>& t) {
  size_t total_size = 0;
  std::apply(
    [&](const auto&... member) {
      total_size =
        (logkv::serializer<std::decay_t<decltype(member)>>::get_size(member) +
         ... + 0);
    },
    t);
  return total_size;
}

template <typename... Args>
inline void write_members(Writer& writer, const std::tuple<Args...>& t) {
  std::apply([&](const auto&... member) { (writer.write(member), ...); }, t);
}

template <typename... Args>
inline void read_members(Reader& reader, std::tuple<Args...>& t) {
  std::apply([&](auto&... member) { (reader.read(member), ...); }, t);
}

template <typename... Args>
inline bool are_members_empty(const std::tuple<Args...>& t) {
  return std::apply(
    [](const auto&... member) {
      return (
        logkv::serializer<std::decay_t<decltype(member)>>::is_empty(member) &&
        ...);
    },
    t);
}

// ----------------------------------------------------------------------------
// Serializer for std::tuple
// ----------------------------------------------------------------------------

template <typename... Args> struct serializer<std::tuple<Args...>> {
  static size_t get_size(const std::tuple<Args...>& t) {
    return get_size_for_members(t);
  }
  static bool is_empty(const std::tuple<Args...>& t) {
    return are_members_empty(t);
  }
  static size_t write(char* dest, size_t size, const std::tuple<Args...>& t) {
    Writer writer(dest, size);
    try {
      write_members(writer, t);
    } catch (const insufficient_buffer& e) {
      return writer.bytes_processed() + e.get_required_bytes();
    }
    return writer.bytes_processed();
  }
  static size_t read(const char* src, size_t size, std::tuple<Args...>& t) {
    Reader reader(src, size);
    try {
      read_members(reader, t);
    } catch (const insufficient_buffer& e) {
      return reader.bytes_processed() + e.get_required_bytes();
    }
    return reader.bytes_processed();
  }
};

// ----------------------------------------------------------------------------
// Serializer for any T that uses composite_traits<T>
// ----------------------------------------------------------------------------

template <typename T>
struct serializer<T, std::void_t<typename composite_traits<T>::member_types>> {
  static size_t get_size(const T& obj) {
    if constexpr (requires {
                    composite_traits<T>::get_members_by_const_reference(obj);
                  }) {
      return get_size_for_members(
        composite_traits<T>::get_members_by_const_reference(obj));
    } else {
      return get_size_for_members(
        composite_traits<T>::get_members_by_value(obj));
    }
  }
  static bool is_empty(const T& obj) {
    if constexpr (requires {
                    composite_traits<T>::get_members_by_const_reference(obj);
                  }) {
      return are_members_empty(
        composite_traits<T>::get_members_by_const_reference(obj));
    } else {
      return are_members_empty(composite_traits<T>::get_members_by_value(obj));
    }
  }
  static size_t write(char* dest, size_t size, const T& obj) {
    Writer writer(dest, size);
    try {
      if constexpr (requires {
                      composite_traits<T>::get_members_by_const_reference(obj);
                    }) {
        write_members(writer,
                      composite_traits<T>::get_members_by_const_reference(obj));
      } else {
        write_members(writer, composite_traits<T>::get_members_by_value(obj));
      }
    } catch (const insufficient_buffer& e) {
      return writer.bytes_processed() + e.get_required_bytes();
    }
    return writer.bytes_processed();
  }
  static size_t read(const char* src, size_t size, T& obj) {
    Reader reader(src, size);
    try {
      if constexpr (requires {
                      composite_traits<T>::get_members_by_reference(obj);
                    }) {
        auto member_refs = composite_traits<T>::get_members_by_reference(obj);
        read_members(reader, member_refs);
      } else {
        typename composite_traits<T>::member_types members;
        auto check_constructible = []<typename... Args>(
                                     const std::tuple<Args...>&) {
          static_assert(
            std::is_constructible_v<T, Args...>,
            "\n\n>>> Serialization Error: The class is not constructible from "
            "its members. <<<\n"
            "    Must provide a constructor that matches the types in "
            "composite_traits::member_types.\n");
        };
        check_constructible(members);
        read_members(reader, members);
        obj = std::make_from_tuple<T>(members);
      }
    } catch (const insufficient_buffer& e) {
      return reader.bytes_processed() + e.get_required_bytes();
    }
    return reader.bytes_processed();
  }
};

// ----------------------------------------------------------------------------
// AutoSerializableObject
// ----------------------------------------------------------------------------

template <typename Derived> class AutoSerializableObject {
public:
  auto operator<=>(const AutoSerializableObject&) const = default;
};

template <typename T>
struct composite_traits<
  T, std::enable_if_t<std::is_base_of_v<AutoSerializableObject<T>, T>>> {
  using member_types = decltype(std::declval<T>()._as_const_member_tuple());
  static auto get_members_by_value(const T& obj) {
    return obj._as_const_member_tuple();
  }
  static auto get_members_by_const_reference(const T& obj) {
    return obj._as_const_member_tie();
  }
  static auto get_members_by_reference(T& obj) { return obj._as_member_tie(); }
};

} // namespace logkv

#define AUTO_SERIALIZABLE_MEMBERS(...)                                         \
  auto _as_const_member_tuple() const { return std::make_tuple(__VA_ARGS__); } \
  auto _as_const_member_tie() const { return std::tie(__VA_ARGS__); }          \
  auto _as_member_tie() { return std::tie(__VA_ARGS__); }

#endif // _LOGKV_AUTOSER_H_