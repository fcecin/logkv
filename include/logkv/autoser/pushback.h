#ifndef _LOGKV_AUTOSER_PUSHBACK_H_
#define _LOGKV_AUTOSER_PUSHBACK_H_

#include <logkv/autoser.h>

#include <vector>
#include <deque>
#include <list>

namespace logkv {

// ----------------------------------------------------------------------------
// std::deque<T>, std::list<T>
// std::vector<T> if T is not raw bytes (!(sizeof(T) == 1 && is_trivial_v(T)))
// T must be logkv::serializable<>
// ----------------------------------------------------------------------------

template <typename T> struct DynamicPushBackSerializer {
  static size_t get_size(const T& container) {
    size_t container_size = container.size();
    if (container_size > MAX_AUTOSER_ITEMS) {
      throw std::runtime_error("autoser element count limit exceeded");
    }
    size_t total_size = serializer<VarUint<uint64_t>>::get_size(container_size);
    for (const auto& elem : container) {
      total_size += serializer<typename T::value_type>::get_size(elem);
    }
    return total_size;
  }
  static bool is_empty(const T& container) { return container.size() == 0; }
  static size_t write(char* dest, size_t size, const T& container) {
    size_t container_size = container.size();
    if (container_size > MAX_AUTOSER_ITEMS) {
      throw std::runtime_error("autoser element count limit exceeded");
    }
    Writer writer(dest, size);
    try {
      writer.write(VarUint<uint64_t>(container_size));
      for (const auto& elem : container) {
        writer.write(elem);
      }
    } catch (const insufficient_buffer& e) {
      return writer.bytes_processed() + e.get_required_bytes();
    }
    return writer.bytes_processed();
  }
  static size_t read(const char* src, size_t size, T& container) {
    Reader reader(src, size);
    try {
      VarUint<uint64_t> len_var;
      reader.read(len_var);
      const uint64_t len = len_var.value;
      if (len > MAX_AUTOSER_ITEMS) {
        throw std::runtime_error("autoser element count limit exceeded");
      }
      container.clear();
      if constexpr (requires { container.reserve(len); }) {
        container.reserve(len);
      }
      for (uint64_t i = 0; i < len; ++i) {
        auto& new_elem = container.emplace_back();
        reader.read(new_elem);
      }
    } catch (const insufficient_buffer& e) {
      return reader.bytes_processed() + e.get_required_bytes();
    }
    return reader.bytes_processed();
  }
};

template <typename T>
struct serializer<std::vector<T>,
                  std::enable_if_t<!(sizeof(T) == 1 && std::is_trivial_v<T>)>>
    : public logkv::DynamicPushBackSerializer<std::vector<T>> {};

template <typename T, typename A>
struct serializer<std::deque<T, A>>
    : public logkv::DynamicPushBackSerializer<std::deque<T, A>> {};

template <typename T, typename A>
struct serializer<std::list<T, A>>
    : public logkv::DynamicPushBackSerializer<std::list<T, A>> {};

}

#endif