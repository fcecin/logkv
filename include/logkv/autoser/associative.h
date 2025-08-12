#ifndef _LOGKV_AUTOSER_ASSOCIATIVE_H_
#define _LOGKV_AUTOSER_ASSOCIATIVE_H_

#include <logkv/autoser.h>

#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>

namespace logkv {

// ----------------------------------------------------------------------------
// std::map<K,V>, std::set<K>, std::unordered_map<K,V>, std::unordered_set<K>
// K and V must be logkv::serializable<>
// ----------------------------------------------------------------------------

template <typename T> struct DynamicAssociativeSerializer {
  static size_t get_size(const T& container) {
    size_t container_size = container.size();
    if (container_size > MAX_AUTOSER_ITEMS) {
      throw std::runtime_error("autoser element count limit exceeded");
    }
    size_t total_size = serializer<VarUint<uint64_t>>::get_size(container_size);
    for (const auto& elem : container) {
      if constexpr (requires {
                      elem.first;
                      elem.second;
                    }) {
        total_size += serializer<typename T::key_type>::get_size(elem.first);
        total_size += serializer<typename T::mapped_type>::get_size(elem.second);
      } else {
        total_size += serializer<typename T::value_type>::get_size(elem);
      }
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
        if constexpr (requires {
                        elem.first;
                        elem.second;
                      }) {
          writer.write(elem.first);
          writer.write(elem.second);
        } else {
          writer.write(elem);
        }
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
        if constexpr (requires {
                        typename T::key_type;
                        typename T::mapped_type;
                      }) {
          typename T::key_type key;
          typename T::mapped_type value;
          reader.read(key);
          reader.read(value);
          container.insert({std::move(key), std::move(value)});
        } else {
          typename T::value_type elem;
          reader.read(elem);
          container.insert(std::move(elem));
        }
      }
    } catch (const insufficient_buffer& e) {
      return reader.bytes_processed() + e.get_required_bytes();
    }
    return reader.bytes_processed();
  }
};

template <typename K, typename C, typename A>
struct serializer<std::set<K, C, A>>
    : public logkv::DynamicAssociativeSerializer<std::set<K, C, A>> {};

template <typename K, typename V, typename C, typename A>
struct serializer<std::map<K, V, C, A>>
    : public logkv::DynamicAssociativeSerializer<std::map<K, V, C, A>> {};

template <typename K, typename H, typename P, typename A>
struct serializer<std::unordered_set<K, H, P, A>>
    : public logkv::DynamicAssociativeSerializer<
        std::unordered_set<K, H, P, A>> {};

template <typename K, typename V, typename H, typename P, typename A>
struct serializer<std::unordered_map<K, V, H, P, A>>
    : public logkv::DynamicAssociativeSerializer<
        std::unordered_map<K, V, H, P, A>> {};

}

#endif