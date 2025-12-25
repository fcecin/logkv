#ifndef _LOGKV_AUTOSER_BYTES_H_
#define _LOGKV_AUTOSER_BYTES_H_

#include <logkv/autoser.h>

#include <string>
#include <vector>

namespace logkv {

// ----------------------------------------------------------------------------
// std::string, std::vector<T> for sizeof(T) == 1 && std::is_trivial_v(T)
// ----------------------------------------------------------------------------

// Template for all size(), data(), resize() contiguous-memory byte heaps
template <typename T> struct DynamicBytesSerializer {
  static size_t get_size(const T& container) {
    const size_t container_size = container.size();
    if (container_size > MAX_AUTOSER_BYTES) {
      throw std::runtime_error("autoser byte size limit exceeded");
    }
    return serializer<VarUint<uint64_t>>::get_size(container_size) +
           container_size;
  }
  static bool is_empty(const T& container) { return container.size() == 0; }
  static size_t write(char* dest, size_t size, const T& container) {
    const size_t container_size = container.size();
    if (container_size > MAX_AUTOSER_BYTES) {
      throw std::runtime_error("autoser byte size limit exceeded");
    }
    const VarUint<uint64_t> len_var(container_size);
    const size_t len_size = serializer<VarUint<uint64_t>>::get_size(len_var);
    const size_t required = len_size + container_size;
    if (size < required) {
      return required;
    }
    size_t written = serializer<VarUint<uint64_t>>::write(dest, size, len_var);
    if (container_size > 0) {
      std::memcpy(dest + written, container.data(), container_size);
    }
    return required;
  }
  static size_t read(const char* src, size_t size, T& container) {
    VarUint<uint64_t> len_var;
    size_t len_size = serializer<VarUint<uint64_t>>::read(src, size, len_var);
    if (len_size > size) {
      return len_size;
    }
    const uint64_t len = len_var.value;
    if (len > MAX_AUTOSER_BYTES) {
      throw std::runtime_error("autoser byte size limit exceeded");
    }
    const size_t required = len_size + len;
    if (size < required) {
      return required;
    }
    container.resize(len);
    if (len > 0) {
      std::memcpy(&container[0] /*== container.data()*/, src + len_size, len);
    }
    return required;
  }
};

template <>
struct serializer<std::string>
    : public logkv::DynamicBytesSerializer<std::string> {};

template <typename T>
struct serializer<std::vector<T>,
                  std::enable_if_t<sizeof(T) == 1 && std::is_trivial_v<T>>>
    : public logkv::DynamicBytesSerializer<std::vector<T>> {};

} // namespace logkv

#endif