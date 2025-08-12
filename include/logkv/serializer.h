#ifndef _LOGKV_SERIALIZATION_H_
#define _LOGKV_SERIALIZATION_H_

#include <cstddef>
#include <stdexcept>
#include <type_traits>

namespace logkv {

/**
 * Root serialization template.
 * All types that want to work as logkv::Store keys or values need to specialize
 * this template, providing four methods:
 *
 * namespace logkv {
 * template <typename T>
 * struct serializer<T> {
 *  static size_t get_size(const T& val) { ... }
 *  static bool is_empty(const T& val) { ... }
 *  static size_t write(char* dest, size_t size, const T& val) { ... }
 *  static size_t read(const char* src, size_t size, T& val) { ... }
 * };
 * }
 *
 * `get_size()` must return the total number of bytes required to serialize
 * `val`.
 *
 * `is_empty()` must return `true` if `val` is the empty value of T, `false`
 * otherwise.
 *
 * `write()` must return the same value as `get_size(val)`, and if `size` (size
 * of `dest`) is sufficient to serialize `val`, must serialize (write) `val` to
 * `dest`. May thrown a `std::runtime_exception` if `val` is too large.
 *
 * `read()` must return the minimum number of bytes required to continue reading
 * the object (if `size`, which is the  size of `src`, is insufficient) and
 * `val` is not updated, or the number of bytes consumed from `src` to read
 * (deserialize) the object into `val` (if `size` is sufficient). May throw a
 * `std::runtime_exception` if `val` is too large.
 */
template <typename T, typename Enable = void> struct serializer;

} // namespace logkv

#endif