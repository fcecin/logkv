#ifndef _LOGKV_BYTES_H_
#define _LOGKV_BYTES_H_

#include <cstring>
#include <iostream>
#include <span>
#include <string>
#include <vector>

#include <logkv/hex.h>

namespace logkv {

/**
 * A dynamic byte array utility class that can be used as K, V in
 * logkv::Store.
 */
using Bytes = std::vector<char>;

inline size_t hash_value(const Bytes& b) {
  size_t hv = 0xcbf29ce484222325ULL;
  const char* data = b.data();
  size_t size = b.size();
  for (size_t i = 0; i < size; ++i) {
    hv ^= static_cast<unsigned char>(data[i]);
    hv *= 0x100000001b3ULL;
  }
  return hv;
}

/**
 * A version of logkv::Bytes that swaps the FNV container hashing operation with
 * simply copying a part of its own contents (which is already a hash of some
 * sort) to fit a container hash value.
 */
using Hash = std::vector<std::byte>;

inline size_t hash_value(const Hash& h) {
  size_t hv = 0;
  size_t avail = h.size();
  size_t count = avail < sizeof(size_t) ? avail : sizeof(size_t);
  if (count > 0) {
    memcpy(&hv, h.data(), count);
  }
  return hv;
}

inline Bytes hashToBytes(const Hash& h) {
  const char* data = reinterpret_cast<const char*>(h.data());
  return Bytes(data, data + h.size());
}

inline Hash bytesToHash(const Bytes& b) {
  const std::byte* data = reinterpret_cast<const std::byte*>(b.data());
  return Hash(data, data + b.size());
}

inline Bytes makeBytes(const char* str) {
  if (!str) {
    return Bytes();
  }
  return Bytes(str, str + std::strlen(str));
}

inline Bytes makeBytes(const std::string& str) {
  return Bytes(str.begin(), str.end());
}

inline Bytes bytesDecodeHex(const char* hexData, size_t hexSize) {
  Bytes result(hexSize / 2);
  logkv::decodeHex(result.data(), result.size(), hexData, hexSize);
  return result;
}

inline Bytes bytesDecodeHex(const std::string& hexStr) {
  return bytesDecodeHex(hexStr.data(), hexStr.size());
}

inline Bytes bytesDecodeHex(const Bytes& hexBytes) {
  return bytesDecodeHex(hexBytes.data(), hexBytes.size());
}

inline Bytes bytesDecodeHex(const Hash& hexBytes) {
  return bytesDecodeHex(reinterpret_cast<const char*>(hexBytes.data()),
                        hexBytes.size());
}

inline Bytes bytesEncodeHex(const char* data, size_t size, bool upper = false) {
  Bytes result(size * 2);
  logkv::encodeHex(result.data(), result.size(), data, size, upper);
  return result;
}

inline Bytes bytesEncodeHex(const std::string& str, bool upper = false) {
  return bytesEncodeHex(str.data(), str.size(), upper);
}

inline Bytes bytesEncodeHex(const Bytes& bytes, bool upper = false) {
  return bytesEncodeHex(bytes.data(), bytes.size(), upper);
}

inline Bytes bytesEncodeHex(const Hash& bytes, bool upper = false) {
  return bytesEncodeHex(reinterpret_cast<const char*>(bytes.data()),
                        bytes.size(), upper);
}

inline std::ostream& operator<<(std::ostream& os, const Bytes& b) {
  os.write(b.data(), b.size());
  return os;
}

inline std::ostream& operator<<(std::ostream& os, const Hash& h) {
  os.write(reinterpret_cast<const char*>(h.data()), h.size());
  return os;
}

} // namespace logkv

namespace std {

template <> struct hash<logkv::Bytes> {
  size_t operator()(const logkv::Bytes& b) const noexcept {
    return logkv::hash_value(b);
  }
};

template <> struct hash<logkv::Hash> {
  size_t operator()(const logkv::Hash& h) const noexcept {
    return logkv::hash_value(h);
  }
};

} // namespace std

#endif