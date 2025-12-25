#ifndef _LOGKV_HEX_H_
#define _LOGKV_HEX_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <stdexcept>

namespace logkv_detail {
constexpr std::array<int, 256> createHexLookup() {
  std::array<int, 256> lookup{};
  for (int i = 0; i < 256; ++i) {
    lookup[i] = -1;
  }
  for (char c = '0'; c <= '9'; ++c)
    lookup[static_cast<unsigned char>(c)] = c - '0';
  for (char c = 'a'; c <= 'f'; ++c)
    lookup[static_cast<unsigned char>(c)] = c - 'a' + 10;
  for (char c = 'A'; c <= 'F'; ++c)
    lookup[static_cast<unsigned char>(c)] = c - 'A' + 10;
  return lookup;
}
inline const char hexEncodeLookupUpper[] = "0123456789ABCDEF";
inline const char hexEncodeLookupLower[] = "0123456789abcdef";
inline constexpr std::array<int, 256> hexLookup = createHexLookup();
} // namespace logkv_detail

namespace logkv {

inline void encodeHex(char* dest, size_t dest_len, const char* src,
                      size_t src_len, bool upper = false) {
  if (!src_len) {
    return;
  }
  if (!dest || !src) {
    throw std::invalid_argument("Null buffer and nonzero source length.");
  }
  const size_t len = src_len * 2;
  if (dest_len < len) {
    throw std::invalid_argument("Destination buffer too small.");
  }
  const char* lookup = upper ? logkv_detail::hexEncodeLookupUpper
                             : logkv_detail::hexEncodeLookupLower;
  for (size_t i = 0; i < src_len; ++i) {
    const uint8_t byte = static_cast<uint8_t>(src[i]);
    dest[i * 2] = lookup[byte >> 4];
    dest[i * 2 + 1] = lookup[byte & 0x0F];
  }
}

inline void decodeHex(char* dest, size_t dest_len, const char* src,
                      size_t src_len) {
  if (!src_len) {
    return;
  }
  if (!dest || !src) {
    throw std::invalid_argument("Null buffer and nonzero source length.");
  }
  if (src_len % 2 != 0) {
    throw std::invalid_argument(
      "Hex string must have an even number of characters.");
  }
  const size_t len = src_len / 2;
  if (dest_len < len) {
    throw std::invalid_argument("Destination buffer too small.");
  }
  for (size_t i = 0; i < len; ++i) {
    const unsigned char hchar = src[2 * i];
    const unsigned char lchar = src[2 * i + 1];
    const int hval = logkv_detail::hexLookup[hchar];
    const int lval = logkv_detail::hexLookup[lchar];
    if (hval == -1 || lval == -1) {
      throw std::invalid_argument("Hex string has invalid characters");
    }
    dest[i] = static_cast<char>((hval << 4) | lval);
  }
}

} // namespace logkv

#endif