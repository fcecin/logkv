#ifndef LOGKV_CRC_H
#define LOGKV_CRC_H

#include <cstddef>
#include <cstdint>

#include <logkv/crc/crc16.h>
#include <crc32c/crc32c.h>

namespace logkv {

inline uint16_t computeCRC16(const void* data, size_t len) {
  return crc16::xmodem(data, len);
}

inline uint32_t computeCRC32(const void* data, size_t len) {
    return crc32c::Crc32c(reinterpret_cast<const char*>(data), len);
}

} // namespace logkv

#endif