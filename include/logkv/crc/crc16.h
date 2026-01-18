/**
 * Header-only CRC16 implementation.
 * Ripped off of Frank Bösing's FastCRC library:
 *  https://github.com/FrankBoesing/FastCRC
 */

/* FastCRC library code is placed under the MIT license
 * Copyright (c) 2014 - 2021 Frank Bösing
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

//
// Thanks to:
// - Catalogue of parametrised CRC algorithms, CRC RevEng
// http://reveng.sourceforge.net/crc-catalogue/
//
// - Danjel McGougan (CRC-Table-Generator)
//

#ifndef LOGKV_CRC16_H
#define LOGKV_CRC16_H

#include <cstddef>
#include <cstdint>

namespace logkv::crc16 {

namespace detail {

static inline uint32_t REV16(uint32_t value) {
  return (value >> 8) | ((value & 0xff) << 8);
}

inline const uint16_t* getTable() {
  static const uint16_t table[1][1024] = {{
#include "crc16table.inc"
  }};
  return table[0];
}

} // namespace detail

// ================= 16-BIT CRC ===================

#define crc_n4(crc, data, table)                                               \
  crc ^= data;                                                                 \
  crc = table[(crc & 0xff) + 0x300] ^ table[((crc >> 8) & 0xff) + 0x200] ^     \
        table[((data >> 16) & 0xff) + 0x100] ^ table[data >> 24];

/** XMODEM
 * Alias ZMODEM, CRC-16/ACORN
 * @param data Pointer to Data
 * @param datalen Length of Data
 * @return CRC value
 */
inline uint16_t xmodem_upd(const uint8_t* data, size_t len, uint16_t seed) {
  uint16_t crc = seed;
  const uint16_t* crc_table_xmodem = detail::getTable();

  while (((uintptr_t)data & 3) && len) {
    crc = (crc >> 8) ^ crc_table_xmodem[(crc & 0xff) ^ *data++];
    len--;
  }

  while (len >= 16) {
    len -= 16;
    crc_n4(crc, ((uint32_t*)data)[0], crc_table_xmodem);
    crc_n4(crc, ((uint32_t*)data)[1], crc_table_xmodem);
    crc_n4(crc, ((uint32_t*)data)[2], crc_table_xmodem);
    crc_n4(crc, ((uint32_t*)data)[3], crc_table_xmodem);
    data += 16;
  }

  while (len--) {
    crc = (crc >> 8) ^ crc_table_xmodem[(crc & 0xff) ^ *data++];
  }

  seed = crc;
  crc = detail::REV16(crc);
  return crc;
}

inline uint16_t xmodem(const void* data, size_t datalen) {
  // width=16 poly=0x1021 init=0x0000 refin=false refout=false xorout=0x0000
  // check=0x31c3
  return xmodem_upd((const uint8_t*)data, datalen, 0x0000);
}

} // namespace logkv::crc16

#undef crc_n4

#endif