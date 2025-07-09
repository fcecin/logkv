#ifndef _LOGKV_BYTES_H_
#define _LOGKV_BYTES_H_

#include <array>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include "object.h"

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
static const char hexEncodeLookupUpper[] = "0123456789ABCDEF";
static const char hexEncodeLookupLower[] = "0123456789abcdef";
static constexpr std::array<int, 256> hexLookup = createHexLookup();
} // namespace logkv_detail

namespace logkv {

void encodeHex(char* dest, size_t dest_len, const char* src, size_t src_len,
               bool upper = false) {
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

void decodeHex(char* dest, size_t dest_len, const char* src, size_t src_len) {
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

/**
 * A dynamic byte array utility class that can be used as K, V in
 * logkv::Store.
 */
class Bytes : public Object {
private:
  char* data_;
  size_t size_;

public:
  virtual bool empty() const { return !size_; }

  virtual size_t serialize(char* dest, size_t size) const {
    size_t reqsz = size_ + sizeof(size_t);
    if (size >= reqsz) {
      memcpy(dest, &size_, sizeof(size_t));
      if (size_ > 0) {
        memcpy(dest + sizeof(size_t), data_, size_);
      }
    }
    return reqsz;
  }

  virtual size_t deserialize(const char* src, size_t size) {
    if (size < sizeof(size_t)) {
      return sizeof(size_t);
    }
    size_t tmpsz;
    memcpy(&tmpsz, src, sizeof(size_t));
    size_t reqsz = tmpsz + sizeof(size_t);
    if (tmpsz) {
      if (size >= reqsz) {
        if (size_ != tmpsz) {
          size_ = tmpsz;
          delete[] data_;
          data_ = new char[size_];
        }
        memcpy(data_, src + sizeof(size_t), size_);
      }
    } else {
      clear();
    }
    return reqsz;
  }

  static Bytes decodeHex(const char* hexData, size_t hexSize) {
    Bytes result(hexSize / 2);
    logkv::decodeHex(result.data(), result.size(), hexData, hexSize);
    return result;
  }

  static Bytes decodeHex(const std::string& hexStr) {
    return decodeHex(hexStr.data(), hexStr.size());
  }

  static Bytes decodeHex(const Bytes& hexBytes) {
    return decodeHex(hexBytes.data(), hexBytes.size());
  }

  static Bytes encodeHex(const char* data, size_t size, bool upper = false) {
    Bytes result(size * 2);
    logkv::encodeHex(result.data(), result.size(), data, size, upper);
    return result;
  }

  static Bytes encodeHex(const std::string& str, bool upper = false) {
    return encodeHex(str.data(), str.size(), upper);
  }

  static Bytes encodeHex(const Bytes& bytes, bool upper = false) {
    return encodeHex(bytes.data(), bytes.size(), upper);
  }

  Bytes() : data_(nullptr), size_(0) {}

  Bytes(const size_t& size) {
    if (size) {
      data_ = new char[size];
    } else {
      data_ = nullptr;
    }
    size_ = size;
  }

  Bytes(const Bytes& other) : size_(other.size_) {
    if (size_ > 0) {
      data_ = new char[size_];
      std::memcpy(data_, other.data_, size_);
    } else {
      data_ = nullptr;
    }
  }

  Bytes(const std::vector<uint8_t>& vec) : size_(vec.size()) {
    if (size_ > 0) {
      data_ = new char[size_];
      std::memcpy(data_, reinterpret_cast<const char*>(vec.data()), size_);
    } else {
      data_ = nullptr;
    }
  }

  Bytes(const std::string& str) : size_(str.size()) {
    if (size_ > 0) {
      data_ = new char[size_];
      std::memcpy(data_, str.data(), size_);
    } else {
      data_ = nullptr;
    }
  }

  Bytes(char* data, size_t size) {
    if (data == nullptr || size == 0) {
      data_ = nullptr;
      size_ = 0;
    } else {
      data_ = new char[size];
      size_ = size;
      std::memcpy(data_, data, size);
    }
  }

  Bytes(Bytes&& other) noexcept : data_(other.data_), size_(other.size_) {
    other.data_ = nullptr;
    other.size_ = 0;
  }

  virtual ~Bytes() { delete[] data_; }

  Bytes& operator=(const Bytes& other) {
    if (this != &other) {
      if (other.size_ > 0) {
        if (size_ != other.size_) {
          size_ = other.size_;
          delete[] data_;
          data_ = new char[size_];
        }
        std::memcpy(data_, other.data_, size_);
      } else {
        delete[] data_;
        data_ = nullptr;
        size_ = 0;
      }
    }
    return *this;
  }

  Bytes& operator=(const std::vector<uint8_t>& vec) {
    size_t vecSize = vec.size();
    if (vecSize > 0) {
      if (size_ != vecSize) {
        size_ = vecSize;
        delete[] data_;
        data_ = new char[size_];
      }
      std::memcpy(data_, reinterpret_cast<const char*>(vec.data()), size_);
    } else {
      delete[] data_;
      data_ = nullptr;
      size_ = 0;
    }
    return *this;
  }

  Bytes& operator=(const std::string& str) {
    size_t strSize = str.size();
    if (strSize > 0) {
      if (size_ != strSize) {
        size_ = strSize;
        delete[] data_;
        data_ = new char[size_];
      }
      std::memcpy(data_, str.data(), size_);
    } else {
      delete[] data_;
      data_ = nullptr;
      size_ = 0;
    }
    return *this;
  }

  Bytes& operator=(Bytes&& other) noexcept {
    if (this != &other) {
      delete[] data_;
      data_ = other.data_;
      size_ = other.size_;
      other.data_ = nullptr;
      other.size_ = 0;
    }
    return *this;
  }

  auto operator<=>(const Bytes& other) const {
    if (!data_ && !other.data_) {
      return std::strong_ordering::equal;
    }
    if (!data_) {
      return std::strong_ordering::less;
    }
    if (!other.data_) {
      return std::strong_ordering::greater;
    }
    if (auto cmp = size_ <=> other.size_; cmp != 0) {
      return cmp;
    }
    if (size_ == 0) {
      return std::strong_ordering::equal;
    }
    int result = std::memcmp(data_, other.data_, size_);
    if (result < 0) {
      return std::strong_ordering::less;
    }
    if (result > 0) {
      return std::strong_ordering::greater;
    }
    return std::strong_ordering::equal;
  }

  bool operator==(const Bytes& other) const {
    if (!data_ && !other.data_) {
      return true;
    }
    if (!data_ || !other.data_) {
      return false;
    }
    if (size_ != other.size_) {
      return false;
    }
    if (size_ == 0) {
      return true;
    }
    if (data_ == other.data_) {
      return true;
    }
    return std::memcmp(data_, other.data_, size_) == 0;
  }

  char& operator[](size_t index) { return data_[index]; }

  const char& operator[](size_t index) const { return data_[index]; }

  char* data() const { return data_; }

  size_t size() const { return size_; }

  void clear() {
    delete[] data_;
    data_ = nullptr;
    size_ = 0;
  }

  void resize(const size_t& size) {
    if (size_ != size) {
      if (!size) {
        delete[] data_;
        data_ = nullptr;
        size_ = 0;
      } else {
        char* data = new char[size];
        size_t bytes = std::min(size_, size);
        if (bytes) {
          std::memcpy(data, data_, bytes);
        }
        delete[] data_;
        data_ = data;
        size_ = size;
      }
    }
  }

  void wrap(char* data, size_t size) {
    delete[] data_;
    if (size > 0) {
      data_ = data;
    } else {
      delete[] data;
      data_ = nullptr;
    }
    size_ = size;
  }

  void assign(const char* data, size_t size) {
    if (size) {
      if (size_ != size) {
        delete[] data_;
        data_ = new char[size];
        size_ = size;
      }
      std::memcpy(data_, data, size);
    } else {
      clear();
    }
  }

  std::vector<uint8_t> toVector() const {
    if (!data_) {
      return {};
    }
    return std::vector<uint8_t>(reinterpret_cast<uint8_t*>(data_),
                                reinterpret_cast<uint8_t*>(data_) + size_);
  }

  std::string toString() const {
    if (!data_) {
      return "";
    }
    return std::string(data_, size_);
  }
};

size_t hash_value(const Bytes& b) {
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
 *
 * NOTE: It is better to create a template hash class that is backed by
 * std::array instead, since hashes are of a fixed size. Container hashing
 * could also be faster, e.g. `*(reinterpret_cast<const size_t*>(h.data()))`.
 */
class Hash : public Bytes {
public:
  Hash() = default;
  Hash(const Hash& other) = default;
  Hash(Hash&& other) noexcept = default;
  Hash& operator=(const Hash& other) = default;
  Hash& operator=(Hash&& other) noexcept = default;
  Hash(const Bytes& other) : Bytes(other) {}
  Hash(Bytes&& other) noexcept : Bytes(std::move(other)) {}
};

size_t hash_value(const Hash& h) {
  size_t hv = 0;
  size_t avail = h.size();
  size_t count = avail < sizeof(size_t) ? avail : sizeof(size_t);
  if (count > 0) {
    memcpy(&hv, h.data(), count);
  }
  return hv;
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