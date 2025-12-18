#pragma once
#include <logkv/autoser.h>
#include <tuple>
#include <type_traits>

namespace logkv {

// ----------------------------------------------------------------------------
// Use this instead of AutoSerializableObject<T> to suppport partial updates.
// Use AUTO_PARTIAL_SERIALIZABLE_MEMBERS to specify the partial members.
// Use AUTO_SERIALIZABLE_MEMBERS to specify serializable members in full mode.
// Calling _setFullSerialization() controls per-thread serialization mode.
// ----------------------------------------------------------------------------

struct ObjectEncoding {
  enum : uint8_t {
    full = 0x00, // full object follow encoded (all members)
    part = 0x01, // partial object follows encoded (partial members)
    none = 0x02  // zero members encoded (no data; empty/erased object)
  };
};

template <typename Derived> class AutoPartialSerializableObject {
public:
  auto operator<=>(const AutoPartialSerializableObject&) const = default;
};

template <typename T>
struct serializer<
  T, std::enable_if_t<std::is_base_of_v<AutoPartialSerializableObject<T>, T>>> {

  static bool is_empty(const T& obj) {
    return logkv::are_members_empty(obj._as_const_member_tie());
  }

  static size_t get_size(const T& obj) {
    bool isSnapshot = T::_logkvStoreSnapshot();
    bool full = isSnapshot || T::_getFullSerialization();
    if (full) {
      return logkv::get_size_for_members(obj._as_const_member_tie());
    }
    return logkv::get_size_for_members(obj._as_partial_const_member_tie()) +
           (isSnapshot ? 0 : 1);
  }

  static size_t write(char* dest, size_t size, const T& obj) {
    Writer writer(dest, size);
    bool isSnapshot = T::_logkvStoreSnapshot();
    bool full = isSnapshot || T::_getFullSerialization();
    bool objectIsEmptyForSure = false;

    try {
      if (!isSnapshot) {
        objectIsEmptyForSure = is_empty(obj);
        uint8_t header;
        if (objectIsEmptyForSure) {
          header = ObjectEncoding::none;
        } else if (full) {
          header = ObjectEncoding::full;
        } else {
          header = ObjectEncoding::part;
        }
        writer.write(header);
      }

      if (!objectIsEmptyForSure) {
        if (full) {
          logkv::write_members(writer, obj._as_const_member_tie());
        } else {
          logkv::write_members(writer, obj._as_partial_const_member_tie());
        }
      }
    } catch (const insufficient_buffer& e) {
      return writer.bytes_processed() + e.get_required_bytes();
    }
    return writer.bytes_processed();
  }

  static size_t read(const char* src, size_t size, T& obj) {
    Reader reader(src, size);
    bool isSnapshot = T::_logkvStoreSnapshot();
    bool full = isSnapshot;
    bool objectIsEmptyForSure = false;

    try {
      if (!isSnapshot) {
        uint8_t header;
        reader.read(header);
        if (header == ObjectEncoding::none)
          objectIsEmptyForSure = true;
        else if (header == ObjectEncoding::full)
          full = true;
        else if (header == ObjectEncoding::part)
          full = false;
        else
          throw std::runtime_error("Invalid PartialSerializableObject header");
      }

      if (objectIsEmptyForSure) {
        obj = T();
      } else if (full) {
        auto members = obj._as_member_tie();
        logkv::read_members(reader, members);
      } else {
        auto members = obj._as_partial_member_tie();
        logkv::read_members(reader, members);
      }
    } catch (const insufficient_buffer& e) {
      return reader.bytes_processed() + e.get_required_bytes();
    }
    return reader.bytes_processed();
  }
};

} // namespace logkv

#define AUTO_PARTIAL_SERIALIZABLE_MEMBERS(...)                                 \
  auto _as_partial_const_member_tie() const { return std::tie(__VA_ARGS__); }  \
  auto _as_partial_member_tie() { return std::tie(__VA_ARGS__); }              \
  inline static thread_local bool _logkv_snapshot_flag = false;                \
  inline static thread_local bool _full_serialization_flag = false;            \
  static void _logkvStoreSnapshot(bool s) { _logkv_snapshot_flag = s; }        \
  static bool _logkvStoreSnapshot() { return _logkv_snapshot_flag; }           \
  static void _setFullSerialization(bool f) { _full_serialization_flag = f; }  \
  static bool _getFullSerialization() { return _full_serialization_flag; }
