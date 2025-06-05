#ifndef _LOGKV_OBJECT_H_
#define _LOGKV_OBJECT_H_

#include <cstddef>

namespace logkv {

/**
 * Interface to be implemented by objects to be used as keys or values in a
 * logkv::Store.
 */
class Object {
public:
  /**
   * Check if object is empty. Empty value (V) objects in a K,V mapping are not
   * stored; they are used to signal an absent key or a key to be erased.
   * A default-constructed Object should be in an empty state.
   * @return `true` if object is empty, `false` otherwise.
   */
  virtual bool empty() const = 0;

  /**
   * Writes the object to dest. If the object needs more than size bytes
   * in dest, it should not write itself.
   * Should throw an exception if there is any write error.
   * @param dest Destination buffer to write the serialized object to
   * @param size Bytes currently available to write in `dest`.
   * @return Total bytes written or required to write the entire object.
   */
  virtual size_t serialize(char* dest, size_t size) const = 0;

  /**
   * Reads the object from src. If the object needs more than size bytes
   * in src, it should not read itself.
   * This method does not have to return the definitive amount of bytes
   * required to read the object; it can return the minimum amount it
   * knows is missing, if it e.g. needs to read a data size field first.
   * Should throw an exception if there is any read error.
   * @param src Source buffer to read the serialized object from.
   * @param size Amount of bytes available to read in `src`.
   * @return Total bytes read or minimum required to continue reading.
   */
  virtual size_t deserialize(const char* src, size_t size) = 0;
};

} // namespace logkv

#endif
