#include <logkv/store.h>
using namespace logkv;

#include <logkv/bytes.h>

#include <boost/unordered/unordered_flat_map.hpp>

#include <iostream>

int main(int argc, char* argv[]) {
  const std::string dirStr = "./hellodata";

  Bytes k = bytesDecodeHex("aabbcc");
  Bytes v = bytesDecodeHex("ddeeff");

  std::cout << "test: write data" << std::endl;
  std::cout << "  key  : " << bytesEncodeHex(k) << std::endl;
  std::cout << "  value: " << bytesEncodeHex(v) << std::endl;

  {
    Store<boost::unordered_flat_map, Bytes, Bytes> objs(
      dirStr, StoreFlags::createDir | StoreFlags::deleteData);
    objs.update(k, v);
    objs.save();
  }

  std::cout << "test: read data" << std::endl;

  {
    Store<boost::unordered_flat_map, Bytes, Bytes> objs2(dirStr);

    if (objs2().find(k) == objs2().end()) {
      std::cout << "test failed: no key" << std::endl;
      return 1;
    }

    std::cout << "value: " << bytesEncodeHex(objs2[k]) << std::endl;

    if (objs2[k] == v) {
      std::cout << "test passed: correct value." << std::endl;
    } else {
      std::cout << "test failed: wrong value." << std::endl;
      return 1;
    }
  }
  return 0;
}