#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include <logkv/crc.h>

int test_crc32() {
  const std::string testData = "123456789";

  std::cout << "[ Testing CRC-32C (Castagnoli) ]\n";

  const uint32_t EXPECTED_CRC = 0xE3069283;

  uint32_t result = logkv::computeCRC32(testData.data(), testData.size());

  std::cout << "Input:      \"" << testData << "\"\n";
  std::cout << "Expected:   0x" << std::hex << std::uppercase << std::setw(8)
            << std::setfill('0') << EXPECTED_CRC << "\n";
  std::cout << "Calculated: 0x" << std::setw(8) << std::setfill('0') << result
            << "\n";

  if (result == EXPECTED_CRC) {
    std::cout << "Result:     PASS\n\n";
    return 0;
  } else {
    std::cout << "Result:     FAIL\n\n";
    return 1;
  }
}

int test_crc16() {
  const std::string testData = "123456789";
  const uint16_t EXPECTED_CRC = 0x31C3;

  std::cout << "[ Testing CRC-16 (XMODEM) ]\n";

  uint16_t result = logkv::computeCRC16(testData.data(), testData.size());

  std::cout << "Input:      \"" << testData << "\"\n";
  std::cout << "Expected:   0x" << std::hex << std::uppercase << std::setw(4)
            << std::setfill('0') << EXPECTED_CRC << "\n";
  std::cout << "Calculated: 0x" << std::setw(4) << std::setfill('0') << result
            << "\n";

  if (result == EXPECTED_CRC) {
    std::cout << "Result:     PASS\n\n";
    return 0;
  } else {
    std::cout << "Result:     FAIL\n\n";
    return 1;
  }
}

int main() {
  int crc32_status = test_crc32();
  int crc16_status = test_crc16();

  if (crc32_status == 0 && crc16_status == 0) {
    std::cout << "All Tests Passed.\n";
  } else {
    std::cout << "Some Tests Failed.\n";
  }

  return crc32_status | crc16_status;
}