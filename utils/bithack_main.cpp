
#include <iostream>

#include "utils/bithack.hpp"

int main(int argc, char** argv)
{
  std::cout << utils::bithack::bit_count((int8_t) 86) << std::endl;
  std::cout << utils::bithack::bit_count((int16_t) 86) << std::endl;
  std::cout << utils::bithack::bit_count((int32_t) 86) << std::endl;
  std::cout << utils::bithack::bit_count((int64_t) 86) << std::endl;
  
  std::cout << utils::bithack::static_most_significant_bit<86>::result << std::endl;
  std::cout << utils::bithack::most_significant_bit((int8_t) 86) << std::endl;
  
  std::cout << "log_2 8  " << utils::bithack::floor_log2(8) << " " << utils::bithack::static_floor_log2<8>::result << std::endl;
  std::cout << "log_2 16 " << utils::bithack::floor_log2(16) << std::endl;
  std::cout << "log_2 32 " << utils::bithack::floor_log2(32) << std::endl;
  std::cout << "log_2 64 " << utils::bithack::floor_log2(64) << std::endl;
}
