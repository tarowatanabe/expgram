
#include <iostream>
#include <string>

#include "utils/compress_stream.hpp"

#include "codec/fastlz_codec.hpp"
#include "codec/quicklz_codec.hpp"
#include "codec/block_device.hpp"

int main(int argc, char** argv)
{
  if (argc <= 1) return 1;

  // sample to use quicklz...
  boost::iostreams::filtering_ostream os;
  os.push(codec::block_sink<codec::fastlz_codec>(argv[1]));
  
  char buffer[4096];
  
  do {
    std::cin.read(buffer, 4096);
    if (std::cin.gcount() > 0)
      os.write(buffer, std::cin.gcount());
  } while(std::cin);
  
}
