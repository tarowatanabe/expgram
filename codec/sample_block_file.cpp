
#include <iostream>
#include <string>

#include "utils/compress_stream.hpp"

#include "codec/block_file.hpp"
#include "codec/fastlz_codec.hpp"
#include "codec/quicklz_codec.hpp"

int main(int argc, char** argv)
{
  if (argc >= 2) {
    
    codec::block_file<char, codec::fastlz_codec> file(argv[1]);
    
    std::cerr << "size: " << file.size()
	      << " file size: " << file.file_size()
	      << std::endl;
    
    for (codec::block_file<char, codec::fastlz_codec>::iterator iter = file.begin(); iter != file.end(); ++ iter)
      std::cout.write(&(*iter), 1);
    
  } else {
    
    
  }
}
