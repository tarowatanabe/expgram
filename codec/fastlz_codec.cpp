
#include "fastlz_codec.hpp"
#include "fastlz.h"

namespace codec
{
  
  void fastlz_codec::compress(const void* source, size_type size, buffer_type& destination)
  {
    const size_type size_dest = std::max(size_type(66), size + size >> 1);
    
    destination.reserve(size_dest);
    destination.resize(size_dest);
    
    const size_type size_compressed =  fastlz_compress(source, size, &(*destination.begin()));
    destination.resize(size_compressed);
  }
  
  void fastlz_codec::decompress(const void* source, size_type size, buffer_type& destination)
  {
    destination.resize(std::max(size, size_type(128)));
    
    size_type size_decompressed = 0;
    do {
      destination.resize(destination.size() * 2);
      size_decompressed = fastlz_decompress(source, size, &(*destination.begin()), destination.size());
    } while (size_decompressed == 0);
    destination.resize(size_decompressed);    
  }
};
