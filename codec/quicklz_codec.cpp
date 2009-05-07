
#define QLZ_COMPRESSION_LEVEL 3
#define QLZ_STREAMING_BUFFER  0

#include "quicklz_codec.hpp"
#include "quicklz.h"
#include "quicklz.c"

namespace codec
{
  quicklz_codec::quicklz_codec() : __handle((void*) new char[QLZ_SCRATCH_COMPRESS]) {}
  quicklz_codec::quicklz_codec(const quicklz_codec&) : __handle((void*) new char[QLZ_SCRATCH_COMPRESS]) {}
  quicklz_codec::~quicklz_codec() { if (__handle) { delete [] ((char*) __handle); } }
  
  void quicklz_codec::compress(const void* source, size_type size, buffer_type& destination)
  {
    destination.reserve(size + 512);
    destination.resize(size + 512);
    const size_type size_compressed = qlz_compress(source, &(*destination.begin()), size, (char*) __handle);
    destination.resize(size_compressed);
  }
  
  void quicklz_codec::decompress(const void* source, size_type size, buffer_type& destination)
  {
    const size_type size_decompressed = qlz_size_decompressed(reinterpret_cast<const char*>(source));
    destination.reserve(size_decompressed);
    destination.resize(size_decompressed);
    const size_type size_decompressed_actual = qlz_decompress(reinterpret_cast<const char*>(source), &(*destination.begin()), (char*) __handle);
  }
};
