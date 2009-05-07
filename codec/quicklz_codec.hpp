// -*- mode: c++ -*-

#ifndef __CODEC__QUICKLZ__HPP__
#define __CODEC__QUICKLZ__HPP__ 1

#include <string>
#include <codec/codec.hpp>

namespace codec
{
  class quicklz_codec : public codec
  {
  public:
    
    static const std::string& name()
    {
      static const std::string __name("quicklz");
      return __name;
    }

    quicklz_codec();
    quicklz_codec(const quicklz_codec& x);
    ~quicklz_codec();
    quicklz_codec& operator=(const quicklz_codec& x) { return *this; }
    
    void compress(const void* source, size_type size, buffer_type& destination);
    void decompress(const void* source, size_type size, buffer_type& destination);
    
    void* __handle;
  };
};

#endif
