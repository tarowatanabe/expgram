// -*- mode: c++ -*-

#ifndef __CODEC__FASTLZ__HPP__
#define __CODEC__FASTLZ__HPP__ 1

#include <string>
#include <codec/codec.hpp>

namespace codec
{
  class fastlz_codec : public codec
  {
  public:
    
    static const std::string& name()
    {
      static const std::string __name("fastlz");
      return __name;
    }
    
    void compress(const void* source, size_type size, buffer_type& destination);
    void decompress(const void* source, size_type size, buffer_type& destination);
  };
};

#endif
