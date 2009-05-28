// -*- mode: c++ -*-

#ifndef __EXPGRAM__STAT__HPP__
#define __EXPGRAM__STAT__HPP__ 1

#include <stdint.h>

namespace expgram
{
  struct Stat
  {
    typedef uint64_t size_type;

    typedef       size_type& reference;
    typedef const size_type& const_reference;
    
    Stat() : __bytes(0), __compressed(0), __cache(0) {}
    Stat(const size_type _bytes,
	 const size_type _compressed,
	 const size_type _cache)
      : __bytes(_bytes), __compressed(_compressed), __cache(_cache) {}
    
    inline const_reference bytes() const { return __bytes; }
    inline       reference bytes()       { return __bytes; }
    
    inline const_reference compressed() const { return __compressed; }
    inline       reference compressed()       { return __compressed; }
    
    inline const_reference cache() const { return __cache; }
    inline       reference cache()       { return __cache; }
    
    Stat& operator+=(const Stat& x)
    {
      __bytes      += x.__bytes;
      __compressed += x.__compressed;
      __cache      += x.__cache;
      return *this;
    }
    
    Stat& operator-=(const Stat& x)
    {
      __bytes      -= x.__bytes;
      __compressed -= x.__compressed;
      __cache      -= x.__cache;
      return *this;
    }
    
    size_type __bytes;
    size_type __compressed;
    size_type __cache;
  };
  
};

#endif
