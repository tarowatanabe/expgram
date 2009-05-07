// -*- mode: c++ -*-

#ifndef __CODEC__CODEC__HPP__
#define __CODEC__CODEC__HPP__ 1

#include <vector>

namespace codec
{
  struct codec
  {
    typedef size_t    size_type;
    typedef ptrdiff_t difference_type;
    typedef char      char_type;
    typedef std::vector<char_type, std::allocator<char_type> > buffer_type;
    
  };
  
};

#endif

