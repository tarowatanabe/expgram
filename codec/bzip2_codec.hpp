// -*- mode: c++ -*-

#ifndef __CODEC__BZIP2__HPP__
#define __CODEC__BZIP2__HPP__ 1

#include <codec/codec.hpp>

namespace codec
{
  class bzip2_codec : public codec
  {
  public:
    static const std::string& name()
    {
      static const std::string __name("bzip2");
      return __name;
    }
    bzip2_codec() : __os(), __is() { __initialize(); }
    bzip2_codec(const bzip2_codec& x) : __os(), __is() { __initialize(); }
    ~bzip2_codec()
    {
      if (__os) delete __os;
      if (__is) delete __is;
    }
    bzip2_codec& operator=(const bzip2_codec& x) { return *this; }
    
    void compress(const void* source, size_type size, buffer_type& destination)
    {
      destination.clear();
      __os->push(boost::iostreams::back_insert_device<buffer_type>(destination));
      __os->write((char*) source, size);
      __os->pop();
    }
    
    void decompress(const void* source, size_type size, buffer_type& destination)
    {
      __is->push(boost::iostreams::array_source((const char*) source, size));
      destination.clear();
      do {
	const size_t size_prev = destination.size();
	destination.resize(size_prev + 4096);
	__is->read(&(*(destination.begin() + size_prev)), 4096);
	destination.resize(size_prev + __is->gcount());
      } while (*__is);
      __is->pop();
    }

  private:
    void __initialize()
    {
      __os = new boost::iostreams::filtering_ostream();
      __is = new boost::iostreams::filtering_istream();
      
      __os->push(boost::iostreams::bzip2_compressor());
      __is->push(boost::iostreams::bzip2_decompressor());
    }

  private:
    
    boost::iostreams::filtering_ostream* __os;
    boost::iostreams::filtering_istream* __is;
  };
};

#endif
