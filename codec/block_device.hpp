// -*- mode: c++ -*-

#ifndef __CODEC__BLOCK_DEVICE__HPP__
#define __CODEC__BLOCK_DEVICE__HPP__ 1

// TODO!

#include <stdint.h>

#include <string>
#include <algorithm>
#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>

#include <boost/filesystem/path.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/shared_ptr.hpp>

#include <codec/quicklz_codec.hpp>

#include <utils/repository.hpp>
#include <utils/map_file.hpp>

namespace codec
{
  
  struct __block_device_params
  {
    typedef int64_t   off_type;
    typedef size_t    size_type;
    typedef char      char_type;
    
    typedef boost::iostreams::stream_offset stream_offset;
    typedef boost::filesystem::path         path_type;
    
    static const off_type block_size = 1024 * 8;
  };
  
  template <typename Codec>
  class block_sink  : public __block_device_params
  {
  public:
    struct category : public boost::iostreams::sink_tag,
		      public boost::iostreams::closable_tag {};
    
  public:
    block_sink(const path_type& file, const size_type buffer_size=4096) : pimpl(new impl()) { open(file, buffer_size); }
    block_sink() : pimpl (new impl()) {}
    

  public:
    std::streamsize write(const char_type* s, std::streamsize n)
    {
      std::streamsize __n = 0;
      std::streamsize available = n;
      while (available) {
	const std::streamsize written = pimpl->write(s, available);
	if (written == -1)
	  return (__n > 0 ? __n : -1);
	available -= written;
	s += written;
	__n += written;
      }
      return __n;
    }
    
    bool is_open() const { return pimpl->is_open(); }
    void open(const path_type& file, const size_type buffer_size=4096) { pimpl->open(file, buffer_size); }
    void close() { pimpl->close(); }
    
  private:
    struct impl
    {
      typedef Codec                               codec_type;
      typedef typename codec_type::buffer_type    buffer_type;
      typedef boost::iostreams::filtering_ostream ostream_type;
      
      // buffer
      buffer_type buffer;
      buffer_type buffer_compressed;
      off_type offset;
      
      // data stream
      boost::shared_ptr<ostream_type> dstream;
      // index stream
      boost::shared_ptr<ostream_type> istream;
      // codec...
      codec_type codec;
      
      // path...
      path_type filename;
      off_type filesize;
      off_type filesize_compressed;
      
      // actual 
      impl() : offset(0), filesize(0), filesize_compressed(0) {}
      ~impl() { close(); }
      
      std::streamsize write(const char_type* s, std::streamsize n);
      void open(const path_type& file, const size_type buffer_size=4096);
      bool is_open() const;
      void close();
    };
    
    boost::shared_ptr<impl> pimpl;
  };
  
  template <typename Codec>
  inline
  bool block_sink<Codec>::impl::is_open() const { return dstream; }
  
  template <typename Codec>
  inline
  std::streamsize block_sink<Codec>::impl::write(const char_type* s, std::streamsize n)
  {
    if (! is_open()) return -1;
    
    const std::streamsize copy_size = std::min(std::streamsize(buffer.size() - offset), n);
    
    std::copy(s, s + copy_size, buffer.begin() + offset);
    offset += copy_size;
    filesize += copy_size;
    
    if (offset == buffer.size()) {
      
      // compress the block...
      buffer_compressed.clear();
      codec.compress(&(*buffer.begin()), buffer.size(), buffer_compressed);
      filesize_compressed += buffer_compressed.size();
      dstream->write(&(*buffer_compressed.begin()), buffer_compressed.size());
      istream->write((char*) &filesize_compressed, sizeof(filesize_compressed));
      
      offset = 0;
    }
    return copy_size;
  }
  
  template <typename Codec>
  inline
  void block_sink<Codec>::impl::close()
  {
    if (is_open()) {
      
      if (offset > 0) {
	buffer.resize(offset);
	
	buffer_compressed.clear();
	codec.compress(&(*buffer.begin()), buffer.size(), buffer_compressed);
	filesize_compressed += buffer_compressed.size();
	dstream->write(&(*buffer_compressed.begin()), buffer_compressed.size());
	istream->write((char*) &filesize_compressed, sizeof(filesize_compressed));
	
	offset = 0;
      }
      
      if (filesize_compressed == 0)
	istream->write((char*) &filesize_compressed, sizeof(filesize_compressed));
      
      // finish...
      istream.reset();
      dstream.reset();
      
      // file-size info...
      {
	typedef utils::repository repository_type;
	repository_type repository(filename, repository_type::read);
	
	std::ostringstream size_stream;
	size_stream << filesize;
	repository["size"] = size_stream.str();
      }
    }
    
    buffer.clear();
    buffer_compressed.clear();
    offset = 0;
    
    filename = path_type();
    filesize = 0;
    filesize_compressed = 0;
  }
  
  template <typename Codec>
  inline
  void block_sink<Codec>::impl::open(const path_type& file, const size_type buffer_size)
  {
    typedef utils::repository repository_type;
    
    close();
    
    filename = file;
    filesize = 0;
    filesize_compressed = 0;
    
    buffer.reserve(block_size);
    buffer.resize(block_size);
    buffer_compressed.reserve(block_size);
    offset = 0;
    
    repository_type repository(filename, repository_type::write);
    repository["type"] = Codec::name() + "-block";
    
    // data stream
    dstream.reset(new ostream_type());
    dstream->push(boost::iostreams::file_sink(repository.path("data").file_string(), std::ios_base::out | std::ios_base::trunc), buffer_size);
    
    // index stream
    istream.reset(new ostream_type());
    istream->push(boost::iostreams::file_sink(repository.path("index").file_string(), std::ios_base::out | std::ios_base::trunc));    
  }
  
};

#endif

