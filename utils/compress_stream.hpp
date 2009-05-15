// -*- Mode: C++ -*-

#ifndef __UTILS__COMPRESS_STREAM__HPP__
#define __UTILS__COMPRESS_STREAM__HPP__ 1

#include <cstring>
#include <iostream>
#include <fstream>

#include <unistd.h>

#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filter/bzip2.hpp>
#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/device/file.hpp>

#include <boost/filesystem.hpp>

#include <utils/icu_filter.hpp>

namespace utils
{
  namespace impl
  {
    
    typedef enum {
      COMPRESS_STREAM_GZIP,
      COMPRESS_STREAM_BZIP,
      COMPRESS_STREAM_UNKNOWN
    } compress_format_type;
    
    inline compress_format_type compress_iformat (const std::string& filename)
    {
      char buffer[8];
      
      ::memset(buffer, 0, sizeof(char)*3);
      std::ifstream ifs(filename.c_str());
      ifs.read((char*) buffer, sizeof(char)*3);
      
      if (buffer[0] == '\037' && buffer[1] == '\213')
	return COMPRESS_STREAM_GZIP;
      else if (buffer[0] == 'B' && buffer[1] == 'Z' && buffer[2] == 'h')
	return COMPRESS_STREAM_BZIP;
      else
	return COMPRESS_STREAM_UNKNOWN;
    }
    inline compress_format_type compress_iformat(const boost::filesystem::path& path) { return compress_iformat(path.file_string()); }
    
    inline compress_format_type compress_oformat (const std::string& filename)
    {
      if (filename.size() > 3 && strncmp(&(filename.c_str()[filename.size() - 3]), ".gz", 3) == 0)
	return COMPRESS_STREAM_GZIP;
      else if (filename.size() > 4 && strncmp(&(filename.c_str()[filename.size() - 4]), ".bz2", 4) == 0)
	return COMPRESS_STREAM_BZIP;
      else
	return COMPRESS_STREAM_UNKNOWN;
    }
    inline compress_format_type compress_oformat(const boost::filesystem::path& path) { return compress_oformat(path.file_string()); }    
  };
  
  class compress_ostream : public boost::iostreams::filtering_ostream
  {
  private:
    typedef boost::iostreams::filtering_ostream stream_type;
    
  public:
    compress_ostream(const boost::filesystem::path& path,
		     const size_t buffer_size = 4096) { __initialize(path, buffer_size); }
    
    compress_ostream(const boost::filesystem::path& path,
		     const std::string& codepage_from,
		     const std::string& codepage_to,
		     utils::icu_filter::callback_type callback = utils::icu_filter::stop,
		     size_t buffer_size = 4096) { __initialize(path, codepage_from, codepage_to, callback, buffer_size); }
    
  private:
    void __initialize(const boost::filesystem::path& path,
		      size_t buffer_size = 4096)
    {
      if (path.file_string() == "-")  
	__stream().push(boost::iostreams::file_descriptor_sink(::dup(STDOUT_FILENO), true), buffer_size);
      else {
	switch (impl::compress_oformat(path)) {
	case impl::COMPRESS_STREAM_GZIP:
	  __stream().push(boost::iostreams::gzip_compressor());
	  break;
	case impl::COMPRESS_STREAM_BZIP:
	  __stream().push(boost::iostreams::bzip2_compressor());
	  break;
	}
	__stream().push(boost::iostreams::file_sink(path.file_string(), std::ios_base::out | std::ios_base::trunc), buffer_size);
      }
    }

    void __initialize(const boost::filesystem::path& path,
		      const std::string& codepage_from,
		      const std::string& codepage_to,
		      utils::icu_filter::callback_type callback = utils::icu_filter::stop,
		      size_t buffer_size = 4096)
    {
      __stream().push(utils::icu_filter(codepage_from, codepage_to, callback));
      
      if (path.file_string() == "-")
	__stream().push(boost::iostreams::file_descriptor_sink(::dup(STDOUT_FILENO), true), buffer_size);
      else {
	switch (impl::compress_oformat(path)) {
	case impl::COMPRESS_STREAM_GZIP:
	  __stream().push(boost::iostreams::gzip_compressor());
	  break;
	case impl::COMPRESS_STREAM_BZIP:
	  __stream().push(boost::iostreams::bzip2_compressor());
	  break;
	}
	__stream().push(boost::iostreams::file_sink(path.file_string(), std::ios_base::out | std::ios_base::trunc), buffer_size);
      }
    }
    
    stream_type& __stream() { return static_cast<stream_type&>(*this); }
  };
  
  class compress_istream : public boost::iostreams::filtering_istream
  {
  private:
    typedef boost::iostreams::filtering_istream stream_type;
    
  public:
    compress_istream(const boost::filesystem::path& path,
		     size_t buffer_size = 4096) { __initialize(path, buffer_size); }
    compress_istream(const boost::filesystem::path& path,
		     const std::string& codepage_from,
		     const std::string& codepage_to,
		     utils::icu_filter::callback_type callback = utils::icu_filter::stop,
		     size_t buffer_size = 4096) { __initialize(path, codepage_from, codepage_to, callback, buffer_size); }
  private:
    void __initialize(const boost::filesystem::path& path,
		      size_t buffer_size = 4096)
    {
      if (path.file_string() == "-")  
	__stream().push(boost::iostreams::file_descriptor_source(::dup(STDIN_FILENO), true), buffer_size);
      else {
	switch (impl::compress_iformat(path)) {
	case impl::COMPRESS_STREAM_GZIP:
	  __stream().push(boost::iostreams::gzip_decompressor());
	  break;
	case impl::COMPRESS_STREAM_BZIP:
	  __stream().push(boost::iostreams::bzip2_decompressor());
	  break;
	}
	__stream().push(boost::iostreams::file_source(path.file_string()), buffer_size);
      }
    }
    
    void __initialize(const boost::filesystem::path& path,
		      const std::string& codepage_from,
		      const std::string& codepage_to,
		      utils::icu_filter::callback_type callback = utils::icu_filter::stop,
		      size_t buffer_size = 4096)
    {
      __stream().push(utils::icu_filter(codepage_from, codepage_to, callback));
      
      if (path.file_string() == "-")  
	__stream().push(boost::iostreams::file_descriptor_source(::dup(STDIN_FILENO), true), buffer_size);
      else {
	switch (impl::compress_iformat(path)) {
	case impl::COMPRESS_STREAM_GZIP:
	  __stream().push(boost::iostreams::gzip_decompressor());
	  break;
	case impl::COMPRESS_STREAM_BZIP:
	  __stream().push(boost::iostreams::bzip2_decompressor());
	  break;
	}
	__stream().push(boost::iostreams::file_source(path.file_string()), buffer_size);
      }
    }
    
    stream_type& __stream() { return static_cast<stream_type&>(*this); }
  };
  
};

#endif
