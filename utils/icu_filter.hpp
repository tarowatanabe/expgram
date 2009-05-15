// -*- mode: c++ -*-

#ifndef __UTILS__ICU_FILTER__HPP__
#define __UTILS__ICU_FILTER__HPP__ 1

#include <memory>
#include <stdexcept>

#include <unicode/uchar.h>
#include <unicode/ucnv.h>

#include <boost/iostreams/filter/symmetric.hpp>
#include <boost/iostreams/pipeline.hpp>
#include <boost/iostreams/detail/ios.hpp>

#include <string>

namespace utils
{

  struct icu_filter_param
  {
    typedef enum {
      substitute = 0,
      skip,
      stop,
      escape,
      escape_icu,
      escape_java,
      escape_c,
      escape_xml,
      escape_xml_hex,
      escape_xml_dec,
      escape_unicode,
      __callback_size,
    } callback_type;
    
  };
  
  class __icu_filter_impl : public icu_filter_param
  {
  public:
    typedef char      char_type;
    typedef size_t    size_type;
    typedef ptrdiff_t difference_type;
    
    typedef icu_filter_param::callback_type callback_type;

    __icu_filter_impl(const std::string& codepage_from,
		      const std::string& codepage_to,
		      const callback_type callback=stop)
      : ucnv_from(0), ucnv_to(0), pivot_start(0) { __initialize(codepage_from, codepage_to, callback); }
    ~__icu_filter_impl() { close(); __clear(); }
    
  public:
    bool filter(const char_type*& src_begin, const char_type* src_end,
		char_type*& dest_begin, char_type* dest_end,
		bool flush)
    {
      UChar* pivot_end = pivot_start + 4096;
      
      UErrorCode status = U_ZERO_ERROR;
      ucnv_toUnicode(ucnv_from, &pivot_target, pivot_end, &src_begin, src_end, 0, flush, &status);
      if (status != U_BUFFER_OVERFLOW_ERROR && U_FAILURE(status)) {
	UErrorCode status_getname = U_ZERO_ERROR;
	const char* encoding = ucnv_getName(ucnv_from, &status_getname);
	throw BOOST_IOSTREAMS_FAILURE(std::string("ucnv_toUnicode(): ") + u_errorName(status) + " from " + encoding);
      }
    
      status = U_ZERO_ERROR;
      ucnv_fromUnicode(ucnv_to, &dest_begin, dest_end, &pivot_source, pivot_target, 0, flush, &status);
      if (status != U_BUFFER_OVERFLOW_ERROR && U_FAILURE(status)) {
	UErrorCode status_getname = U_ZERO_ERROR;
	const char* encoding = ucnv_getName(ucnv_to, &status_getname);
	throw BOOST_IOSTREAMS_FAILURE(std::string("ucnv_fromUnicode(): ") + u_errorName(status) + " to " + encoding);
      }
    
      if (pivot_source == pivot_target) {
	pivot_source = pivot_start;
	pivot_target = pivot_start;
      }
      
      return status == U_BUFFER_OVERFLOW_ERROR;
    }
    
    void close()
    {
      if (ucnv_from)
	ucnv_reset(ucnv_from);
      if (ucnv_to)
	ucnv_reset(ucnv_to);
    }
    
  private:
    void __initialize(const std::string& codepage_from,
		      const std::string& codepage_to,
		      const callback_type callback=stop);
    void __clear();    
    
  private:
    UConverter* ucnv_from;
    UConverter* ucnv_to;

    UChar*       pivot_start;    
    const UChar* pivot_source;
    UChar*       pivot_target;
  };
  
  template <typename Alloc = std::allocator<char> >
  struct basic_icu_filter
    : boost::iostreams::symmetric_filter<__icu_filter_impl, Alloc>,
    icu_filter_param
  {
  private:
    typedef __icu_filter_impl impl_type;
    typedef boost::iostreams::symmetric_filter<__icu_filter_impl, Alloc> base_type;
    
  public:
    typedef typename base_type::char_type char_type;
    typedef typename base_type::category  category;
    
    typedef typename impl_type::callback_type callback_type;
    

    basic_icu_filter(const std::string codepage_from = "",
		     const std::string codepage_to = "",
		     const callback_type callback = impl_type::stop,
		     int buffer_size = 4096)
      : base_type(buffer_size, codepage_from, codepage_to, callback) {}
  };
  
  typedef basic_icu_filter<> icu_filter;

};

#endif
