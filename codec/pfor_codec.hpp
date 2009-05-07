// -*- mode: c++ -*-

#ifndef __CODEC__PFOR_CODEC__HPP__
#define __CODEC__PFOR_CODEC__HPP__ 1

#include <boost/numeric/conversion/bounds.hpp>

#include <codec/codec.hpp>

#include <utils/bithack.hpp>
#include <utils/bitpack.hpp>

namespace codec
{
  template <typename Tp>
  struct pfor_codec
  {
    typedef size_t    size_type;
    typedef ptrdiff_t difference_type;

    typedef Tp value_type;

    static const size_type bit_size = sizeof(Tp) * 8;
    
    template <typename _Tp, size_t ByteSize>
    struct __unsigned_value {};
    template <typename _Tp>
    struct __unsigned_value<_Tp,1> { typedef uint8_t value_type; };
    template <typename _Tp>
    struct __unsigned_value<_Tp,2> { typedef uint16_t value_type; };
    template <typename _Tp>
    struct __unsigned_value<_Tp,4> { typedef uint32_t value_type; };
    template <typename _Tp>
    struct __unsigned_value<_Tp,8> { typedef uint64_t value_type; };
    
    
    
    
    template <typename Entries, typename PackedCodes, typename Exceptions>
    static inline
    Tp decompress(size_type pos,
		  const Tp& base,
		  size_type bits,
		  const Entries& entries,
		  const PackedCodes& packed_codes,
		  const Exceptions& exceptions)
    {
      const size_type pos_entry = pos >> 7;
      
      if (entries[pos_entry] == uint32_t(-1))
	return utils::bitpack::unpack(&(*packed_codes.begin()), pos, bits) + base;
      else {
	typedef typename __unsigned_value<Tp, sizeof(Tp)>::value_type uvalue_type;
	
	const size_type mask_exception = ((size_type(1) << 25) - 1);
	
	size_type pos_miss      = (entries[pos_entry] & 0x7f) + (pos_entry << 7);
	size_type pos_exception = (entries[pos_entry] >> 7) & mask_exception;
	
	//std::cerr << "initial exception: pos: " << pos << " miss: " << pos_miss << " exception: "  << pos_exception << std::endl;

	while (pos_miss < pos) {
	  pos_miss += utils::bitpack::unpack(&(*packed_codes.begin()), pos_miss, bits) + 1;
	  ++ pos_exception;
	}
	
	//std::cerr << "searched exception: pos: " << pos << " miss: " << pos_miss << " exception: "  << pos_exception << std::endl;

	return (pos_miss == pos
		? exceptions[pos_exception]
		: utils::bitpack::unpack(&(*packed_codes.begin()), pos, bits) + base);
      }
    };
    
    template <typename Entries, typename PackedCodes, typename Exceptions>
    static inline
    void compress(const Tp* input,
		  size_type input_size,
		  const Tp& base,
		  size_type bits,
		  Entries& entries,
		  PackedCodes& packed_codes,
		  Exceptions& exceptions)
    {
      const size_type input_block_size = (input_size >> 7);
      const size_type input_block_mask = ((size_type(1) << 7) - 1);
      const size_type block_size = 128;
      
      const size_type shift_value = utils::bithack::static_floor_log2<sizeof(Tp) * 8>::result;
      
      // we will use code size multiple of bit-size of Tp
      const size_type input_filled_size = (((input_size + (sizeof(Tp) * 8 - 1)) >> shift_value) << shift_value);
      
      std::vector<Tp, std::allocator<Tp> > codes(input_filled_size);
      
      for (size_type num_block = 0; num_block < input_block_size; ++ num_block)
	compress(input, num_block << 7, (num_block + 1) << 7,
		 base, bits, entries, codes, exceptions);
      
      if ((input_block_size << 7) < input_size)
	compress(input, (input_block_size << 7), input_size,
		 base, bits, entries, codes, exceptions);
      
      // perform bit packing... we will also fill with zero values..
      packed_codes.reserve((input_filled_size >> shift_value) * bits);
      packed_codes.resize((input_filled_size >> shift_value) * bits);
      utils::bitpack::pack(&(*codes.begin()), &(*packed_codes.begin()), codes.size(), bits);
    }
    
    template <typename Entries, typename Codes, typename Exceptions>
    static inline
    void compress(const Tp* input,
		  size_type first,
		  size_type last,
		  const Tp& base,
		  size_type bits,
		  Entries& entries,
		  Codes& codes,
		  Exceptions& exceptions)
    {
      typedef typename __unsigned_value<Tp, sizeof(Tp)>::value_type uvalue_type;

      const uvalue_type mask = ~((uvalue_type(1) << bits) - 1);
      
      const size_type offset = first;
      while (first < last && ! (uvalue_type(input[first] - base) & mask)) {
	codes[first] = input[first] - base;
	++ first;
      }
      
      size_type j = 0;
      std::vector<size_type, std::allocator<size_type> > miss(last - first);

      for (/**/; first != last; ++ first) {
	const Tp code = input[first] - base;
	codes[first] = code;
	miss[j] = first;
	
	j += (uvalue_type(code) & mask) || (j > 0 && (uvalue_type(miss[j] - miss[j - 1]) & mask));
      }
      
      // loop2... create patch list
      entries.push_back(uint32_t(-1));
      if (j > 0) {
	// entry point (32bit) will contain: current exceptions.size() (25bit)
	// the first exception point miss[0] (7bit)
	
	entries.back() = (exceptions.size() << 7) | ((miss[0] - offset) & 0x7f);
	
	size_type prev = miss[0];
	exceptions.push_back(input[prev]);
	for (int i = 1; i < j; ++ i) {
	  const size_type curr = miss[i];
	  exceptions.push_back(input[curr]);
	  codes[prev] = curr - prev - 1;
	  
	  prev = curr;
	}
	codes[prev] = (uvalue_type(1) << bits) - 1;
      }
    }
    
    static inline
    std::pair<Tp, size_type> estimate(const Tp* buffer, size_type sample_size)
    {
      typedef std::vector<Tp, std::allocator<Tp> > buffer_type;
      
      buffer_type sorted(buffer, buffer + sample_size);
      std::sort(sorted.begin(), sorted.end());
            
      double    min_estimate = boost::numeric::bounds<double>::highest();
      size_type min_bits = 32;
      Tp        min_value = 0;
      for (int bits = 1; bits < sizeof(Tp) * 8; ++ bits) {
	std::pair<typename buffer_type::iterator, size_type> result = analyze(sorted.begin(), sorted.end(), bits);
	const double exception_rate = double(sample_size - result.second) / sample_size;
	const double rate = bits + exception_rate * 8 * sizeof(Tp);
	
	const bool found = (rate < min_estimate);

	min_estimate = (found - 1) * min_estimate + found * rate;
	min_bits = (size_type(found - 1) & min_bits) | ((~size_type(found - 1)) & bits);
	min_value = (value_type(found - 1) & min_value) | ((~value_type(found - 1)) & *(result.first));
      }
      return std::make_pair(min_value, min_bits);
    }
    
    

    // analyze sorted range...
    template <typename Iterator>
    static inline
    std::pair<Iterator, size_type> analyze(Iterator first, Iterator last, size_type bits)
    {
      typedef typename __unsigned_value<Tp, sizeof(Tp)>::value_type uvalue_type;

      size_type length = 0;
      uvalue_type range = uvalue_type(1) << bits;
      
      Iterator iter_base = first;
      Iterator iter_lo = first;
      for (Iterator iter_hi = first; iter_hi != last; ++ iter_hi)
	if (*iter_hi - *iter_lo > range) {
	  if (iter_hi - iter_lo > length) {
	    iter_base = iter_lo;
	    length = iter_hi - iter_lo;
	  }
	  while (*iter_hi - *iter_lo >= range) ++ iter_lo;
	}
      return std::make_pair(iter_base, length + 1);
    }
  };
  
};

#endif
