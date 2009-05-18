// -*- mode: c++ -*-

#ifndef __EXPGRAM__NGRAM__HPP__
#define __EXPGRAM__NGRAM__HPP__ 1

#include <stdint.h>

#include <expgram/Word.hpp>
#include <expgram/Vocab.hpp>
#include <expgram/NGramIndex.hpp>

#include <boost/array.hpp>

#include <utils/hashmurmur.hpp>
#include <utils/packed_vector.hpp>
#include <utils/succinct_vector.hpp>
#include <utils/map_file.hpp>
#include <utils/mathop.hpp>

namespace expgram
{
  class NGram
  {
  public:
    typedef Word               word_type;
    typedef Vocab              vocab_type;
    
    typedef size_t             size_type;
    typedef ptrdiff_t          difference_type;
    
    typedef word_type::id_type id_type;
    typedef uint64_t           count_type;
    typedef float              logprob_type;
    typedef double             prob_type;
    typedef uint8_t            quantized_type;
    
    typedef boost::filesystem::path path_type;
    
  public:
    struct ShardData
    {
    public:
      typedef utils::map_file<logprob_type, std::allocator<logprob_type> >                 logprob_set_type;
      typedef utils::packed_vector_mapped<quantized_type, std::allocator<quantized_type> > quantized_set_type;
      typedef boost::array<logprob_type, 256>                                              logprob_map_type;
      typedef std::vector<logprob_map_type, std::allocator<logprob_map_type> >             logprob_map_set_type;
      
      ShardData()
	: logprobs(), quantized(), maps(), offset(0) {}
      ShardData(const path_type& path)
      	: logprobs(), quantized(), maps(), offset(0) { open(path); }
      
      void open(const path_type& path);
      void write(const path_type& file) const;
      path_type path() const { return (quantized.is_open() ? quantized.path().parent_path() : logprobs.path().parent_path()); }
      
      void close() { clear(); }
      void clear()
      {
	logprobs.clear();
	quantized.clear();
	maps.clear();
	offset = 0;
      }
      
      logprob_type operator()(size_type pos, int order) const
      {
	return (quantized.is_open() ? maps[order][quantized[pos - offset]] : logprobs[pos - offset]);
      }
      
      size_type size() const
      {
	return (quantized.is_open() ? quantized.size() + offset : logprobs.size() + offset);
      }

      bool is_quantized() const { return quantized.is_open(); }
      
      logprob_set_type     logprobs;
      quantized_set_type   quantized;
      logprob_map_set_type maps;
      size_type            offset;
    };
    
    typedef ShardData  shard_data_type;
    typedef std::vector<shard_data_type, std::allocator<shard_data_type> > shard_data_set_type;
    typedef NGramIndex shard_index_type;
    
  public:
    NGram(const int _debug=0) : debug(_debug) { clear(); }
    NGram(const path_type& path, const size_type shard_size=16, const int _debug=0) : debug(_debug) { open(path, shard_size); }
    
  public:
    static const logprob_type logprob_min() { return boost::numeric::bounds<logprob_type>::lowest(); }
    
    
  public:
    template <typename Iterator>
    bool exists(Iterator first, Iterator last) const
    {
      if (first == last) return false;
      return index.traverse(first, last).first == last;
    }
    
    template <typename Iterator>
    logprob_type logbound(Iterator first, Iterator last) const
    {
      if (first == last) return 0.0;
      
      first = std::max(first, last - index.order());
      
      bool performed_backoff = false;
      logprob_type logbackoff = 0.0;
      for (/**/; first != last - 1; ++ first) {
	const int order = last - first;
	const size_type shard_index = index.shard_index(first, last);
	std::pair<Iterator, size_type> result = index.traverse(shard_index, first, last);

	if (result.first == last) {
	  const logprob_type __logbound = (! performed_backoff && result.second < logbounds[shard_index].size()
					  ? logbounds[shard_index](result.second, order)
					  : logprobs[shard_index](result.second, order));
	  if(__logbound != logprob_min())
	    return logbackoff + __logbound;
	  else {
	    const size_type parent = index[shard_index].parent(result.second);
	    if (parent != size_type(-1))
	      logbackoff += backoffs[shard_index](parent, order - 1);
	  }
	} else if (result.first == last - 1)
	  logbackoff += backoffs[shard_index](result.second, order - 1);
	
	performed_backoff = true;
      }
      
      const int order = last - first;
      const size_type shard_index = index.shard_index(first, last);
      std::pair<Iterator, size_type> result = index.traverse(shard_index, first, last);
      
      if (result.first == last) {
	const logprob_type __logbound = (! performed_backoff && result.second < logbounds[shard_index].size()
					 ? logbounds[shard_index](result.second, order)
					 : logprobs[shard_index](result.second, order));
	return logbackoff + (__logbound != logprob_min() ? __logbound : smooth);
      } else
	return logbackoff + smooth;
    }
    
    template <typename Iterator>
    logprob_type operator()(Iterator first, Iterator last) const
    {
      return logprob(first, last);
    }
    
    template <typename Iterator>
    logprob_type logprob(Iterator first, Iterator last) const
    {
      if (first == last) return 0.0;
      
      first = std::max(first, last - index.order());
      
      logprob_type logbackoff = 0.0;
      for (/**/; first != last - 1; ++ first) {
	const int order = last - first;
	const size_type shard_index = index.shard_index(first, last);
	std::pair<Iterator, size_type> result = index.traverse(shard_index, first, last);
	
	if (result.first == last) {
	  const logprob_type __logprob = logprobs[shard_index](result.second, order);
	  if (__logprob != logprob_min())
	    return logbackoff + __logprob;
	  else {
	    const size_type parent = index[shard_index].parent(result.second);
	    if (parent != size_type(-1))
	      logbackoff += backoffs[shard_index](parent, order - 1);
	  }
	} else if (result.first == last - 1)
	  logbackoff += backoffs[shard_index](result.second, order - 1);
      }
      
      const int order = last - first;
      const size_type shard_index = index.shard_index(first, last);
      std::pair<Iterator, size_type> result = index.traverse(shard_index, first, last);
      
      return logbackoff + (result.first == last && logprobs[shard_index](result.second, order) != logprob_min()
			   ? logprobs[shard_index](result.second, 1)
			   : smooth);
    }
    
    
  public:
    path_type path() const { return index.path().parent_path(); }
    size_type size() const { return index.size(); }
    bool empty() const { return index.empty(); }
    
    void open(const path_type& path,
	      const size_type shard_size=16);
    void write(const path_type& path) const;
    void dump(const path_type& path) const;
    
    void open_shard(const path_type& path, int shard);
    
    void close() { clear(); }
    void clear()
    {
      index.clear();
      logprobs.clear();
      backoffs.clear();
      logbounds.clear();
      smooth = utils::mathop::log(1e-7);
    }
    
    
    void quantize();
    void bounds();
    
    bool is_open() const { return index.is_open(); }
    bool has_bounds() const { return ! logbounds.empty(); }
    
    
  private:
    void open_binary(const path_type& path);
    void open_arpa(const path_type& path,
		   const size_type shard_size=16);
    
  public:
    shard_index_type    index;
    shard_data_set_type logprobs;
    shard_data_set_type backoffs;
    shard_data_set_type logbounds;
    
    logprob_type   smooth;
    int debug;
  };
  
};

#endif
