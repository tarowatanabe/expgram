// -*- mode: c++ -*-
//
//  Copyright(C) 2009-2012 Taro Watanabe <taro.watanabe@nict.go.jp>
//

#ifndef __EXPGRAM__NGRAM__HPP__
#define __EXPGRAM__NGRAM__HPP__ 1

#include <stdint.h>

#include <expgram/Word.hpp>
#include <expgram/Vocab.hpp>
#include <expgram/NGramIndex.hpp>
#include <expgram/Stat.hpp>

#include <boost/array.hpp>

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

    typedef Stat stat_type;
    
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
      
      void populate()
      {
	logprobs.populate();
	quantized.populate();
      }

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
      
      stat_type stat() const
      {
	return (quantized.is_open()
		? stat_type(quantized.size_bytes(), quantized.size_compressed(), quantized.size_cache())
		: stat_type(logprobs.size_bytes(), logprobs.size_compressed(), logprobs.size_cache()));
      }

      
      logprob_set_type     logprobs;
      quantized_set_type   quantized;
      logprob_map_set_type maps;
      size_type            offset;
    };
    
    typedef ShardData  shard_data_type;
    typedef std::vector<shard_data_type, std::allocator<shard_data_type> > shard_data_set_type;
    typedef NGramIndex shard_index_type;
    
    typedef shard_index_type::state_type state_type;

  public:
    NGram(const int _debug=0) : debug(_debug) { clear(); }
    NGram(const path_type& path,
	  const size_type shard_size=16,
	  const int _debug=0)
      : debug(_debug) { open(path, shard_size); }
    
  public:
    static const logprob_type logprob_min() { return boost::numeric::bounds<logprob_type>::lowest(); }
    static const logprob_type logprob_bos() { return -99.0 * M_LN10; }
    
  public:
    
    state_type root() const { return index.root(); }

    template <typename Iterator>
    std::pair<Iterator, Iterator> prefix(Iterator first, Iterator last) const
    {
      return index.prefix(first, last);
    }
    
    template <typename Iterator>
    state_type suffix(Iterator first, Iterator last) const
    {
      return index.suffix(first, last);
    }
    
    template <typename _Word>
    std::pair<state_type, logprob_type> logbound(state_type state, const _Word& word, bool backoffed=false, int max_order=0) const
    {
      return logbound(state, index.vocab()[word], backoffed, max_order);
    }
    
    std::pair<state_type, logprob_type> logbound(state_type state, const id_type& word, bool backoffed=false, int max_order=0) const
    {
      // returned state... maximum suffix of state + word, since we may forced backoff :-)
      max_order = utils::bithack::branch(max_order <= 0, index.order(), utils::bithack::min(index.order(), max_order));
      
      int order = index.order(state) + 1;
      while (order > max_order) {
	state = index.suffix(state);
	order = index.order(state) + 1;
      }
      
      state_type state_ret;
      logprob_type logbackoff = 0.0;
      for (;;) {
	const state_type state_next = index.next(state, word);
	
	if (! state_next.is_root_node()) {
	  
	  if (state_ret.is_root())
	    state_ret = (order >= max_order ? index.suffix(state_next) : state_next);
	  
	  const size_type shard_index = utils::bithack::branch(state_next.is_root_shard(), size_type(0), state_next.shard());
	  const logprob_type __logprob = (! backoffed && state_next.node() < logbounds[shard_index].size()
					  ? logbounds[shard_index](state_next.node(), order)
					  : logprobs[shard_index](state_next.node(), order));
	  
	  if (__logprob != logprob_min())
	    return std::make_pair(state_ret, __logprob + logbackoff);
	}
	
	backoffed = true;
	
	if (state.is_root())
	  return std::make_pair(state_ret, (index.is_bos(word) ? logprob_bos() : smooth) + logbackoff);
	
	// we will backoff
	const size_type shard_index = utils::bithack::branch(state.is_root_shard(), size_type(0), state.shard());
	logbackoff += backoffs[shard_index](state.node(), order - 1);
	state = index.suffix(state);
	order = index.order(state) + 1;
      }
    }
    
    template <typename _Word>
    std::pair<state_type, logprob_type> logprob(state_type state, const _Word& word, bool backoffed = false, int max_order = 0) const
    {
      return logprob(state, index.vocab()[word], backoffed, max_order);
    }
    
    std::pair<state_type, logprob_type> logprob(state_type state, const id_type& word, bool backoffed = false, int max_order = 0) const
    {
      // returned state... maximum suffix of state + word, since we may forced backoff :-)
      max_order = utils::bithack::branch(max_order <= 0, index.order(), utils::bithack::min(index.order(), max_order));
      
      int order = index.order(state) + 1;
      while (order > max_order) {
	state = index.suffix(state);
	order = index.order(state) + 1;
      }

      state_type state_ret;

      logprob_type logbackoff = 0.0;
      for (;;) {
	const state_type state_next = index.next(state, word);

	if (! state_next.is_root_node()) {
	  
	  if (state_ret.is_root())
	    state_ret = (order >= max_order ? index.suffix(state_next) : state_next);
	  
	  const size_type shard_index = utils::bithack::branch(state_next.is_root_shard(), size_type(0), state_next.shard());
	  const logprob_type __logprob = logprobs[shard_index](state_next.node(), order);
	  
	  if (__logprob != logprob_min())
	    return std::make_pair(state_ret, __logprob + logbackoff);
	}

	backoffed = true;
	
	if (state.is_root())
	  return std::make_pair(state_ret, (index.is_bos(word) ? logprob_bos() : smooth) + logbackoff);
	
	// we will backoff
	const size_type shard_index = utils::bithack::branch(state.is_root_shard(), size_type(0), state.shard());
	logbackoff += backoffs[shard_index](state.node(), order - 1);
	state = index.suffix(state);
	order = index.order(state) + 1;
      }
    }

    template <typename Iterator>
    std::pair<Iterator, Iterator> ngram_prefix(Iterator first, Iterator last) const
    {
      if (first == last || first + 1 == last) return std::make_pair(first, last);
      
      const size_type shard_index = index.shard_index(first, last);
      std::pair<Iterator, size_type> result = index.traverse(shard_index, first, last);
      
      return std::make_pair(first, std::min(result.first + 1, last));
    }

    template <typename Iterator>
    std::pair<Iterator, Iterator> ngram_suffix(Iterator first, Iterator last) const
    {
      if (first == last || first + 1 == last) return std::make_pair(first, last);
      
      first = std::max(first, last - index.order());
      
      int       shard_prev = -1;
      size_type node_prev = size_type(-1);
      
      for (/**/; first != last - 1; ++ first) {
	const size_type shard_index = index.shard_index(first, last);
	
	std::pair<Iterator, size_type> result = index.traverse(shard_index, first, last, shard_prev, node_prev);
	
	shard_prev = -1;
	node_prev = size_type(-1);
	
	if (result.first == last)
	  return std::make_pair(first, last);
	else if (result.first == last - 1) {
	  shard_prev = shard_index;
	  node_prev = result.second;
	}
      }
      
      return std::make_pair(first, last);
    }

    template <typename Iterator>
    bool exists(Iterator first, Iterator last) const
    {
      if (first == last) return false;
      return index.traverse(first, last).first == last;
    }
    
    template <typename Iterator>
    logprob_type logbound(Iterator first, Iterator last, bool smooth_smallest=false) const
    {
      if (first == last) return 0.0;

      const int order = last - first;
      
      if (order >= index.order())
	return logprob(first, last, smooth_smallest);
      
      if (order >= 2) { 
	const size_type shard_index = index.shard_index(first, last);
	const size_type shard_index_backoff = size_type((order == 2) - 1) & shard_index;
	std::pair<Iterator, size_type> result = index.traverse(shard_index, first, last);
	
	if (result.first == last) {
	  const logprob_type __logbound = (result.second < logbounds[shard_index].size()
					   ? logbounds[shard_index](result.second, order)
					   : logprobs[shard_index](result.second, order));
	  if(__logbound != logprob_min())
	    return __logbound;
	  else {
	    const size_type parent = index[shard_index].parent(result.second);
	    const logprob_type logbackoff = (parent != size_type(-1)
					     ? backoffs[shard_index_backoff](parent, order - 1)
					     : logprob_type(0.0));
	    return logprob(first + 1, last, smooth_smallest) + logbackoff;
	  }
	} else {
	  const logprob_type logbackoff = (result.first == last - 1
					   ? backoffs[shard_index_backoff](result.second, order - 1)
					   : logprob_type(0.0));
	  return logprob(first + 1, last, smooth_smallest) + logbackoff; 
	}
      } else {
	const size_type shard_index = index.shard_index(first, last);
	std::pair<Iterator, size_type> result = index.traverse(shard_index, first, last);
	
	if (result.first == last) {
	  const logprob_type __logbound = (result.second < logbounds[shard_index].size()
					   ? logbounds[shard_index](result.second, order)
					   : logprobs[shard_index](result.second, order));
	  return (__logbound != logprob_min()
		  ? __logbound
		  : (index.is_bos(*first)
		     ? logprob_bos()
		     : (smooth_smallest ? logprob_min() : smooth)));
	} else
	  return (smooth_smallest ? logprob_min() : smooth);
      }
    }
    
    template <typename Iterator>
    logprob_type operator()(Iterator first, Iterator last, bool smooth_smallest=false) const
    {
      return logprob(first, last, smooth_smallest);
    }
    
    template <typename Iterator>
    logprob_type logprob(Iterator first, Iterator last, bool smooth_smallest=false) const
    {
#if 0
      typedef typename std::iterator_traits<Iterator>::value_type value_type;

      return __logprob_dispatch(first, last, value_type(), smooth_smallest);
#endif

      if (first == last) return 0.0;
      
      first = std::max(first, last - index.order());
      
      int       shard_prev = -1;
      size_type node_prev = size_type(-1);
      
      logprob_type logbackoff = 0.0;
      for (/**/; first != last - 1; ++ first) {
	const int order = last - first;
	const size_type shard_index = index.shard_index(first, last);
	const size_type shard_index_backoff = size_type((order == 2) - 1) & shard_index;
	
	std::pair<Iterator, size_type> result = index.traverse(shard_index, first, last, shard_prev, node_prev);
#if 0
	std::pair<Iterator, size_type> result2 = index.traverse(shard_index, first, last);
#endif

	shard_prev = -1;
	node_prev = size_type(-1);
	
	if (result.first == last) {
#if 0
	  // testing...
	  if (result2.first != result.first)
	    std::cerr << "no iterator match???" << std::endl;
	  if (result2.second != result.second)
	    std::cerr << "no node match???" << std::endl;
#endif
	  
	  const logprob_type __logprob = logprobs[shard_index](result.second, order);
	  if (__logprob != logprob_min())
	    return logbackoff + __logprob;
	  else {
	    const size_type parent = index[shard_index].parent(result.second);
	    if (parent != size_type(-1)) {
	      logbackoff += backoffs[shard_index_backoff](parent, order - 1);
	      
	      shard_prev = shard_index;
	      node_prev = parent;
	    }
	  }
	} else if (result.first == last - 1) {
#if 0
	  if (result2.first != result.first)
	    std::cerr << "no backoff iterator match???" << std::endl;
	  if (result2.second != result.second)
	    std::cerr << "no backoff node match???" << std::endl;
#endif
	  
	  logbackoff += backoffs[shard_index_backoff](result.second, order - 1);
	  
	  shard_prev = shard_index;
	  node_prev = result.second;
	} else {
#if 0
	  if (result2.first == last || result2.first == last - 1)
	    std::cerr << "no match..." << std::endl;
#endif
	}
      }
      
      const int order = last - first;
      const size_type shard_index = index.shard_index(first, last);
      std::pair<Iterator, size_type> result = index.traverse(shard_index, first, last);
      
      return (result.first == last && logprobs[shard_index](result.second, order) != logprob_min()
	      ? logbackoff + logprobs[shard_index](result.second, order)
	      : (index.is_bos(*first)
		 ? logprob_bos()
		 : (smooth_smallest ? logprob_min() : logbackoff + smooth)));
    }

  private:
    template <typename Iterator, typename _Word>
    logprob_type __logprob_dispatch(Iterator first, Iterator last, _Word __word, bool smooth_smallest=false) const
    {
      if (first == last) return 0.0;
      
    }

    template <typename Iterator>
    logprob_type __logprob_dispatch(Iterator first, Iterator last, const id_type __word, bool smooth_smallest=false) const
    {
      if (first == last) return 0.0;
      
      const int order = std::distance(first, last);

      if (order == 1) {
	const state_type state = index.next(state_type(), *(last - 1));
	
	if (state.is_root_node()) {
	  // not found...!
	  
	} else {
	  // found!...
	  
	}
	
      } else {
	logprob_type backoff = 0.0;
	
      }
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
    void write_prepare(const path_type& path) const;
    void write_shard(const path_type& path, int shard) const;
    
    void close() { clear(); }
    void clear()
    {
      index.clear();
      logprobs.clear();
      backoffs.clear();
      logbounds.clear();
      smooth = utils::mathop::log(1e-7);
    }

    void populate()
    {
      index.populate();
      
      populate(logprobs.begin(), logprobs.end());
      populate(backoffs.begin(), backoffs.end());
      populate(logbounds.begin(), logbounds.end());
    }

    template <typename Iterator>
    void populate(Iterator first, Iterator last)
    {
      for (/**/; first != last; ++ first)
	first->populate();
    }
    
    void quantize();
    void bounds();
    
    bool is_open() const { return index.is_open(); }
    bool has_bounds() const { return ! logbounds.empty(); }
    
    stat_type stat_index() const { return index.stat_index(); }
    stat_type stat_pointer() const { return index.stat_pointer(); }
    stat_type stat_vocab() const { return index.stat_vocab(); }

  private:
    template <typename Iterator>
    stat_type __stat_aux(Iterator first, Iterator last) const
    {
      stat_type stat;
      for (/**/; first != last; ++ first)
	stat += first->stat();
      return stat;
    }
  public:
    stat_type stat_logprob() const
    {
      return __stat_aux(logprobs.begin(), logprobs.end());
    }
    stat_type stat_backoff() const
    {
      return __stat_aux(backoffs.begin(), backoffs.end());
    }
    stat_type stat_logbound() const
    {
      return __stat_aux(logbounds.begin(), logbounds.end());
    }
    
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
