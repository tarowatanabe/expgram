//
//  Copyright(C) 2009-2013 Taro Watanabe <taro.watanabe@nict.go.jp>
//

#include <sstream>
#include <queue>
#include <deque>

#include "NGramCounts.hpp"
#include "NGramCountsIndexer.hpp"
#include "Discount.hpp"

#include <boost/tokenizer.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>
#include <boost/math/special_functions/expm1.hpp>

#include "utils/lockfree_queue.hpp"
#include "utils/lockfree_list_queue.hpp"
#include "utils/compress_stream.hpp"
#include "utils/space_separator.hpp"
#include "utils/tempfile.hpp"
#include "utils/vector2.hpp"
#include "utils/lexical_cast.hpp"

namespace expgram
{
  template <typename Path, typename Data>
  inline
  void dump_file(const Path& file, const Data& data)
  {
    boost::iostreams::filtering_ostream os;
    os.push(boost::iostreams::file_sink(file.string(), std::ios_base::out | std::ios_base::trunc), 1024 * 1024);
    os.exceptions(std::ostream::eofbit | std::ostream::failbit | std::ostream::badbit);
    
    const int64_t file_size = sizeof(typename Data::value_type) * data.size();
    for (int64_t offset = 0; offset < file_size; offset += 1024 * 1024)
      os.write(((char*) &(*data.begin())) + offset, std::min(int64_t(1024 * 1024), file_size - offset));
  }  
  
  
  struct NGramCountsModifyMapReduce
  {
    typedef expgram::NGramCounts ngram_type;
    
    typedef ngram_type::size_type       size_type;
    typedef ngram_type::difference_type difference_type;
    
    typedef ngram_type::count_type      count_type;
    typedef ngram_type::word_type       word_type;
    typedef ngram_type::id_type         id_type;
    typedef ngram_type::vocab_type      vocab_type;
    typedef ngram_type::path_type       path_type;
    
    typedef std::vector<id_type, std::allocator<id_type> > context_type;
    typedef std::pair<context_type, count_type>            context_count_type;
    
    typedef utils::lockfree_list_queue<context_count_type, std::allocator<context_count_type> > queue_type;
    typedef boost::shared_ptr<queue_type>                                                  queue_ptr_type;
    typedef std::vector<queue_ptr_type, std::allocator<queue_ptr_type> >                   queue_ptr_set_type;
    
    typedef boost::thread                                                  thread_type;
    typedef boost::shared_ptr<thread_type>                                 thread_ptr_type;
    typedef std::vector<thread_ptr_type, std::allocator<thread_ptr_type> > thread_ptr_set_type;
    
  };
  
  inline
  void swap(NGramCountsModifyMapReduce::context_count_type& x,
	    NGramCountsModifyMapReduce::context_count_type& y)
  {
    x.first.swap(y.first);
    std::swap(x.second, y.second);
  }

  struct NGramCountsModifyMapper
  {
    typedef NGramCountsModifyMapReduce map_reduce_type;
    
    typedef map_reduce_type::ngram_type ngram_type;

    typedef map_reduce_type::id_type    id_type;
    typedef map_reduce_type::size_type  size_type;
    typedef map_reduce_type::count_type count_type;
    typedef map_reduce_type::vocab_type vocab_type;

    typedef map_reduce_type::context_type       context_type;
    typedef map_reduce_type::context_count_type context_count_type;
    
    typedef map_reduce_type::queue_ptr_set_type queue_ptr_set_type;

    
    const ngram_type&   ngram;
    queue_ptr_set_type& queues;
    int                 shard;
    int                 debug;
    
    NGramCountsModifyMapper(const ngram_type&   _ngram,
			    queue_ptr_set_type& _queues,
			    const int           _shard,
			    const int           _debug)
      : ngram(_ngram),
	queues(_queues),
	shard(_shard),
	debug(_debug)
    {}
    
    void operator()()
    {
      typedef std::vector<count_type, std::allocator<count_type> > count_set_type;
      
      const id_type bos_id = ngram.index.vocab()[vocab_type::BOS];
      const int max_order = ngram.index.order();
      
      count_set_type unigrams(ngram.index[shard].offsets[1], 0);
      context_type context;
      
      for (int order_prev = 1; order_prev < max_order; ++ order_prev) {
	
	if (debug)
	  std::cerr << "modify counts: shard: " << shard << " order: " << (order_prev + 1) << std::endl;

	const size_type pos_context_first = ngram.index[shard].offsets[order_prev - 1];
	const size_type pos_context_last  = ngram.index[shard].offsets[order_prev];
	
	context.resize(order_prev + 1);
	
	size_type pos_last_prev = pos_context_last;
	for (size_type pos_context = pos_context_first; pos_context < pos_context_last; ++ pos_context) {
	  const size_type pos_first = pos_last_prev;
	  const size_type pos_last = ngram.index[shard].children_last(pos_context);
	  pos_last_prev = pos_last;
	  
	  if (pos_first == pos_last) continue;
	  
	  context_type::iterator citer_curr = context.end() - 2;
	  for (size_type pos_curr = pos_context; pos_curr != size_type(-1); pos_curr = ngram.index[shard].parent(pos_curr), -- citer_curr)
	    *citer_curr = ngram.index[shard][pos_curr];
	  
	  // BOS handling...
	  if (shard == 0 && order_prev == 1 && context.front() == bos_id)
	    unigrams[context.front()] += ngram.counts[shard][pos_context];
	  
	  for (size_type pos = pos_first; pos != pos_last; ++ pos) {
	    context.back() = ngram.index[shard][pos];
	    
	    if (context.size() == 2)
	      ++ unigrams[context.back()];
	    else
	      queues[ngram.index.shard_index(context.begin() + 1, context.end())]->push(std::make_pair(context_type(context.begin() + 1, context.end()),
												       count_type(1)));
	    
	    if (context.front() == bos_id && order_prev + 1 != max_order)
	      queues[ngram.index.shard_index(context.begin(), context.end())]->push(std::make_pair(context, ngram.counts[shard][pos]));
	  }
	}
      }
      
      for (id_type id = 0; id < unigrams.size(); ++ id)
	if (unigrams[id])
	  queues[0]->push(std::make_pair(context_type(1, id), unigrams[id]));
      
      for (size_type shard = 0; shard != queues.size(); ++ shard)
	queues[shard]->push(std::make_pair(context_type(), 0.0));
    }
  };
  
  struct NGramCountsModifyReducer
  {
    typedef NGramCountsModifyMapReduce map_reduce_type;

    typedef map_reduce_type::ngram_type ngram_type;
    
    typedef map_reduce_type::id_type    id_type;
    typedef map_reduce_type::size_type  size_type;
    typedef map_reduce_type::count_type count_type;
    typedef map_reduce_type::path_type  path_type;

    typedef map_reduce_type::context_type       context_type;
    typedef map_reduce_type::context_count_type context_count_type;
    
    typedef map_reduce_type::queue_type queue_type;

    ngram_type& ngram;
    queue_type& queue;
    int         shard;
    int         debug;
    
    NGramCountsModifyReducer(ngram_type& _ngram,
			     queue_type& _queue,
			     const int   _shard,
			     const int   _debug)
      : ngram(_ngram),
	queue(_queue),
	shard(_shard),
	debug(_debug)
    {}
    
    template <typename Iterator>
    void dump(std::ostream& os, Iterator first, Iterator last)
    {
      typedef typename std::iterator_traits<Iterator>::value_type value_type;
      
      while (first != last) {
	const size_type write_size = std::min(size_type(1024 * 1024), size_type(last - first));
	os.write((char*) &(*first), write_size * sizeof(value_type));
	first += write_size;
      }
    }

    void operator()()
    {
      typedef std::vector<count_type, std::allocator<count_type> > count_set_type;
      
      const size_type offset = ngram.counts[shard].offset;
      count_set_type counts_modified(ngram.index[shard].position_size() - offset, count_type(0));
      
      context_count_type context_count;
      size_type num_empty = 0;
      
      while (num_empty < ngram.index.size()) {
	queue.pop_swap(context_count);
	
	if (context_count.first.empty()) {
	  ++ num_empty;
	  continue;
	}
	
	const context_type& context = context_count.first;
	
	std::pair<context_type::const_iterator, size_type> result = ngram.index.traverse(shard, context.begin(), context.end());
	if (result.first != context.end() || result.second == size_type(-1)) {

	  if (debug >= 2) {
	    std::cerr << "WARNING: no ngram for:";
	    context_type::const_iterator citer_end = context.end();
	    for (context_type::const_iterator citer = context.begin(); citer != citer_end; ++ citer)
	      std::cerr << ' ' << ngram.index.vocab()[*citer];
	    std::cerr << std::endl;
	  }
	  continue;
	}
	
	counts_modified[result.second - offset] += context_count.second;
      }
      
      const path_type path = utils::tempfile::directory_name(utils::tempfile::tmp_dir() / "expgram.modified.XXXXXX");
      utils::tempfile::insert(path);
      
      boost::iostreams::filtering_ostream os;
      os.push(utils::packed_sink<count_type, std::allocator<count_type> >(path));
      dump(os, counts_modified.begin(), counts_modified.end());
      
      // dump the last order...
      for (size_type pos = ngram.index[shard].position_size(); pos < ngram.counts[shard].size(); ++ pos) {
	const count_type count = ngram.counts[shard][pos];
	os.write((char*) &count, sizeof(count_type));
      }
      os.pop();
      
      ::sync();
      while (! ngram_type::shard_data_type::count_set_type::exists(path))
	boost::thread::yield();
      
      utils::tempfile::permission(path);
      
      // set up offset and counts!
      ngram.types[shard].offset = ngram.counts[shard].offset;
      ngram.types[shard].counts.open(path);
    }
  };
  
  void NGramCounts::modify()
  {
    typedef NGramCountsModifyMapReduce map_reduce_type;
    typedef NGramCountsModifyMapper    mapper_type;
    typedef NGramCountsModifyReducer   reducer_type;
    
    typedef map_reduce_type::thread_type         thread_type;
    typedef map_reduce_type::thread_ptr_set_type thread_ptr_set_type;
    
    typedef map_reduce_type::queue_type          queue_type;
    typedef map_reduce_type::queue_ptr_set_type  queue_ptr_set_type;
    
    // if we already have modified counts, then,  no modification...
    
    if (counts.empty())
      throw std::runtime_error("no indexed counts?");
    
    // already modified?
    if (is_modified())
      return;
    
    // allocate types...
    types.reserve(index.size());
    types.resize(index.size());
    
    queue_ptr_set_type  queues(index.size());
    thread_ptr_set_type threads_mapper(index.size());
    thread_ptr_set_type threads_reducer(index.size());

    // first, run reducer...
    for (size_type shard = 0; shard != counts.size(); ++ shard) {
      queues[shard].reset(new queue_type(1024 * 64));
      threads_reducer[shard].reset(new thread_type(reducer_type(*this, *queues[shard], shard, debug)));
    }
    
    // second, mapper...
    for (size_type shard = 0; shard != counts.size(); ++ shard)
      threads_mapper[shard].reset(new thread_type(mapper_type(*this, queues, shard, debug)));
    
    // termination...
    for (size_type shard = 0; shard != counts.size(); ++ shard)
      threads_mapper[shard]->join();
    for (size_type shard = 0; shard != counts.size(); ++ shard)
      threads_reducer[shard]->join();
  }
  

  struct NGramCountsEstimateMapReduce
  {
    typedef expgram::NGramCounts ngram_type;
    
    typedef ngram_type::size_type       size_type;
    typedef ngram_type::difference_type difference_type;
    
    typedef ngram_type::word_type       word_type;
    typedef ngram_type::id_type         id_type;
    typedef ngram_type::vocab_type      vocab_type;
    
    typedef ngram_type::count_type      count_type;
    typedef ngram_type::prob_type       prob_type;
    typedef ngram_type::logprob_type   logprob_type;

    typedef Discount                                                   discount_type;
    typedef std::vector<discount_type, std::allocator<discount_type> > discount_set_type;
    
    typedef std::vector<logprob_type, std::allocator<logprob_type> > logprob_shard_type;
    typedef std::vector<logprob_type, std::allocator<logprob_type> > logbound_shard_type;
    typedef std::vector<logprob_type, std::allocator<logprob_type> > backoff_shard_type;
    
    typedef std::vector<logprob_shard_type, std::allocator<logprob_shard_type> >   logprob_shard_set_type;
    typedef std::vector<logbound_shard_type, std::allocator<logbound_shard_type> > logbound_shard_set_type;
    typedef std::vector<backoff_shard_type, std::allocator<backoff_shard_type> >   backoff_shard_set_type;
    
    typedef std::vector<size_type, std::allocator<size_type> > offset_set_type;
    
    typedef std::map<count_type, count_type, std::less<count_type>, std::allocator<std::pair<const count_type, count_type> > > count_set_type;
    typedef std::vector<count_set_type, std::allocator<count_set_type> > count_map_type;
    
    typedef boost::thread                                                  thread_type;
    typedef boost::shared_ptr<thread_type>                                 thread_ptr_type;
    typedef std::vector<thread_ptr_type, std::allocator<thread_ptr_type> > thread_ptr_set_type;
  };

  struct NGramCountsEstimateDiscountMapper
  {
    typedef NGramCountsEstimateMapReduce map_reduce_type;
    
    typedef map_reduce_type::ngram_type     ngram_type;
    typedef map_reduce_type::count_type     count_type;
    typedef map_reduce_type::size_type      size_type;
    typedef map_reduce_type::id_type        id_type;
    typedef map_reduce_type::vocab_type     vocab_type;
    typedef map_reduce_type::count_map_type count_map_type;
    
    const ngram_type& ngram;
    count_map_type& cc_types;
    count_map_type& cc_counts;
    int shard;
    bool remove_unk;
    
    NGramCountsEstimateDiscountMapper(const ngram_type& _ngram,
				      count_map_type&   _cc_types,
				      count_map_type&   _cc_counts,
				      const int         _shard,
				      const bool        _remove_unk)
      : ngram(_ngram),
	cc_types(_cc_types),
	cc_counts(_cc_counts),
	shard(_shard),
	remove_unk(_remove_unk) {}
    
    void operator()()
    {
      //const id_type bos_id = ngram.index.vocab()[vocab_type::BOS];
      const id_type unk_id = ngram.index.vocab()[vocab_type::UNK];

      cc_types.clear();
      cc_types.reserve(ngram.index.order() + 1);
      cc_types.resize(ngram.index.order() + 1);
      
      cc_counts.clear();
      cc_counts.reserve(ngram.index.order() + 1);
      cc_counts.resize(ngram.index.order() + 1);
      
      for (int order = (shard == 0 ? 1 : 2); order <= ngram.index.order(); ++ order) {
	const size_type pos_first = ngram.index[shard].offsets[order - 1];
	const size_type pos_last  = ngram.index[shard].offsets[order];
	
	// count-of-count for type-counts
	for (size_type pos = pos_first; pos != pos_last; ++ pos) {
	  const count_type count = ngram.types[shard][pos];
	  if (count && (order == 1 || ! remove_unk || ngram.index[shard][pos] != unk_id))
	    ++ cc_types[order][count];
	}
	
	// count-of-count for raw-counts
	for (size_type pos = pos_first; pos != pos_last; ++ pos) {
	  const count_type count = ngram.counts[shard][pos];
	  if (count && (order == 1 || ! remove_unk || ngram.index[shard][pos] != unk_id))
	    ++ cc_counts[order][count];
	}
      }
    }
  };
  
  struct NGramCountsEstimateBigramMapper
  {
    typedef NGramCountsEstimateMapReduce map_reduce_type;
    
    typedef map_reduce_type::ngram_type ngram_type;

    typedef map_reduce_type::id_type      id_type;
    typedef map_reduce_type::size_type    size_type;
    typedef map_reduce_type::vocab_type   vocab_type;
    
    typedef map_reduce_type::count_type   count_type;
    typedef map_reduce_type::prob_type    prob_type;
    typedef map_reduce_type::logprob_type logprob_type;
    
    typedef map_reduce_type::discount_set_type       discount_set_type;
    typedef map_reduce_type::logprob_shard_set_type  logprob_shard_set_type;
    typedef map_reduce_type::logbound_shard_set_type logbound_shard_set_type;
    typedef map_reduce_type::backoff_shard_set_type  backoff_shard_set_type;
    
    const ngram_type&        ngram;
    const discount_set_type& discounts;
    const discount_set_type& discounts_raw;
    logprob_shard_set_type&  logprobs;
    logbound_shard_set_type& logbounds;
    backoff_shard_set_type&  backoffs;
    int                      shard;
    bool                     remove_unk;
    int                      debug;
    
    NGramCountsEstimateBigramMapper(const ngram_type&        _ngram,
				    const discount_set_type& _discounts,
				    const discount_set_type& _discounts_raw,
				    logprob_shard_set_type&  _logprobs,
				    logbound_shard_set_type& _logbounds,
				    backoff_shard_set_type&  _backoffs,
				    const int                _shard,
				    const bool               _remove_unk,
				    const int                _debug)
      : ngram(_ngram),
	discounts(_discounts),
	discounts_raw(_discounts_raw),
	logprobs(_logprobs),
	logbounds(_logbounds),
	backoffs(_backoffs),
	shard(_shard),
	remove_unk(_remove_unk),
	debug(_debug) {}
    
    void operator()()
    {
      const int order = 2;
      const int order_prev = 1;
      
      const size_type pos_context_first = ngram.index[0].offsets[order_prev - 1];
      const size_type pos_context_last  = ngram.index[0].offsets[order_prev];

      //const id_type bos_id = ngram.index.vocab()[vocab_type::BOS];
      const id_type unk_id = ngram.index.vocab()[vocab_type::UNK];
      
      //size_type pos_last_prev = pos_context_last;
      for (size_type pos_context = pos_context_first; pos_context < pos_context_last; ++ pos_context)
	if (static_cast<int>(id_type(pos_context) % ngram.index.size()) == shard) {
	  
	  if (ngram.index.order() > 2) {
	    // estiamte logbounds...
	    count_type total = 0;
	    count_type observed = 0;
	    count_type min2 = 0;
	    count_type min3 = 0;
	    
	    for (size_type shard = 0; shard != ngram.index.size(); ++ shard) {
	      const size_type pos_first = ngram.index[shard].children_first(pos_context);
	      const size_type pos_last  = ngram.index[shard].children_last(pos_context);
	    
	      for (size_type pos = pos_first; pos != pos_last; ++ pos) {
		if (remove_unk && ngram.index[shard][pos] == unk_id) continue;
		
		const count_type count = ngram.counts[shard][pos];
		
		if (count == 0) continue;
		
		total += count;
		++ observed;
		min2 += (count >= discounts_raw[order].mincount2);
		min3 += (count >= discounts_raw[order].mincount3);
	      }
	    }
	    
	    if (total > 0) {
	      for (size_type shard = 0; shard != ngram.index.size(); ++ shard) {
		const size_type pos_first = ngram.index[shard].children_first(pos_context);
		const size_type pos_last  = ngram.index[shard].children_last(pos_context);

		const size_type offset = ngram.counts[shard].offset;
	      
		for (size_type pos = pos_first; pos != pos_last; ++ pos) {
		  const id_type    id = ngram.index[shard][pos];
		  
		  if (remove_unk && id == unk_id) continue;
		  
		  const count_type count = ngram.counts[shard][pos];

		  if (count == 0) continue;
		
		  const prob_type discount = discounts_raw[order].discount(count, total, observed);
		  const prob_type prob = (discount * count / total);
		
		  const prob_type lower_order_weight = discounts_raw[order].lower_order_weight(total, observed, min2, min3);
		  const logprob_type lower_order_logprob = logprobs[0][id];
		  const prob_type lower_order_prob = utils::mathop::exp(double(lower_order_logprob));
		  const prob_type prob_combined = prob + lower_order_weight * lower_order_prob;
		  const logprob_type logprob = utils::mathop::log(prob_combined);
		  
		  logbounds[shard][pos - offset] = logprob;
		}
	      }
	    }
	  }
	  
	  count_type total = 0;
	  count_type observed = 0;
	  count_type min2 = 0;
	  count_type min3 = 0;
	  
	  double logsum_lower_order = boost::numeric::bounds<double>::lowest();
	  
	  for (size_type shard = 0; shard != ngram.index.size(); ++ shard) {
	    const size_type pos_first = ngram.index[shard].children_first(pos_context);
	    const size_type pos_last  = ngram.index[shard].children_last(pos_context);
	    
	    for (size_type pos = pos_first; pos != pos_last; ++ pos) {
	      const id_type    id = ngram.index[shard][pos];
	      
	      // simply treat it as a special zero event
	      if (remove_unk && id == unk_id) continue;
	      
	      const count_type count = ngram.types[shard][pos];

	      if (count == 0) continue;
	      
	      total += count;
	      ++ observed;
	      min2 += (count >= discounts[order].mincount2);
	      min3 += (count >= discounts[order].mincount3);
	      
	      logsum_lower_order = utils::mathop::logsum(logsum_lower_order, double(logprobs[0][id]));
	    }
	  }
	  
	  if (total == 0) continue;
	  
	  double logsum = 0.0;
	  for (/**/; logsum >= 0.0; ++ total) {
	    logsum = boost::numeric::bounds<double>::lowest();
	    
	    for (size_type shard = 0; shard != ngram.index.size(); ++ shard) {
	      const size_type pos_first = ngram.index[shard].children_first(pos_context);
	      const size_type pos_last  = ngram.index[shard].children_last(pos_context);

	      const size_type offset = ngram.types[shard].offset;
	      
	      for (size_type pos = pos_first; pos != pos_last; ++ pos) {
		const id_type    id = ngram.index[shard][pos];
		
		if (remove_unk && id == unk_id) continue;
		
		const count_type count = ngram.types[shard][pos];
		
		if (count == 0) continue;
		
		const prob_type discount = discounts[order].discount(count, total, observed);
		const prob_type prob = (discount * count / total);
		
		const prob_type lower_order_weight = discounts[order].lower_order_weight(total, observed, min2, min3);
		const logprob_type lower_order_logprob = logprobs[0][id];
		const prob_type lower_order_prob = utils::mathop::exp(double(lower_order_logprob));
		const prob_type prob_combined = prob + lower_order_weight * lower_order_prob;
		const logprob_type logprob = utils::mathop::log(prob_combined);
		
		logsum = utils::mathop::logsum(logsum, double(logprob));
		
		logprobs[shard][pos - offset] = logprob;
	      }
	    }
	  }

	  using namespace boost::math::policies;
	  typedef policy<domain_error<errno_on_error>,
	    pole_error<errno_on_error>,
	    overflow_error<errno_on_error>,
	    rounding_error<errno_on_error>,
	    evaluation_error<errno_on_error>
	    > policy_type;
	  
	  //const double numerator = 1.0 - utils::mathop::exp(logsum);
	  //const double denominator = 1.0 - utils::mathop::exp(logsum_lower_order);
	  
	  const double numerator = - boost::math::expm1(logsum, policy_type());
	  const double denominator =  - boost::math::expm1(logsum_lower_order, policy_type());
	  
	  if (numerator > 0.0) {
	    if (denominator > 0.0)
	      backoffs[0][pos_context] = utils::mathop::log(numerator) - utils::mathop::log(denominator);
	    else {
	      for (size_type shard = 0; shard != ngram.index.size(); ++ shard) {
		const size_type pos_first = ngram.index[shard].children_first(pos_context);
		const size_type pos_last  = ngram.index[shard].children_last(pos_context);

		const size_type offset = ngram.types[shard].offset;
		
		for (size_type pos = pos_first; pos != pos_last; ++ pos)
		  if (ngram.types[shard][pos] && ((! remove_unk) || (ngram.index[shard][pos] != unk_id)))
		    logprobs[shard][pos - offset] -= logsum;
	      }
	    }
	  }
	}
    }
  };
  
  struct NGramCountsEstimateMapper
  {
    typedef NGramCountsEstimateMapReduce map_reduce_type;
    
    typedef map_reduce_type::ngram_type ngram_type;

    typedef map_reduce_type::id_type      id_type;
    typedef map_reduce_type::size_type    size_type;
    typedef map_reduce_type::vocab_type   vocab_type;
    
    typedef map_reduce_type::count_type   count_type;
    typedef map_reduce_type::prob_type    prob_type;
    typedef map_reduce_type::logprob_type logprob_type;
    
    typedef map_reduce_type::discount_set_type       discount_set_type;
    typedef map_reduce_type::logprob_shard_set_type  logprob_shard_set_type;
    typedef map_reduce_type::logbound_shard_set_type logbound_shard_set_type;
    typedef map_reduce_type::backoff_shard_set_type  backoff_shard_set_type;
    typedef map_reduce_type::offset_set_type         offset_set_type;

    const ngram_type&        ngram;
    const discount_set_type& discounts;
    const discount_set_type& discounts_raw;
    logprob_shard_set_type&  logprobs;
    logbound_shard_set_type& logbounds;
    backoff_shard_set_type&  backoffs;
    offset_set_type&         offsets;
    int                      shard;
    bool                     remove_unk;
    int                      debug;
    
    NGramCountsEstimateMapper(const ngram_type&        _ngram,
			      const discount_set_type& _discounts,
			      const discount_set_type& _discounts_raw,
			      logprob_shard_set_type&  _logprobs,
			      logbound_shard_set_type& _logbounds,
			      backoff_shard_set_type&  _backoffs,
			      offset_set_type&         _offsets,
			      const int                _shard,
			      const bool               _remove_unk,
			      const int                _debug)
      : ngram(_ngram),
	discounts(_discounts),
	discounts_raw(_discounts_raw),
        logprobs(_logprobs),
        logbounds(_logbounds),
	backoffs(_backoffs),
	offsets(_offsets),
	shard(_shard),
	remove_unk(_remove_unk),
	debug(_debug) {}
    
    template <typename Iterator>
    logprob_type logprob_backoff(Iterator first, Iterator last) const
    {
      logprob_type logbackoff = 0.0;
      for (/**/; first != last - 1; ++ first) {
	const int order = last - first;
	const size_type shard_index = ngram.index.shard_index(first, last);
	const size_type offset = ngram.types[shard_index].offset;
	
	const size_type shard_index_backoff = size_type((order == 2) - 1) & shard_index;
	const size_type offset_backoff = size_type((order == 2) - 1) & offset;
	
	std::pair<Iterator, size_type> result = ngram.index.traverse(shard_index, first, last);
	
	if (result.first == last) {
	  while (utils::atomicop::fetch_and_add(offsets[shard_index], size_type(0)) <= result.second)
	    boost::thread::yield();
	  
	  if (logprobs[shard_index][result.second - offset] != expgram::NGram::logprob_min())
	    return logbackoff + logprobs[shard_index][result.second - offset];
	  else {
	    const size_type parent = ngram.index[shard_index].parent(result.second);
	    if (parent != size_type(-1))
	      logbackoff += backoffs[shard_index_backoff][parent - offset_backoff];
	  }
	} else if (result.first == last - 1)
	  logbackoff += backoffs[shard_index_backoff][result.second - offset_backoff];
      }
      
      // unigram...
      return logbackoff + logprobs[0][*first];
    }

    void operator()()
    {
      typedef std::vector<id_type, std::allocator<id_type> >           context_type;
      typedef std::vector<logprob_type, std::allocator<logprob_type> > logprob_set_type;

      const size_type offset = ngram.types[shard].offset;

      //const id_type bos_id = ngram.index.vocab()[vocab_type::BOS];
      const id_type unk_id = ngram.index.vocab()[vocab_type::UNK];

      context_type     context;
      logprob_set_type lowers;

      for (int order_prev = 2; order_prev < ngram.index.order(); ++ order_prev) {
	const int order = order_prev + 1;

	if (debug)
	  std::cerr << "order: " << order << " shard: " << shard << std::endl;
	
	const size_type pos_context_first = ngram.index[shard].offsets[order_prev - 1];
	const size_type pos_context_last  = ngram.index[shard].offsets[order_prev];
	
	context.resize(order);
	
	size_type pos_last_prev = pos_context_last;
	for (size_type pos_context = pos_context_first; pos_context < pos_context_last; ++ pos_context) {
	  const size_type pos_first = pos_last_prev;
	  const size_type pos_last = ngram.index[shard].children_last(pos_context);
	  pos_last_prev = pos_last;
	  
	  // update offsets
	  offsets[shard] = pos_first;
	  
	  if (pos_first == pos_last) continue;
	  
	  context_type::iterator citer_curr = context.end() - 2;
	  for (size_type pos_curr = pos_context; pos_curr != size_type(-1); pos_curr = ngram.index[shard].parent(pos_curr), -- citer_curr)
	    *citer_curr = ngram.index[shard][pos_curr];
	  
	  
	  lowers.resize(pos_last - pos_first);
	  
	  // first, compute lower-order ngram probabilities...
	  logprob_set_type::iterator liter = lowers.begin();
	  for (size_type pos = pos_first; pos != pos_last; ++ pos, ++ liter) {
	    const id_type id = ngram.index[shard][pos];
	    
	    if (remove_unk && id == unk_id) continue;
	    
	    context.back() = id;
	    
	    *liter = logprob_backoff(context.begin() + 1, context.end());
	  }
	  
	  // estimate for logbound...
	  if (ngram.index.order() > order) {
	    count_type total = 0;
	    count_type observed = 0;
	    count_type min2 = 0;
	    count_type min3 = 0;
	    
	    for (size_type pos = pos_first; pos != pos_last; ++ pos) {
	      if (remove_unk && ngram.index[shard][pos] == unk_id) continue;
	      
	      const count_type count = ngram.counts[shard][pos];
	      
	      if (count == 0) continue;
	      
	      total += count;
	      ++ observed;
	      min2 += (count >= discounts_raw[order].mincount2);
	      min3 += (count >= discounts_raw[order].mincount3);
	    }

	    if (total > 0) {
	      logprob_set_type::const_iterator liter = lowers.begin();
	      
	      for (size_type pos = pos_first; pos != pos_last; ++ pos, ++ liter) {
		if (remove_unk && ngram.index[shard][pos] == unk_id) continue;
		
		const count_type count = ngram.counts[shard][pos];
		
		if (count == 0) continue;
		
		const prob_type discount = discounts_raw[order].discount(count, total, observed);
		const prob_type prob = (discount * count / total);
		
		const prob_type lower_order_weight = discounts_raw[order].lower_order_weight(total, observed, min2, min3);
		const logprob_type lower_order_logprob = *liter;
		const prob_type lower_order_prob = utils::mathop::exp(double(lower_order_logprob));
		const prob_type prob_combined = prob + lower_order_weight * lower_order_prob;
		const logprob_type logprob = utils::mathop::log(prob_combined);
		
		logbounds[shard][pos - offset] = logprob;
	      }
	    }
	  }
	  
	  count_type total = 0;
	  count_type observed = 0;
	  count_type min2 = 0;
	  count_type min3 = 0;
	  
	  double logsum_lower_order = boost::numeric::bounds<double>::lowest();
	  
	  {
	    logprob_set_type::const_iterator liter = lowers.begin();
	    for (size_type pos = pos_first; pos != pos_last; ++ pos, ++ liter) {
	      if (remove_unk && ngram.index[shard][pos] == unk_id) continue;
	      
	      const count_type count = ngram.types[shard][pos];
	      
	      if (count == 0) continue;
	      
	      total += count;
	      ++ observed;
	      min2 += (count >= discounts[order].mincount2);
	      min3 += (count >= discounts[order].mincount3);
	      
	      logsum_lower_order = utils::mathop::logsum(logsum_lower_order, double(*liter));
	    }
	  }
	  
	  if (total == 0) continue;
	  
	  double logsum = 0.0;
	  for (/**/; logsum >= 0.0; ++ total) {
	    logsum = boost::numeric::bounds<double>::lowest();
	    
	    logprob_set_type::const_iterator liter = lowers.begin();
	    for (size_type pos = pos_first; pos != pos_last; ++ pos, ++ liter) {
	      if (remove_unk && ngram.index[shard][pos] == unk_id) continue;
	      
	      const count_type count = ngram.types[shard][pos];

	      if (count == 0) continue;
	      
	      const prob_type discount = discounts[order].discount(count, total, observed);
	      const prob_type prob = (discount * count / total);
	      
	      const prob_type lower_order_weight = discounts[order].lower_order_weight(total, observed, min2, min3);
	      const logprob_type lower_order_logprob = *liter;
	      const prob_type lower_order_prob = utils::mathop::exp(double(lower_order_logprob));
	      const prob_type prob_combined = prob + lower_order_weight * lower_order_prob;
	      const logprob_type logprob = utils::mathop::log(prob_combined);
	      
	      logsum = utils::mathop::logsum(logsum, double(logprob));
	      logprobs[shard][pos - offset] = logprob;
	    }
	  }

	  using namespace boost::math::policies;
	  typedef policy<domain_error<errno_on_error>,
	    pole_error<errno_on_error>,
	    overflow_error<errno_on_error>,
	    rounding_error<errno_on_error>,
	    evaluation_error<errno_on_error>
	    > policy_type;
	  
	  //const double numerator = 1.0 - utils::mathop::exp(logsum);
	  //const double denominator = 1.0 - utils::mathop::exp(logsum_lower_order);
	  
	  const double numerator = - boost::math::expm1(logsum, policy_type());
	  const double denominator =  - boost::math::expm1(logsum_lower_order, policy_type());
	  
	  if (numerator > 0.0) {
	    if (denominator > 0.0)
	      backoffs[shard][pos_context - offset] = utils::mathop::log(numerator) - utils::mathop::log(denominator);
	    else {
	      for (size_type pos = pos_first; pos != pos_last; ++ pos) 
		if (ngram.types[shard][pos] && ((! remove_unk) ||(ngram.index[shard][pos] != unk_id)))
		  logprobs[shard][pos - offset] -= logsum;
	    }
	  }
	  
	  // update offsets
	  offsets[shard] = pos_last;
	}
      }
    }
  };
  
  void NGramCounts::estimate(ngram_type& ngram, const bool remove_unk) const
  {
    typedef NGramCountsEstimateMapReduce map_reduce_type;
    
    typedef map_reduce_type::discount_set_type       discount_set_type;
    typedef map_reduce_type::logprob_shard_type      logprob_shard_type;
    typedef map_reduce_type::logprob_shard_set_type  logprob_shard_set_type;
    typedef map_reduce_type::logbound_shard_type     logbound_shard_type;
    typedef map_reduce_type::logbound_shard_set_type logbound_shard_set_type;
    typedef map_reduce_type::backoff_shard_type      backoff_shard_type;
    typedef map_reduce_type::backoff_shard_set_type  backoff_shard_set_type;
    typedef map_reduce_type::offset_set_type         offset_set_type;
    typedef map_reduce_type::count_map_type          count_map_type;
    
    typedef map_reduce_type::thread_type            thread_type;
    typedef map_reduce_type::thread_ptr_set_type    thread_ptr_set_type;
    
    // collect count of counts...
    count_map_type count_of_counts(index.order() + 1);
    count_map_type count_of_counts_raw(index.order() + 1);
    
    {
      typedef NGramCountsEstimateDiscountMapper mapper_type;

      std::vector<count_map_type, std::allocator<count_map_type> > types(index.size());
      std::vector<count_map_type, std::allocator<count_map_type> > counts(index.size());
      
      thread_ptr_set_type threads(index.size());
      for (size_type shard = 0; shard != index.size(); ++ shard)
	threads[shard].reset(new thread_type(mapper_type(*this, types[shard], counts[shard], shard, remove_unk)));
      
      for (size_type shard = 0; shard != index.size(); ++ shard) {
	threads[shard]->join();
	
	for (int order = 1; order <= index.order(); ++ order) {
	  count_map_type::value_type::const_iterator titer_end = types[shard][order].end();
	  for (count_map_type::value_type::const_iterator titer = types[shard][order].begin(); titer != titer_end; ++ titer)
	    count_of_counts[order][titer->first] += titer->second;
	  
	  count_map_type::value_type::const_iterator citer_end = counts[shard][order].end();
	  for (count_map_type::value_type::const_iterator citer = counts[shard][order].begin(); citer != citer_end; ++ citer)
	    count_of_counts_raw[order][citer->first] += citer->second;
	}
      }
    }
    
    // distounts
    discount_set_type discounts(index.order() + 1);
    discount_set_type discounts_raw(index.order() + 1);
    
    for (int order = 1; order <= index.order(); ++ order) {
      discounts[order].estimate(count_of_counts[order].begin(), count_of_counts[order].end());
      discounts_raw[order].estimate(count_of_counts_raw[order].begin(), count_of_counts_raw[order].end());
    }
    count_of_counts.clear();
    count_of_counts_raw.clear();
    
    if (debug) {
      for (int order = 1; order <= index.order(); ++ order)
	std::cerr << "type counts: order: " << order << ' ' << discounts[order] << std::endl;
      
      for (int order = 1; order <= index.order(); ++ order)
	std::cerr << "raw counts:  order: " << order << ' ' << discounts_raw[order] << std::endl;
    }
    
    // assignment for index...
    ngram.index = index;
    
    ngram.logprobs.reserve(index.size());
    ngram.logbounds.reserve(index.size());
    ngram.backoffs.reserve(index.size());
    
    ngram.logprobs.resize(index.size());
    ngram.logbounds.resize(index.size());
    ngram.backoffs.resize(index.size());
    
    for (size_type shard = 0; shard != index.size(); ++ shard) {
      ngram.logprobs[shard].offset  = types[shard].offset;
      ngram.logbounds[shard].offset = types[shard].offset;
      ngram.backoffs[shard].offset  = types[shard].offset;
    }
    ngram.smooth = boost::numeric::bounds<logprob_type>::lowest();
    
    // we will allocate large memory here...
    logprob_shard_set_type  logprobs(index.size());
    logbound_shard_set_type logbounds(index.size());
    backoff_shard_set_type  backoffs(index.size());
    for (size_type shard = 0; shard != index.size(); ++ shard) {
      logprobs[shard].clear();
      backoffs[shard].clear();
      
      logprobs[shard].reserve(index[shard].size() - types[shard].offset);
      logprobs[shard].resize(index[shard].size() - types[shard].offset, expgram::NGram::logprob_min());

      logbounds[shard].reserve(index[shard].position_size() - types[shard].offset);
      logbounds[shard].resize(index[shard].position_size() - types[shard].offset, expgram::NGram::logprob_min());
      
      backoffs[shard].reserve(index[shard].position_size() - types[shard].offset);
      backoffs[shard].resize(index[shard].position_size() - types[shard].offset, 0.0);
    }
    
    {
      if (debug)
	std::cerr << "order: "  << 1 << std::endl;
      
      // unigrams...
      const id_type bos_id = index.vocab()[vocab_type::BOS];
      const id_type unk_id = index.vocab()[vocab_type::UNK];
      
      count_type total = 0;
      count_type observed = 0;
      count_type min2 = 0;
      count_type min3 = 0;
      count_type zero_events = 0;
      
      for (size_type pos = 0; pos < index[0].offsets[1]; ++ pos) {
	if (id_type(pos) == bos_id) continue;
	
	const count_type count = types[0][pos];
	
	// when remove-unk is enabled, we treat it as zero_events
	if (count == 0 || (remove_unk && id_type(pos) == unk_id)) {
	  ++ zero_events;
	  continue;
	}
	
	total += count;
	++ observed;
	min2 += (count >= discounts[1].mincount2);
	min3 += (count >= discounts[1].mincount3);
      }
      
      double logsum = 0.0;
      const prob_type uniform_distribution = 1.0 / observed;
      
      for (/**/; logsum >= 0.0; ++ total) {
	logsum = boost::numeric::bounds<double>::lowest();
	
	for (size_type pos = 0; pos < index[0].offsets[1]; ++ pos) {
	  if (id_type(pos) == bos_id) continue;
	  
	  const count_type count = types[0][pos];
	  
	  if (count == 0 || (remove_unk && id_type(pos) == unk_id)) continue;
	  
	  const prob_type discount = discounts[1].discount(count, total, observed);
	  const prob_type prob = (discount * count / total);
	  const prob_type lower_order_weight = discounts[1].lower_order_weight(total, observed, min2, min3);
	  const prob_type lower_order_prob = uniform_distribution;
	  const prob_type prob_combined = prob + lower_order_weight * lower_order_prob;
	  const logprob_type logprob = utils::mathop::log(prob_combined);
	  
	  logprobs[0][pos] = logprob;
	  if (id_type(pos) == unk_id)
	    ngram.smooth = logprob;
	  
	  logsum = utils::mathop::logsum(logsum, double(logprob));
	}
      }
      
      using namespace boost::math::policies;
      typedef policy<domain_error<errno_on_error>,
	pole_error<errno_on_error>,
	overflow_error<errno_on_error>,
	rounding_error<errno_on_error>,
	evaluation_error<errno_on_error>
	> policy_type;
      
      //const double discounted_mass =  1.0 - utils::mathop::exp(logsum);

      const double discounted_mass =  - boost::math::expm1(logsum, policy_type());
      if (discounted_mass > 0.0) {
	if (zero_events > 0) {
	  // distribute probability mass to zero events...
	  
	  // if we set remove_unk, then zero events will be incremented when we actually observed UNK
	  
	  const double logdistribute = utils::mathop::log(discounted_mass) - utils::mathop::log(zero_events);
	  for (size_type pos = 0; pos < index[0].offsets[1]; ++ pos)
	    if (id_type(pos) != bos_id && (types[0][pos] == 0 || (remove_unk && id_type(pos) == unk_id)))
	      logprobs[0][pos] = logdistribute;
	  if (ngram.smooth == boost::numeric::bounds<logprob_type>::lowest())
	    ngram.smooth = logdistribute;
	} else {
	  for (size_type pos = 0; pos < index[0].offsets[1]; ++ pos)
	    if (id_type(pos) != bos_id) {
	      logprobs[0][pos] -= logsum;
	      if (id_type(pos) == unk_id)
		ngram.smooth = logprobs[0][pos];
	    }
	}
      }
      
      // bos is assigned -99 (base 10) from srilm
      logprobs[0][bos_id] = NGram::logprob_bos();
      
      // fallback to uniform distribution...
      if (ngram.smooth == boost::numeric::bounds<logprob_type>::lowest())
	ngram.smooth = utils::mathop::log(uniform_distribution);
            
      if (debug)
	std::cerr << "\tsmooth: " << ngram.smooth << std::endl;
            
      
      if (index.order() > 1) {
	// estimate for logbound...!
	count_type total = 0;
	count_type observed = 0;
	count_type min2 = 0;
	count_type min3 = 0;
	
	for (size_type pos = 0; pos < index[0].offsets[1]; ++ pos) {
	  if (id_type(pos) == bos_id) continue;
	  
	  const count_type count = counts[0][pos];
	
	  // when remove-unk is enabled, we treat it as zero_events
	  if (count == 0 || (remove_unk && id_type(pos) == unk_id)) continue;
	
	  total += count;
	  ++ observed;
	  min2 += (count >= discounts_raw[1].mincount2);
	  min3 += (count >= discounts_raw[1].mincount3);
	}
	
	for (size_type pos = 0; pos < index[0].offsets[1]; ++ pos) {
	  if (id_type(pos) == bos_id) continue;
	  
	  const count_type count = counts[0][pos];
	  
	  if (count == 0 || (remove_unk && id_type(pos) == unk_id)) continue;
	  
	  const prob_type discount = discounts_raw[1].discount(count, total, observed);
	  const prob_type prob = (discount * count / total);
	  const prob_type lower_order_weight = discounts_raw[1].lower_order_weight(total, observed, min2, min3);
	  const prob_type lower_order_prob = uniform_distribution;
	  const prob_type prob_combined = prob + lower_order_weight * lower_order_prob;
	  const logprob_type logprob = utils::mathop::log(prob_combined);
	  
	  logbounds[0][pos] = logprob;
	}
      }
    }
    
    {
      if (debug)
	std::cerr << "order: " << 2 << std::endl;

      // bigrams...
      typedef NGramCountsEstimateBigramMapper mapper_type;
      
      thread_ptr_set_type threads(index.size());
      for (size_type shard = 0; shard != threads.size(); ++ shard)
	threads[shard].reset(new thread_type(mapper_type(*this, discounts, discounts_raw, logprobs, logbounds, backoffs, shard, remove_unk, debug)));
      
      for (size_type shard = 0; shard != threads.size(); ++ shard)
	threads[shard]->join();
    }
    
    {
      // others...
      typedef NGramCountsEstimateMapper mapper_type;
      
      thread_ptr_set_type threads(index.size());
      offset_set_type     offsets(index.size(), size_type(0));
      for (size_type shard = 0; shard != offsets.size(); ++ shard) {
	offsets[shard] = ngram.index[shard].offsets[2];
	
	std::cerr << "shard: " << shard << " offset: " << offsets[shard] << std::endl;
      }
      
      for (size_type shard = 0; shard != threads.size(); ++ shard)
	threads[shard].reset(new thread_type(mapper_type(*this, discounts, discounts_raw, logprobs, logbounds, backoffs, offsets, shard, remove_unk, debug)));
      
      for (size_type shard = 0; shard != threads.size(); ++ shard)
	threads[shard]->join();
    }
    
    // finalize...
    for (size_type shard = 0; shard != index.size(); ++ shard) {
      const path_type tmp_dir       = utils::tempfile::tmp_dir();
      const path_type path_logprob  = utils::tempfile::file_name(tmp_dir / "expgram.logprob.XXXXXX");
      const path_type path_logbound = utils::tempfile::file_name(tmp_dir / "expgram.logbound.XXXXXX");
      const path_type path_backoff  = utils::tempfile::file_name(tmp_dir / "expgram.backoff.XXXXXX");
      
      utils::tempfile::insert(path_logprob);
      utils::tempfile::insert(path_logbound);
      utils::tempfile::insert(path_backoff);
      
      dump_file(path_logprob, logprobs[shard]);
      dump_file(path_logbound, logbounds[shard]);
      dump_file(path_backoff, backoffs[shard]);
      
      ::sync();
      
      while (! boost::filesystem::exists(path_logprob))
	boost::thread::yield();
      while (! boost::filesystem::exists(path_logbound))
	boost::thread::yield();      
      while (! boost::filesystem::exists(path_backoff))
	boost::thread::yield();
      
      utils::tempfile::permission(path_logprob);
      utils::tempfile::permission(path_logbound);
      utils::tempfile::permission(path_backoff);
      
      ngram.logprobs[shard].logprobs.open(path_logprob);
      ngram.logbounds[shard].logprobs.open(path_logbound);
      ngram.backoffs[shard].logprobs.open(path_backoff);
      
      logprobs[shard].clear();
      logbounds[shard].clear();
      backoffs[shard].clear();
      
      logprob_shard_type(logprobs[shard]).swap(logprobs[shard]);
      logbound_shard_type(logbounds[shard]).swap(logbounds[shard]);
      backoff_shard_type(backoffs[shard]).swap(backoffs[shard]);
    }

    // backard trie...
    ngram.backward();
  }
  
  struct NGramCountsDumpMapReduce
  {
    typedef boost::thread                                                  thread_type;
    typedef boost::shared_ptr<thread_type>                                 thread_ptr_type;
    typedef std::vector<thread_ptr_type, std::allocator<thread_ptr_type> > thread_ptr_set_type;
    
    typedef expgram::NGramCounts ngram_type;
    
    typedef ngram_type::word_type       word_type;
    typedef ngram_type::vocab_type      vocab_type;
    typedef ngram_type::id_type         id_type;
    
    typedef ngram_type::count_type      count_type;
    typedef ngram_type::size_type       size_type;
    typedef ngram_type::difference_type difference_type;
    
    typedef boost::filesystem::path         path_type;
    
    typedef std::vector<id_type, std::allocator<id_type> >                 context_type;
    typedef std::pair<id_type, count_type>                                 word_count_type;
    typedef std::vector<word_count_type, std::allocator<word_count_type> > word_set_type;
    typedef std::pair<context_type, word_set_type>                         context_count_type;
    
    typedef utils::lockfree_list_queue<context_count_type, std::allocator<context_count_type> > queue_type;
    typedef boost::shared_ptr<queue_type>                                                  queue_ptr_type;
    typedef std::vector<queue_ptr_type, std::allocator<queue_ptr_type> >                   queue_ptr_set_type;
  };
  
  inline
  void swap(NGramCountsDumpMapReduce::context_count_type& x,
	    NGramCountsDumpMapReduce::context_count_type& y)
  {
    x.first.swap(y.first);
    x.second.swap(y.second);
  }
  
  struct NGramCountsDumpMapper
  {
    typedef NGramCountsDumpMapReduce map_reduce_type;
    
    typedef map_reduce_type::ngram_type ngram_type;
    typedef map_reduce_type::id_type    id_type;
    typedef map_reduce_type::size_type  size_type;
    typedef map_reduce_type::count_type count_type;
    
    typedef map_reduce_type::context_type       context_type;
    typedef map_reduce_type::word_set_type      word_set_type;
    typedef map_reduce_type::context_count_type context_count_type;
    
    typedef map_reduce_type::queue_type queue_type;

    const ngram_type& ngram;
    queue_type&       queue;
    int               shard;
    
    NGramCountsDumpMapper(const ngram_type& _ngram,
			  queue_type&       _queue,
			  const int         _shard)
      : ngram(_ngram),
	queue(_queue),
	shard(_shard) {}
    
    void operator()()
    {
      const int max_order = ngram.index.order();

      context_type       context;
      context_count_type context_count;

      for (int order_prev = 1; order_prev < max_order; ++ order_prev) {
	const size_type pos_context_first = ngram.index[shard].offsets[order_prev - 1];
	const size_type pos_context_last  = ngram.index[shard].offsets[order_prev];
	
	context.resize(order_prev);
	
	size_type pos_last_prev = pos_context_last;
	for (size_type pos_context = pos_context_first; pos_context < pos_context_last; ++ pos_context) {
	  const size_type pos_first = pos_last_prev;
	  const size_type pos_last = ngram.index[shard].children_last(pos_context);
	  pos_last_prev = pos_last;
	  
	  if (pos_first == pos_last) continue;
	  
	  context_type::iterator citer_curr = context.end() - 1;
	  for (size_type pos_curr = pos_context; pos_curr != size_type(-1); pos_curr = ngram.index[shard].parent(pos_curr), -- citer_curr)
	    *citer_curr = ngram.index[shard][pos_curr];
	  
	  context_count.first = context;
	  context_count.second.clear();
	  
	  word_set_type& words = context_count.second;
	  for (size_type pos = pos_first; pos != pos_last; ++ pos) {
	    const count_type count = ngram.counts[shard][pos];
	    
	    if (count > 0)
	      words.push_back(std::make_pair(ngram.index[shard][pos], count));
	  }
	  
	  queue.push_swap(context_count);
	}
      }
      
      queue.push(context_count_type(context_type(), word_set_type()));
    }
  };

  template <typename Tp>
  struct greater_pfirst_size_value
  {
    bool operator()(const boost::shared_ptr<Tp>& x, const boost::shared_ptr<Tp>& y) const
    {
      return x->first.size() > y->first.size() || (x->first.size() == y->first.size() && x->first > y->first);
    }
    
    bool operator()(const Tp* x, const Tp* y) const
    {
      return x->first.size() > y->first.size() || (x->first.size() == y->first.size() && x->first > y->first);
    }
  };
  
  
  void NGramCounts::dump(const path_type& path) const
  {
    typedef NGramCountsDumpMapReduce map_reduce_type;
    typedef NGramCountsDumpMapper    mapper_type;
    
    typedef map_reduce_type::thread_type         thread_type;
    typedef map_reduce_type::thread_ptr_set_type thread_ptr_set_type;
    typedef map_reduce_type::queue_type          queue_type;
    typedef map_reduce_type::queue_ptr_set_type  queue_ptr_set_type;
    
    typedef map_reduce_type::context_type       context_type;
    typedef map_reduce_type::word_set_type      word_set_type;
    typedef map_reduce_type::context_count_type context_count_type;
    
    typedef std::vector<const char*, std::allocator<const char*> > vocab_map_type;

    typedef std::pair<word_set_type, queue_type*>       words_queue_type;
    typedef std::pair<context_type, words_queue_type>   context_words_queue_type;
    typedef boost::shared_ptr<context_words_queue_type> context_words_queue_ptr_type;
    
    typedef std::vector<context_words_queue_ptr_type, std::allocator<context_words_queue_ptr_type> > pqueue_base_type;
    typedef std::priority_queue<context_words_queue_ptr_type, pqueue_base_type, greater_pfirst_size_value<context_words_queue_type> > pqueue_type;
    
    if (index.empty()) return;
    
    thread_ptr_set_type threads(index.size());
    queue_ptr_set_type  queues(index.size());
    
    for (size_type shard = 0; shard != index.size(); ++ shard) {
      queues[shard].reset(new queue_type(1024 * 64));
      threads[shard].reset(new thread_type(mapper_type(*this, *queues[shard], shard)));
    }
    
    vocab_map_type vocab_map;
    vocab_map.reserve(index[0].offsets[1]);
    
    utils::compress_ostream os(path, 1024 * 1024);
    os.exceptions(std::ostream::eofbit | std::ostream::failbit | std::ostream::badbit);
    
    // unigrams

    for (size_type pos = 0; pos < index[0].offsets[1]; ++ pos) {
      const count_type count = counts[0][pos];
      if (count > 0) {
	const id_type id(pos);
	
	if (id >= vocab_map.size())
	  vocab_map.resize(id + 1, 0);
	if (! vocab_map[id])
	  vocab_map[id] = static_cast<const std::string&>(index.vocab()[id]).c_str();
	
	os << vocab_map[id] << '\t' << count << '\n';
      }
    }
    
    pqueue_type pqueue;
    for (size_type shard = 0; shard != index.size(); ++ shard) {
      context_count_type context_count;
      queues[shard]->pop_swap(context_count);
      
      if (! context_count.first.empty()) {
	context_words_queue_ptr_type context_queue(new context_words_queue_type());
	context_queue->first.swap(context_count.first);
	context_queue->second.first.swap(context_count.second);
	context_queue->second.second = &(*queues[shard]);
	
	pqueue.push(context_queue);
      }
    }

    typedef std::vector<const char*, std::allocator<const char*> > phrase_type;
    phrase_type phrase;
    
    while (! pqueue.empty()) {
      context_words_queue_ptr_type context_queue(pqueue.top());
      pqueue.pop();
      
      phrase.clear();
      context_type::const_iterator citer_end = context_queue->first.end();
      for (context_type::const_iterator citer = context_queue->first.begin(); citer != citer_end; ++ citer) {
	if (*citer >= vocab_map.size())
	  vocab_map.resize(*citer + 1, 0);
	if (! vocab_map[*citer])
	  vocab_map[*citer] = static_cast<const std::string&>(index.vocab()[*citer]).c_str();
	
	phrase.push_back(vocab_map[*citer]);
      }
      
      word_set_type::const_iterator witer_end = context_queue->second.first.end();
      for (word_set_type::const_iterator witer = context_queue->second.first.begin(); witer != witer_end; ++ witer) {
	const id_type id = witer->first;
	
	if (id >= vocab_map.size())
	  vocab_map.resize(id + 1, 0);
	if (! vocab_map[id])
	  vocab_map[id] = static_cast<const std::string&>(index.vocab()[id]).c_str();
	
	std::copy(phrase.begin(), phrase.end(), std::ostream_iterator<const char*>(os, " "));
	os << vocab_map[id] << '\t' << witer->second << '\n';
      }
      
      context_count_type context_count;
      context_queue->second.second->pop_swap(context_count);
      if (! context_count.first.empty()) {
	context_queue->first.swap(context_count.first);
	context_queue->second.first.swap(context_count.second);
	
	pqueue.push(context_queue);
      }
    }
  }
  
  void NGramCounts::ShardData::open(const path_type& path)
  {
    typedef utils::repository repository_type;
    
    clear();
    
    repository_type rep(path, repository_type::read);
    
    repository_type::const_iterator oiter = rep.find("offset");
    if (oiter == rep.end())
      throw std::runtime_error("no offset?");
    offset = utils::lexical_cast<size_type>(oiter->second);
    
    counts.open(rep.path("counts"));
  }
  
  void NGramCounts::ShardData::write(const path_type& file) const
  {
    typedef utils::repository repository_type;
    
    if (path() == file) return;
    
    repository_type rep(file, repository_type::write);
    
    counts.write(rep.path("counts"));
    
    rep["offset"] = utils::lexical_cast<std::string>(offset);
  }
  
  template <typename Path, typename Shards>
  inline
  void open_shards(const Path& path, Shards& shards, int shard)
  {
    typedef utils::repository repository_type;
    
    repository_type rep(path, repository_type::read);
    
    repository_type::const_iterator siter = rep.find("shard");
    if (siter == rep.end())
      throw std::runtime_error("no shard size...");
    
    shards.clear();
    shards.reserve(utils::lexical_cast<size_t>(siter->second));
    shards.resize(utils::lexical_cast<size_t>(siter->second));

    if (shard >= static_cast<int>(shards.size()))
      throw std::runtime_error("shard is out of range");
    
    std::ostringstream stream_shard;
    stream_shard << "ngram-" << std::setfill('0') << std::setw(6) << shard;
    
    shards[shard].open(rep.path(stream_shard.str()));
  }

  template <typename Path, typename Shards>
  inline
  void open_shards(const Path& path, Shards& shards)
  {
    typedef size_t    size_type;
    typedef ptrdiff_t difference_type;
    
    typedef utils::repository repository_type;
    
    repository_type rep(path, repository_type::read);
    
    repository_type::const_iterator siter = rep.find("shard");
    if (siter == rep.end())
      throw std::runtime_error("no shard size...");

    shards.clear();
    shards.reserve(utils::lexical_cast<size_t>(siter->second));
    shards.resize(utils::lexical_cast<size_t>(siter->second));
    
    for (size_type shard = 0; shard != shards.size(); ++ shard) {
      std::ostringstream stream_shard;
      stream_shard << "ngram-" << std::setfill('0') << std::setw(6) << shard;
      
      shards[shard].open(rep.path(stream_shard.str()));
    }
  }
  
  template <typename Path, typename Shards>
  inline
  void write_shards_prepare(const Path& path, const Shards& shards)
  {
    typedef utils::repository repository_type;
    
    repository_type rep(path, repository_type::write);
      
    rep["shard"] = utils::lexical_cast<std::string>(shards.size());
  }

  template <typename Path, typename Shards>
  inline
  void write_shards(const Path& path, const Shards& shards, int shard)
  {
    typedef utils::repository repository_type;
    
    while (! repository_type::exists(path))
      boost::thread::yield();
    
    repository_type rep(path, repository_type::read);
    
    std::ostringstream stream_shard;
    stream_shard << "ngram-" << std::setfill('0') << std::setw(6) << shard;
    
    shards[shard].write(rep.path(stream_shard.str()));
  }

  template <typename Path, typename Shard>
  struct TaskWriter
  {
    typedef boost::thread                                                  thread_type;
    typedef boost::shared_ptr<thread_type>                                 thread_ptr_type;
    typedef std::vector<thread_ptr_type, std::allocator<thread_ptr_type> > thread_ptr_set_type;

    Path path;
    const Shard& shard;
    
    TaskWriter(const Path& _path,
	       const Shard& _shard)
      : path(_path),
	shard(_shard) {}
    
    void operator()()
    {
      shard.write(path);
    }
  };

  template <typename Path, typename Shards>
  inline
  void write_shards(const Path& path, const Shards& shards)
  {
    typedef size_t    size_type;
    typedef ptrdiff_t difference_type;
    
    typedef utils::repository repository_type;
    
    typedef TaskWriter<Path, typename Shards::value_type>    task_type;
    typedef typename task_type::thread_type         thread_type;
    typedef typename task_type::thread_ptr_set_type thread_ptr_set_type;
    
    {
      repository_type rep(path, repository_type::write);
      
      rep["shard"] = utils::lexical_cast<std::string>(shards.size());
    }
    
    ::sync();
    
    thread_ptr_set_type threads(shards.size());
    
    {
      for (size_type shard = 1; shard != shards.size(); ++ shard) {
	std::ostringstream stream_shard;
	stream_shard << "ngram-" << std::setfill('0') << std::setw(6) << shard;
	
	threads[shard].reset(new thread_type(task_type(path / stream_shard.str(), shards[shard])));
      }
      
      // dump for root...
      std::ostringstream stream_shard;
      stream_shard << "ngram-" << std::setfill('0') << std::setw(6) << 0;
      task_type(path / stream_shard.str(), shards[0])();
      
      // terminate...
      for (size_type shard = 1; shard != shards.size(); ++ shard)
	threads[shard]->join();
    }
    
    threads.clear();
  }
  
  void NGramCounts::open(const path_type& path, const size_type shard_size)
  {
    if (! boost::filesystem::exists(path))
      throw std::runtime_error(std::string("no file: ") + path.string());
    
    if (! boost::filesystem::is_directory(path))
      throw std::runtime_error(std::string("invalid path: ") + path.string());

    clear();
    
    if (boost::filesystem::exists(path / "prop.list"))
      open_binary(path);
    else
      open_google(path, shard_size);
  }

  void NGramCounts::write_prepare(const path_type& file) const
  {
    typedef utils::repository repository_type;
    
    if (path() == file) return;
    
    {
      repository_type rep(file, repository_type::write);
      
      index.write_prepare(rep.path("index"));
      
      if (! counts.empty())
	write_shards_prepare(rep.path("count"), counts);
      
      if (! types.empty())
	write_shards_prepare(rep.path("type"), types);
    }
    
    ::sync();
  }

  void NGramCounts::write_shard(const path_type& file, int shard) const
  {
    typedef utils::repository repository_type;
    
    if (path() == file) return;
    
    while (! repository_type::exists(file))
      boost::thread::yield();
    
    repository_type rep(file, repository_type::read);
    
    index.write_shard(rep.path("index"), shard);
    
    if (! counts.empty())
      write_shards(rep.path("count"), counts, shard);
    
    if (! types.empty())
      write_shards(rep.path("type"), types, shard);
  }

  void NGramCounts::write(const path_type& file) const
  {
    typedef utils::repository repository_type;
    
    if (path() == file) return;
    
    repository_type rep(file, repository_type::write);
    
    index.write(rep.path("index"));
    
    if (! counts.empty())
      write_shards(rep.path("count"), counts);

    if (! types.empty())
      write_shards(rep.path("type"), types);
  }

  void NGramCounts::open_shard(const path_type& path, int shard)
  {
    typedef utils::repository repository_type;
    
    clear();

    if (! boost::filesystem::exists(path))
      throw std::runtime_error("no file? " + path.string());
    
    repository_type rep(path, repository_type::read);
    
    index.open_shard(rep.path("index"), shard);
    
    if (boost::filesystem::exists(rep.path("count")))
      open_shards(rep.path("count"), counts, shard);

    if (boost::filesystem::exists(rep.path("type")))
      open_shards(rep.path("type"), types, shard);
  }

  void NGramCounts::open_binary(const path_type& path)
  {
    typedef utils::repository repository_type;
    
    clear();
    
    if (path.empty())
      throw std::runtime_error("no ngram counts?");
    else if (! boost::filesystem::exists(path))
      throw std::runtime_error("no ngram counts? " + path.string());

    repository_type rep(path, repository_type::read);
    
    index.open(rep.path("index"));
    
    if (boost::filesystem::exists(rep.path("count")))
      open_shards(rep.path("count"), counts);
    
    if (boost::filesystem::exists(rep.path("type")))
      open_shards(rep.path("type"), types);
  }
  
  static const std::string& __BOS = static_cast<const std::string&>(Vocab::BOS);
  static const std::string& __EOS = static_cast<const std::string&>(Vocab::EOS);
  static const std::string& __UNK = static_cast<const std::string&>(Vocab::UNK);
  
  inline
  NGramCounts::word_type escape_word(const utils::piece& __word)
  {
    const utils::ipiece word(__word);
    
    if (word == __BOS)
      return Vocab::BOS;
    else if (word == __EOS)
      return Vocab::EOS;
    else if (word == __UNK)
      return Vocab::UNK;
    else
      return __word;
  }

  struct NGramCountsIndexMapReduce
  {
    typedef expgram::NGramCounts ngram_type;
    
    typedef ngram_type::size_type       size_type;
    typedef ngram_type::difference_type difference_type;
    
    typedef ngram_type::count_type      count_type;
    typedef ngram_type::word_type       word_type;
    typedef ngram_type::id_type         id_type;
    typedef ngram_type::vocab_type      vocab_type;

    typedef boost::iostreams::filtering_ostream ostream_type;
    
    typedef ngram_type::path_type                              path_type;
    typedef std::vector<path_type, std::allocator<path_type> > path_set_type;

    typedef std::vector<id_type, std::allocator<id_type> >         context_type;
    
    typedef std::vector<std::string, std::allocator<std::string> > ngram_context_type;
    typedef std::pair<ngram_context_type, count_type>              ngram_context_count_type;
    
    typedef utils::lockfree_list_queue<ngram_context_count_type, std::allocator<ngram_context_count_type> > queue_type;
    typedef boost::shared_ptr<queue_type>                                                              queue_ptr_type;
    typedef utils::vector2<queue_ptr_type, std::allocator<queue_ptr_type> >                            queue_ptr_set_type;
    
    typedef boost::thread                                                  thread_type;
    typedef boost::shared_ptr<thread_type>                                 thread_ptr_type;
    typedef std::vector<thread_ptr_type, std::allocator<thread_ptr_type> > thread_ptr_set_type;

    typedef std::vector<id_type, std::allocator<id_type> > vocab_map_type;
  };

  inline
  void swap(NGramCountsIndexMapReduce::ngram_context_count_type& x,
	    NGramCountsIndexMapReduce::ngram_context_count_type& y)
  {
    x.first.swap(y.first);
    std::swap(x.second, y.second);
  }

  struct NGramCountsIndexMapper
  {
    typedef NGramCountsIndexMapReduce map_reduce_type;
    
    typedef map_reduce_type::ngram_type         ngram_type;
    
    typedef map_reduce_type::word_type          word_type;
    typedef map_reduce_type::id_type            id_type;
    typedef map_reduce_type::size_type          size_type;
    typedef map_reduce_type::count_type         count_type;
    
    typedef map_reduce_type::ngram_context_type       ngram_context_type;
    typedef map_reduce_type::ngram_context_count_type ngram_context_count_type;
    
    typedef map_reduce_type::path_type          path_type;
    typedef map_reduce_type::path_set_type      path_set_type;
    typedef map_reduce_type::vocab_map_type     vocab_map_type;
    
    typedef map_reduce_type::queue_ptr_set_type queue_ptr_set_type;
    
    
    const ngram_type&     ngram;
    const vocab_map_type& vocab_map;
    path_set_type         paths;
    queue_ptr_set_type&   queues;
    int                   shard;
    int                   debug;
    
    NGramCountsIndexMapper(const ngram_type&     _ngram,
			   const vocab_map_type& _vocab_map,
			   const path_set_type&  _paths,
			   queue_ptr_set_type&   _queues,
			   const int             _shard,
			   const int             _debug)
      : ngram(_ngram),
	vocab_map(_vocab_map),
	paths(_paths),
	queues(_queues),
	shard(_shard),
	debug(_debug) {}

    template <typename Tp>
    struct greater_ngram
    {
      bool operator()(const Tp* x, const Tp* y) const
      {
	return x->first.front().first > y->first.front().first;
      }
      
      bool operator()(const boost::shared_ptr<Tp>& x, const boost::shared_ptr<Tp>& y) const
      {
	return x->first.front().first > y->first.front().first;
      }
    };
    
    
    size_type shard_index(const ngram_context_type& context) const
    {
      id_type ids[2];
      
      ids[0] = vocab_map[escape_word(context[0]).id()];
      ids[1] = vocab_map[escape_word(context[1]).id()];
      
      return ngram.index.shard_index(ids, ids + 2);
    }
    
    void operator()()
    {
      typedef std::istream istream_type;
      typedef boost::shared_ptr<istream_type>  istream_ptr_type;

      typedef std::pair<ngram_context_type, count_type> context_count_type;
      typedef std::deque<context_count_type, std::allocator<context_count_type> > context_count_set_type;
      
      typedef std::pair<context_count_set_type, istream_type*> context_count_stream_type;
      typedef boost::shared_ptr<context_count_stream_type>     context_count_stream_ptr_type;
      typedef std::vector<context_count_stream_ptr_type, std::allocator<context_count_stream_ptr_type> > pqueue_base_type;
      typedef std::priority_queue<context_count_stream_ptr_type, pqueue_base_type, greater_ngram<context_count_stream_type> > pqueue_type;
      
      typedef std::vector<utils::piece, std::allocator<utils::piece> > tokens_type;
      typedef boost::tokenizer<utils::space_separator, utils::piece::const_iterator, utils::piece> tokenizer_type;

      if (! paths.empty()) {
	
	pqueue_type pqueue;
	
	std::vector<istream_ptr_type, std::allocator<istream_ptr_type> > streams(paths.size());
	
	std::string line;
	tokens_type tokens;
	
	for (size_type i = 0; i != paths.size(); ++ i) {
	  streams[i].reset(new utils::compress_istream(paths[i], 1024 * 1024));
	  
	  context_count_stream_ptr_type context_stream(new context_count_stream_type());
	  context_stream->second = &(*streams[i]);
	  
	  while (context_stream->first.size() < 256 && std::getline(*streams[i], line)) {
	    utils::piece line_piece(line);
	    tokenizer_type tokenizer(line_piece);
	    tokens.clear();
	    tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
	    if (tokens.size() < 3)
	      continue;

	    context_stream->first.push_back(std::make_pair(ngram_context_type(tokens.begin(), tokens.end() - 1),
							   utils::lexical_cast<count_type>(tokens.back())));
	  }
	  
	  if (! context_stream->first.empty())
	    pqueue.push(context_stream);
	}
      
	ngram_context_type context;
	count_type count = 0;
      
	ngram_context_type prefix_shard;
	int ngram_shard = 0;
      
	while (! pqueue.empty()) {
	  context_count_stream_ptr_type context_stream(pqueue.top());
	  pqueue.pop();
	  
	  if (context != context_stream->first.front().first) {
	    if (count > 0) {
	      if (context.size() == 2)
		ngram_shard = shard_index(context);
	      else if (prefix_shard.empty() || ! std::equal(prefix_shard.begin(), prefix_shard.end(), context.begin())) {
		ngram_shard = shard_index(context);
		
		prefix_shard.clear();
		prefix_shard.insert(prefix_shard.end(), context.begin(), context.begin() + 2);
	      }
	      
	      queues(shard, ngram_shard)->push(std::make_pair(context, count));
	    }
	    
	    context.swap(context_stream->first.front().first);
	    count = 0;
	  }
	  
	  count += context_stream->first.front().second;
	  context_stream->first.pop_front();
	  
	  if (context_stream->first.empty())
	    while (context_stream->first.size() < 256 && std::getline(*(context_stream->second), line)) {
	      utils::piece line_piece(line);
	      tokenizer_type tokenizer(line_piece);
	      tokens.clear();
	      tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
	      if (tokens.size() < 3)
		continue;
	      
	      context_stream->first.push_back(std::make_pair(ngram_context_type(tokens.begin(), tokens.end() - 1),
							     utils::lexical_cast<count_type>(tokens.back())));
	    }
	  
	  if (! context_stream->first.empty())
	    pqueue.push(context_stream);
	}
      
	if (count > 0) {
	  if (context.size() == 2)
	    ngram_shard = shard_index(context);
	  else if (prefix_shard.empty() || ! std::equal(prefix_shard.begin(), prefix_shard.end(), context.begin())) {
	    ngram_shard = shard_index(context);
	  
	    prefix_shard.clear();
	    prefix_shard.insert(prefix_shard.end(), context.begin(), context.begin() + 2);
	  }
	
	  queues(shard, ngram_shard)->push(std::make_pair(context, count));
	}

      }
      
      // termination...
      for (size_type ngram_shard = 0; ngram_shard != queues.size2(); ++ ngram_shard)
	queues(shard, ngram_shard)->push(std::make_pair(ngram_context_type(), count_type(0)));
    }
  };
  
  struct NGramCountsIndexReducer
  {
    typedef NGramCountsIndexMapReduce map_reduce_type;
    
    typedef map_reduce_type::ngram_type      ngram_type;
    
    typedef map_reduce_type::size_type       size_type;
    typedef map_reduce_type::difference_type difference_type;
    
    typedef map_reduce_type::word_type       word_type;
    typedef map_reduce_type::vocab_type      vocab_type;
    typedef map_reduce_type::id_type         id_type;
    typedef map_reduce_type::path_type       path_type;
    typedef map_reduce_type::ostream_type    ostream_type;
    
    typedef map_reduce_type::count_type               count_type;

    typedef map_reduce_type::context_type             context_type;
    typedef map_reduce_type::ngram_context_type       ngram_context_type;
    typedef map_reduce_type::ngram_context_count_type ngram_context_count_type;
    
    typedef map_reduce_type::queue_type               queue_type;
    typedef map_reduce_type::queue_ptr_set_type       queue_ptr_set_type;
    
    typedef map_reduce_type::vocab_map_type           vocab_map_type;
    
    typedef std::pair<id_type, count_type>                                 word_count_type;
    typedef std::vector<word_count_type, std::allocator<word_count_type> > word_count_set_type;    
    
    typedef NGramCountsIndexer<ngram_type> indexer_type;
    
    ngram_type&           ngram;
    const vocab_map_type& vocab_map;
    queue_ptr_set_type&   queues;
    ostream_type&         os_count;
    int                   shard;
    int                   debug;
    
    NGramCountsIndexReducer(ngram_type&           _ngram,
			    const vocab_map_type& _vocab_map,
			    queue_ptr_set_type&   _queues,
			    ostream_type&         _os_count,
			    const int             _shard,
			    const int             _debug)
      : ngram(_ngram),
	vocab_map(_vocab_map),
	queues(_queues),
	os_count(_os_count),
	shard(_shard),
	debug(_debug) {}
    
    void operator()()
    {
      typedef std::pair<count_type, queue_type*> count_queue_type;
      typedef std::pair<ngram_context_type, count_queue_type> context_count_queue_type;
      typedef boost::shared_ptr<context_count_queue_type>     context_count_queue_ptr_type;
      typedef std::vector<context_count_queue_ptr_type, std::allocator<context_count_queue_ptr_type> > pqueue_base_type;
      typedef std::priority_queue<context_count_queue_ptr_type, pqueue_base_type, greater_pfirst_size_value<context_count_queue_type> > pqueue_type;

      indexer_type indexer;
      
      pqueue_type pqueue;
      
      ngram_context_count_type context_count;
      
      for (size_type ngram_shard = 0; ngram_shard != queues.size1(); ++ ngram_shard) {
	queues(ngram_shard, shard)->pop_swap(context_count);
	
	if (! context_count.first.empty()) {
	  context_count_queue_ptr_type context_queue(new context_count_queue_type());
	  context_queue->first.swap(context_count.first);
	  context_queue->second.first = context_count.second;
	  context_queue->second.second = &(*queues(ngram_shard, shard));
	  
	  pqueue.push(context_queue);
	}
      }
      

      ngram_context_type  prefix;
      context_type        context;
      word_count_set_type words;
      
      while (! pqueue.empty()) {
	context_count_queue_ptr_type context_queue(pqueue.top());
	pqueue.pop();

	if (prefix.size() + 1 != context_queue->first.size() || ! std::equal(prefix.begin(), prefix.end(), context_queue->first.begin())) {
	  if (! words.empty()) {
	    
	    context.clear();
	    ngram_context_type::const_iterator piter_end = prefix.end();
	    for (ngram_context_type::const_iterator piter = prefix.begin(); piter != piter_end; ++ piter)
	      context.push_back(vocab_map[escape_word(*piter).id()]);
	    
	    indexer(shard, ngram, context, words);
	    words.clear();
	  }
	  
	  prefix.clear();
	  prefix.insert(prefix.end(), context_queue->first.begin(), context_queue->first.end() - 1);
	}
	
	const id_type id = vocab_map[escape_word(context_queue->first.back()).id()];
	
	if (words.empty() || words.back().first != id)
	  words.push_back(std::make_pair(id, context_queue->second.first));
	else
	  words.back().second += context_queue->second.first;
	
	context_queue->second.second->pop_swap(context_count);
	if (! context_count.first.empty()) {
	  context_queue->first.swap(context_count.first);
	  context_queue->second.first = context_count.second;
	  
	  pqueue.push(context_queue);
	}
      }
      
      if (! words.empty()) {
	context.clear();
	ngram_context_type::const_iterator piter_end = prefix.end();
	for (ngram_context_type::const_iterator piter = prefix.begin(); piter != piter_end; ++ piter)
	  context.push_back(vocab_map[escape_word(*piter).id()]);
	
	indexer(shard, ngram, context, words);
	words.clear();

	indexer(shard, ngram, os_count, debug);
      } 
    }
  };


  inline
  boost::filesystem::path compressed_filename(const boost::filesystem::path& path)
  {
    typedef boost::filesystem::path path_type;
    
    if (boost::filesystem::exists(path))
      return path;
    
    const std::string extension = path.extension().string();
    
    if (extension == ".gz" || extension == ".bz2") {
      // try strip and add new extention..
      const path_type path_gz  = path.parent_path() / (path.stem().string() + ".gz");
      const path_type path_bz2 = path.parent_path() / (path.stem().string() + ".bz2");

      if (boost::filesystem::exists(path_gz))
	return path_gz;
      else if (boost::filesystem::exists(path_bz2))
	return path_bz2;
    } else {
      // try add .gz....
      const path_type path_gz  = path.parent_path() / (path.filename().string() + ".gz");
      const path_type path_bz2 = path.parent_path() / (path.filename().string() + ".bz2");
      
      if (boost::filesystem::exists(path_gz))
	return path_gz;
      else if (boost::filesystem::exists(path_bz2))
	return path_bz2;
    }
    
    return path;
  }
  
  void NGramCounts::open_google(const path_type& path,
				const size_type shard_size)
  {
    typedef boost::iostreams::filtering_ostream                              ostream_type;
    typedef boost::shared_ptr<ostream_type>                                  ostream_ptr_type;
    typedef std::vector<ostream_ptr_type, std::allocator<ostream_ptr_type> > ostream_ptr_set_type;
    
    typedef std::vector<path_type, std::allocator<path_type> >               path_set_type;
    
    typedef std::vector<utils::piece, std::allocator<utils::piece> > tokens_type;
    typedef boost::tokenizer<utils::space_separator, utils::piece::const_iterator, utils::piece> tokenizer_type;
    
    typedef NGramCountsIndexMapReduce::vocab_map_type vocab_map_type;

    clear();

    if (! boost::filesystem::exists(path))
      throw std::runtime_error("no file? " + path.string());
    
    index.reserve(shard_size);
    counts.reserve(shard_size);
    
    index.resize(shard_size);
    counts.resize(shard_size);
    
    // setup paths and streams for logprob/backoff
    const path_type tmp_dir = utils::tempfile::tmp_dir();
    
    ostream_ptr_set_type os_counts(shard_size);
    path_set_type        path_counts(shard_size);
    for (size_type shard = 0; shard != shard_size; ++ shard) {
      path_counts[shard] = utils::tempfile::directory_name(tmp_dir / "expgram.count.XXXXXX");
      
      utils::tempfile::insert(path_counts[shard]);
      
      os_counts[shard].reset(new ostream_type());
      os_counts[shard]->push(utils::packed_sink<count_type, std::allocator<count_type> >(path_counts[shard]));
    }

    if (debug)
      std::cerr << "order: " << 1 << std::endl;
    
    // first, index unigram...
    vocab_map_type vocab_map;
    size_type      unigram_size = 0;
    {
      typedef std::vector<word_type, std::allocator<word_type> > word_set_type;
      
      const path_type ngram_dir         = path / "1gms";
      const path_type vocab_file        = compressed_filename(ngram_dir / "vocab.gz");
      const path_type vocab_sorted_file = compressed_filename(ngram_dir / "vocab_cs.gz");
      
      if (! boost::filesystem::exists(vocab_sorted_file))
	throw std::runtime_error("no sorted vocabulary file? " + vocab_sorted_file.string());
      
      utils::compress_istream is(vocab_sorted_file, 1024 * 1024);
      
      word_set_type words;
      
      id_type word_id = 0;
      std::string line;
      tokens_type tokens;
      while (std::getline(is, line)) {
	utils::piece line_piece(line);
	tokenizer_type tokenizer(line_piece);
	
	tokens.clear();
	tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
	
	if (tokens.size() != 2) continue;
	
	const word_type word = escape_word(tokens.front());
	const count_type count = utils::lexical_cast<count_type>(tokens.back());
	
	if (word.id() >= vocab_map.size())
	  vocab_map.resize(word.id() + 1, id_type(-1));
	vocab_map[word.id()] = word_id;
	
	os_counts[0]->write((char*) &count, sizeof(count_type));
	
	words.push_back(word);
	
	++ word_id;
      }
      
      
      
      const path_type path_vocab = utils::tempfile::directory_name(utils::tempfile::tmp_dir() / "expgram.vocab.XXXXXX");
      utils::tempfile::insert(path_vocab);
      
      vocab_type& vocab = index.vocab();
      vocab.open(path_vocab, words.size() >> 1);
      
      word_set_type::const_iterator witer_end = words.end();
      for (word_set_type::const_iterator witer = words.begin(); witer != witer_end; ++ witer)
	vocab.insert(*witer);
      
      words.clear();
      word_set_type(words).swap(words);
      
      vocab.close();
      
      ::sync();
      while (! vocab_type::exists(path_vocab))
	boost::thread::yield();
      
      utils::tempfile::permission(path_vocab);
      
      vocab.open(path_vocab);
      unigram_size = word_id;
      
      vocab_map_type(vocab_map).swap(vocab_map);
    }
    
    if (debug)
      std::cerr << "\t1-gram size: " << unigram_size << std::endl;
    
    for (size_type shard = 0; shard != index.size(); ++ shard) {
      index[shard].offsets.clear();
      index[shard].offsets.push_back(0);
      index[shard].offsets.push_back(unigram_size);
      
      counts[shard].offset = unigram_size;
    }
    counts[0].offset = 0;
    
    // second, ngrams...
    // we need to map/reduce counts, since counts are not unique!
      
    typedef NGramCountsIndexMapReduce map_reduce_type;
    typedef NGramCountsIndexMapper    mapper_type;
    typedef NGramCountsIndexReducer   reducer_type;
      
    typedef map_reduce_type::queue_type              queue_type;
    typedef map_reduce_type::queue_ptr_set_type      queue_ptr_set_type;
      
    typedef map_reduce_type::thread_type             thread_type;
    typedef map_reduce_type::thread_ptr_set_type     thread_ptr_set_type;
      
    typedef map_reduce_type::path_set_type           path_set_type;
      
    for (int order = 2; /**/; ++ order) {
      std::ostringstream stream_ngram;
      stream_ngram << order << "gms";
	
      std::ostringstream stream_index;
      stream_index << order << "gm.idx";
	
      const path_type ngram_dir = path / stream_ngram.str();
      const path_type index_file = ngram_dir / stream_index.str();
	
      if (! boost::filesystem::exists(ngram_dir) || ! boost::filesystem::exists(index_file)) break;
	
      path_set_type paths_ngram;
      {
	utils::compress_istream is_index(index_file);
	std::string line;
	tokens_type tokens;
	while (std::getline(is_index, line)) {
	  utils::piece line_piece(line);
	  tokenizer_type tokenizer(line_piece);
	    
	  tokens.clear();
	  tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
	    
	  if (tokens.empty()) continue;
	  
	  if (static_cast<int>(tokens.size()) != order + 1)
	    throw std::runtime_error(std::string("invalid google ngram format...") + index_file.string());
	  
	  const path_type path_ngram = compressed_filename(ngram_dir / static_cast<std::string>(tokens.front()));
	  
	  if (debug >= 2)
	    std::cerr << "\tfile: " << path_ngram.string() << std::endl;
	  
	  if (! boost::filesystem::exists(path_ngram))
	    throw std::runtime_error(std::string("invalid google ngram format... no file: ") + path_ngram.string());
	  
	  paths_ngram.push_back(path_ngram);
	}
      }
	
      if (paths_ngram.empty()) break;
	
      if (debug)
	std::cerr << "order: " << order << std::endl;

      index.order() = order;
	
      std::vector<path_set_type, std::allocator<path_set_type> > paths(shard_size);
	
      for (size_type i = 0; i < paths_ngram.size(); ++ i)
	paths[i % paths.size()].push_back(paths_ngram[i]);
	
      // map-reduce here!
      queue_ptr_set_type      queues(index.size(), index.size());
      thread_ptr_set_type     threads_mapper(index.size());
      thread_ptr_set_type     threads_reducer(index.size());
	
      for (size_type i = 0; i != shard_size; ++ i)
	for (size_type j = 0; j != shard_size; ++ j)
	  queues(i, j).reset(new queue_type(1024 * 32));
	
      // first, reducer...
      for (size_type shard = 0; shard != shard_size; ++ shard)
	threads_reducer[shard].reset(new thread_type(reducer_type(*this, vocab_map, queues, *os_counts[shard], shard, debug)));
	
      // second, mapper...
      for (size_type shard = 0; shard != shard_size; ++ shard)
	threads_mapper[shard].reset(new thread_type(mapper_type(*this, vocab_map, paths[shard], queues, shard, debug)));
	
      // termination...
      for (size_type shard = 0; shard != shard_size; ++ shard)
	threads_mapper[shard]->join();
      for (size_type shard = 0; shard != shard_size; ++ shard)
	threads_reducer[shard]->join();
    }
      
    // termination...
    for (size_type shard = 0; shard != shard_size; ++ shard)
      os_counts[shard].reset();
    
    ::sync();
    
    for (size_type shard = 0; shard != shard_size; ++ shard) {
      while (! shard_data_type::count_set_type::exists(path_counts[shard]))
	boost::thread::yield();
	
      utils::tempfile::permission(path_counts[shard]);
	
      counts[shard].counts.open(path_counts[shard]);
    }
  }
  
};
