
#include <sstream>

#include "NGramCounts.hpp"

namespace expgram
{
  template <typename Path, typename Data>
  inline
  void dump_file(const Path& file, const Data& data)
  {
    std::auto_ptr<boost::iostreams::filtering_ostream> os(new boost::iostreams::filtering_ostream());
    os->push(boost::iostreams::file_sink(file.native_file_string(), std::ios_base::out | std::ios_base::trunc), 1024 * 1024);
    
    const int64_t file_size = sizeof(typename Data::value_type) * data.size();
    for (int64_t offset = 0; offset < file_size; offset += 1024 * 1024)
      os->write(((char*) &(*data.begin())) + offset, std::min(int64_t(1024 * 1024), file_size - offset));
  }  
  
  
  struct NGramCountsModifyMapReduce
  {
    
    
  };

  struct NGramCountsModifyMapper
  {

    void operator()()
    {
      typedef std::vector<count_type, std::allocator<count_type> > count_set_type;

      const id_type bos_id = ngram.index.vocab()[vocab_type::BOS];
      const int max_order = ngram.index.order();
      
      count_set_type unigrams(ngram.index[shard].offsets[1], 0);
      context_type context;
      
      for (int order_prev = 1; order_prev < max_order; ++ order_prev) {
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
      
      for (int shard = 0; shard < queues.size(); ++ shard)
	queues[shard]->push(std::make_pair(context_type(), 0.0));
    }
  };
  
  struct NGramCountsModifyReducer
  {
    
    template <typename Iterator>
    void dump(std::ostream& os, Iterator first, Iterator last)
    {
      typedef typename std::iterator_traits<Iterator>::value_type value_type;
      os.write((char*) &(*first), (last - first) * sizeof(value_type));
    }

    void operator()()
    {
      
      const size_type offset = shard_data.offset;
      count_set_type counts_modified(ngram.index[shard].position_size() - offset);
      
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
	if (result.first != context.end() || result.second == size_type(-1))
	  throw std::runtime_error("no ngram?");
	
	counts_modified[result.second - offset] += context_count.second;
      }
      
      const path_type path = utils::tempfile::file_name(utils::tempfile::tmp_dir() / "expgram.counts-modified.XXXXXX");
      utils::tempfile::insert(path);
      
      boost::iostreams::filtering_ostream os;
      os.push(utils::packed_sink<count_type, std::allocator<count_type> >(path));
      
      dump(os, counts_modified.begin(), counts_modified.end());
      
      // dump the last order...
      for (size_type pos = index[shard].positin_size(); pos < ngram.counts[shard].size(); ++ pos) {
	const count_type count = ngram.counts[shard][pos];
	os.write((char*) &count, sizeof(count_type));
      }
      
      shard_data.counts.open(path);
    }
  };
  
  void NGramCounts::modify()
  {
    typedef NGramCountsModifyMapReduce map_reduce_type;
    typedef NGramCountsModifyMapper    mapper_type;
    typedef NGramCountsModifyReducer   reducer_type;
    
    typedef map_reduce_type::thread_type        thread_type;
    typedef map_reduce_type::queue_type         queue_type;
    typedef map_reduce_type::queue_ptr_set_type queue_ptr_set_type;
    
    typedef boost::shared_ptr<thread_type>                                 thread_ptr_type;
    typedef std::vector<thread_ptr_type, std::allocator<thread_ptr_type> > thread_ptr_set_type;
    
    // if we already have modified counts, then,  no modification...
    
    if (counts.empty())
      throw std::runtime_error("no indexed counts?");
    
    if (counts_modified.size() == counts.size())
      return;
    
    queue_ptr_set_type  queues(index.size());
    thread_ptr_set_type threads_mapper(index.size());
    thread_ptr_set_type threads_reducer(index.size());

    // first, run reducer...
    counts_modified.clear();
    counts_modified.resize(index.size());
    for (int shard = 0; shard < counts.size(); ++ shard) {
      counts_modified[shard].offset = counts[shard].offset;
      
      queues[shard].reset(new queue_type(1024 * 64));
      threads_reducer[shard].reset(new thread_type(reducer_type(*this, *queues[shard], counts_modified[shard], shard)));
    }
    
    // second, mapper...
    for (int shard = 0; shard < counts.size(); ++ shard)
      threads_mapper[shard].reset(new thread_type(mapper_type(*this, queues, shard)));
    
    // termination...
    for (int shard = 0; shard < counts.size(); ++ shard)
      threads_mapper[shard]->join();
    for (int shard = 0; shard < counts.size(); ++ shard)
      threads_reducer[shard]->join();
  }
  
  struct NGramCountsEstimateDiscountMapper
  {
    typedef std::map<count_type, count_type, std::less<count_type> std::allocator<std::pair<const count_type, count_type> > > count_set_type;
    typedef std::vector<count_set_type, std::allocator<count_set_type> > count_map_type;

    const expgram::NGramCounts& ngram;
    count_map_type& count_of_counts;
    int shard;
    
    NGramCountsEstimateDiscountMapper(const expgram::NGramCounts& _ngram,
				      count_map_type&             _count_of_counts,
				      const int                   _shard)
      : ngram(_ngram),
	count_of_counts(_count_of_counts),
	shard(_shard) {}
    
    void operator()()
    {
      const id_type bos_id = ngram.index.vocab()[vocab_type::BOS];
      const id_type unk_id = ngram.index.vocab()[vocab_type::UNK];
      
      count_of_counts.clear();
      count_of_counts.reserve(ngram.index.order() + 1);
      count_of_counts.resize(ngram.index.order() + 1);
      
      for (int order = (shard == 0 ? 1 : 2); order <= ngram.index.order(); ++ order) {
	const size_type pos_first = ngram.index[shard].offsets[order - 1];
	const size_type pos_last  = ngram.index[shard].offsets[order];
	
	for (size_tyepe pos = pos_first; pos != pos_last; ++ pos) {
	  const count_type count = ngram.counts_modified[shard][pos];
	  if (count > 0)
	    ++ count_of_counts[order][count];
	}
      }
    }
  };

  struct NGramCountsEstimateMapReduce
  {
    
    
  };
  

  struct NGramCountsEstimateMapper
  {
    
    void operator()()
    {
      const id_type bos_id = ngram.index.vocab()[vocab_type::BOS];
      const id_type unk_id = ngram.index.vocab()[vocab_type::UNK];
      
      
    }
  };
  
  
  
  
  void NGramCounts::esimtate(ngram_type& ngram) const
  {
    // collect count of counts...
    
    
    // distouncts...
    
    discount_set_type discounts(index.order() + 1);
    
    for (int order = 1; order <= index.order(); ++ order)
      discounts[order].estimate(count_of_counts[order].begin(), count_of_counts[order].end());
    count_of_counts.clear();
    
    if (debug) {
      for (int order = 1; order <= index.order(); ++ order)
	if (discounts[order].modified)
	  std::cerr << "order: " << order 
		    << " mincount1: " << discounts[order].mincount1
		    << " mincount2: " << discounts[order].mincount2
		    << " mincount3: " << discounts[order].mincount3
		    << " mincount4: " << discounts[order].mincount4
		    << " discount1: " << discounts[order].discount1
		    << " discount2: " << discounts[order].discount2
		    << " discount3plus: " << discounts[order].discount3plus
		    << std::endl;
	else if (discounts[order].discount1 >= 0.0)
	  std::cerr << "order: " << order 
		    << " mincount1: " << discounts[order].mincount1
		    << " mincount2: " << discounts[order].mincount2
		    << " discount1: " << discounts[order].discount1
		    << std::endl;
	else
	  std::cerr << "order: " << order
		    << " witten-bell"
		    << std::endl;
    }
  }
  
  struct NGramCountsDumpMapReduce
  {
    typedef boost::thread                                                  thread_type;
    typedef boost::shared_ptr<thread_type>                                 thread_ptr_type;
    typedef std::vector<thread_ptr_type, std::allocator<thread_ptr_type> > thread_ptr_set_type;
    
    typedef expgram::Word                   word_type;
    typedef expgram::Vocab                  vocab_type;
    typedef word_type::id_type              id_type;
    
    typedef expgram::NGramCounts::count_type      count_type;
    typedef expgram::NGramCounts::size_type       size_type;
    typedef expgram::NGramCounts::difference_type difference_type;
    typedef expgram::NGramCounts::shard_data_type shard_data_type;
    
    typedef boost::filesystem::path         path_type;
    
    typedef std::vector<id_type, std::allocator<id_type> >                 context_type;
    typedef std::pair<id_type, count_type>                                 word_count_type;
    typedef std::vector<word_count_type, std::allocator<word_count_type> > word_set_type;
    typedef std::pair<context_type, word_set_type>                         context_count_type;
    
    typedef utils::lockfree_queue<context_count_type, std::allocator<context_count_type> > queue_type;
    typedef boost::shared_ptr<queue_type>                                                  queue_ptr_type;
    typedef std::vector<queue_ptr_type, std::allocator<queue_ptr_type> >                   queue_ptr_set_type;
  };
  
  inline
  void swap(NGramCountsDumpMapReduce::context_logprob_type& x,
	    NGramCountsDumpMapReduce::context_logprob_type& y)
  {
    x.first.swap(y.first);
    x.second.swap(y.second);
  }
  
  struct NGramCountsDumpMapper
  {
    typedef NGramCountsDumpMapReduce map_reduce_type;
    
    typedef map_reduce_type::id_type    id_type;
    typedef map_reduce_type::size_type  size_type;
    typedef map_reduce_type::count_type count_type;
    
    typedef map_reduce_type::context_type       context_type;
    typedef map_reduce_type::word_set_type      word_set_type;
    typedef map_reduce_type::context_count_type context_count_type;
    
    typedef map_reduce_type::queue_type queue_type;

    const expgram::NGramCounts& ngram;
    queue_type&                 queue;
    int                         shard;
    
    NGramCountsDumpMapper(const expgram::NGramCounts& _ngram,
			  queue_type&                 _queue,
			  const int                   _shard)
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
	    const count_type count(ngram.counts.empty() ? ngram.counts_modified[shard][pos] : ngram.counts[shard][pos]);
	    
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
    
    for (int shard = 0; shard < index.size(); ++ shard) {
      queues[shard].reset(new queue_type(1024 * 64));
      threads[shard].reset(new thread_type(mapper_type(*this, *queues[shard], shard)));
    }
    
    vocab_map_type vocab_map;
    vocab_map.reserve(index[0].offsets[1]);
    
    utils::compress_ostream os(path, 1024 * 1024);
    
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
    for (int shard = 0; shard < index.size(); ++ shard) {
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
    offset = atoll(oiter->second.c_str());
    
    counts.open(rep.path("counts"));
  }
  
  void NGramCounts::ShardData::write(const path_type& file) const
  {
    typedef utils::repository repository_type;
    
    if (path() == file) return;
    
    repository_type rep(file, repository_type::write);

    counts.write(rep.path("counts"));
    
    std::ostringstream stream_offset;
    stream_offset << offset;
    rep["offset"] = stream_offset.str();
  }
  
  template <typename Path, typename Shards>
  inline
  void open_shards(const Path& path, Shards& shards)
  {
    typedef utils::repository repository_type;
    
    repository_type rep(path, repository_type::read);
    
    repository_type::const_iterator siter = rep.find("shard");
    if (siter == rep.end())
      throw std::runtime_error("no shard size...");
    shards.resize(atoi(siter->second.c_str()));
    
    for (int shard = 0; shard < shards.size(); ++ shard) {
      std::ostringstream stream_shard;
      stream_shard << "ngram-" << std::setfill('0') << std::setw(6) << shard;
      
      shards[shard].open(rep.path(stream_shard.str()));
    }
  }
  
  template <typename Path, typename Shards>
  inline
  void write_shards(const Path& path, const Shards& shards)
  {
    typedef utils::repository repository_type;
    
    repository_type rep(path, repository_type::write);
    
    for (int shard = 0; shard < shards.size(); ++ shard) {
      std::ostringstream stream_shard;
      stream_shard << "ngram-" << std::setfill('0') << std::setw(6) << shard;
      
      shards[shard].write(rep.path(stream_shard.str()));
    }
    
    std::ostringstream stream_shard;
    stream_shard << shards.size();
    rep["shard"] = stream_shard.str();
  }
  
  void NGramCounts::open(const path_type& path, const size_type shard_size, const bool unique)
  {
    if (! boost::filesystem::exists(path))
      throw std::runtime_error(std::string("no file: ") + path.file_string());
    
    if (! boost::filesystem::is_directory(path))
      throw std::runtime_error(std::string("invalid path: ") + path.file_string());

    clear();
    
    if (boost::filesystem::exits(path / "prop.list"))
      open_binary(path);
    else
      open_google(path, shard_size, unique);
  }

  void NGramCounts::write(const path_type& file) const
  {
    typedef utils::repository repository_type;
    
    if (path() == file) return;
    
    repository_type rep(file, repository_type::write);
    
    index.write(rep.path("index"));
    if (! counts.empty())
      write_shard(rep.path("count"), counts);
    if (! counts_modified.empty())
      write_shard(rep.path("count-modified"), counts_modified);
  }

  void NGramCounts::open_binary(const path_type& path)
  {
    typedef utils::repository repository_type;
    
    clear();
    
    repository_type rep(path, repository_type::read);
    
    index.open(rep.path("index"));
    if (boost::filesystem::exists(rep.path("count")))
      open_shards(rep.path("count"), counts);
    if (boost::filesystem::exists(rep.path("count-modified")))
      open_shards(rep.path("count-modified"), counts_modified);
  }
  
  static const std::string& __BOS = static_cast<const std::string&>(Vocab::BOS);
  static const std::string& __EOS = static_cast<const std::string&>(Vocab::EOS);
  static const std::string& __UNK = static_cast<const std::string&>(Vocab::UNK);
  
  inline
  NGramCounts::word_type escape_word(const std::string& word)
  {
    if (strcasecmp(word.c_str(), __BOS.c_str()) == 0)
      return Vocab::BOS;
    else if (strcasecmp(word.c_str(), __EOS.c_str()) == 0)
      return Vocab::EOS;
    else if (strcasecmp(word.c_str(), __UNK.c_str()) == 0)
      return Vocab::UNK;
    else
      return word;
  }
  
  struct NGramCountsIndexUniqueMapReduce
  {
    typedef expgram::Word                   word_type;
    typedef expgram::Vocab                  vocab_type;
    typedef word_type::id_type              id_type;
  
    
    typedef expgram::NGramCounts::count_type      count_type;
    typedef expgram::NGramCounts::size_type       size_type;
    typedef expgram::NGramCounts::difference_type difference_type;
    typedef expgram::NGramCounts::shard_data_type shard_data_type;
    
    typedef boost::filesystem::path                            path_type;
    typedef std::vector<path_type, std::allocator<path_type> > path_set_type;
    
  };
  
  // unique-mapper/reducer work like ngram-index-reducer
  
  struct NGramCountsIndexUniqueReducer
  {
    typedef NGramCountsIndexUniqueMapReduce map_reduce_type;
    
    typedef map_reduce_type::thread_type     thread_type;
    typedef map_reduce_type::size_type       size_type;
    typedef map_reduce_type::difference_type difference_type;
    
    typedef map_reduce_type::word_type       word_type;
    typedef map_reduce_type::vocab_type      vocab_type;
    typedef map_reduce_type::id_type         id_type;
    typedef map_reduce_type::path_type       path_type;
    
    typedef map_reduce_type::context_type       context_type;
    typedef map_reduce_type::count_type         count_type;
    typedef map_reduce_type::context_count_type context_count_type;
    
    typedef map_reduce_type::queue_type         queue_type;
    typedef map_reduce_type::ostream_type       ostream_type;
    
    typedef std::pair<id_type, count_type>                                 word_count_type;
    typedef std::vector<word_count_type, std::allocator<word_count_type> > word_count_set_type;
    
    typedef utils::packed_vector<id_type, std::allocator<id_type> >       id_set_type;
    typedef utils::packed_vector<count_type, std::allocator<count_type> > count_set_type;
    typedef std::vector<size_type, std::allocator<size_type> >            size_set_type;
    
    expgram::NGramCounts& ngram;
    queue_type&           queue;
    ostream_type&         os_count;
    int shard;
    
    // thread local...
    id_set_type    ids;
    count_set_type counts;
    size_set_type  positions_first;
    size_set_type  positions_last;
    
    NGramCountsIndexUniqueReducer(expgram::NGram& _ngram,
				  queue_type&     _queue,
				  ostream_type&   _os_count,
				  const int       _shard)
      : ngram(_ngram),
	queue(_queue),
	os_count(_os_count),
	shard(_shard) {}
    
    template <typename Tp>
    struct greater_second_first
    {
      bool operator()(const Tp& x, const Tp& y) const
      {
	return x.second.first > y.second.first;
      }
    };
    
    template <typename Tp>
    struct less_first
    {
      bool operator()(const Tp& x, const Tp& y) const
      {
	return x.first < y.first;
      }
    };
    
    
    template <typename Iterator>
    void dump(std::ostream& os, Iterator first, Iterator last)
    {
      typedef typename std::iterator_traits<Iterator>::value_type value_type;
      os.write((char*) &(*first), (last - first) * sizeof(value_type));
    }
    
    void index_ngram()
    {
      typedef utils::succinct_vector<std::allocator<int32_t> > position_set_type;
      
      const int order_prev = ngram.index[shard].offsets.size() - 1;
      const size_type positions_size = ngram.index[shard].offsets[order_prev] - ngram.index[shard].offsets[order_prev - 1];
      
      const path_type tmp_dir       = utils::tempfile::tmp_dir();
      const path_type path_id       = utils::tempfile::file_name(tmp_dir / "expgram.index.XXXXXX");
      const path_type path_position = utils::tempfile::file_name(tmp_dir / "expgram.position.XXXXXX");
      
      utils::tempfile::insert(path_id);
      utils::tempfile::insert(path_position);
      
      position_set_type positions;
      if (ngram.index[shard].positions.is_open())
	positions = ngram.index[shard].positions;
      
      boost::iostreams::filtering_ostream os_id;
      os_id.push(utils::packed_sink<id_type, std::allocator<id_type> >(path_id));
      
      if (ngram.index[shard].ids.is_open()) {
	for (size_type pos = 0; pos < ngram.index[shard].ids.size(); ++ pos) {
	  const id_type id = ngram.index[shard].ids[pos];
	  os_id.write((char*) &id, sizeof(id_type));
	}
      }
      
      // index id and count...
      ids.build();
      counts.build();
      for (size_type i = 0; i < positions_size; ++ i) {
	const size_type pos_first = positions_first[i];
	const size_type pos_last  = positions_last[i];
	
	for (size_type pos = pos_first; pos != pos_last; ++ pos) {
	  const id_type    id    = ids[pos];
	  const count_type count = counts[pos];
	  
	  os_id.write((char*) &id, sizeof(id_type));
	  os_count.write((char*) &count, sizeof(count_type));
	  positions.set(positions.size(), true);
	}
	positions.set(positions.size(), false);
      }
      
      // perform indexing...
      os_id.pop();
      positions.write(path_position);
      
      // close and remove old index...
      if (ngram.index[shard].ids.is_open()) {
	const path_type path = ngram.index[shard].ids.path();
	ngram.index[shard].ids.close();
	utils::filesystem::remove_all(path);
	utils::tempfile::erase(path);
      }
      if (ngram.index[shard].positions.is_open()) {
	const path_type path = ngram.index[shard].positions.path();
	utils::filesystem::remove_all(path);
	utils::tempfile::erase(path);
      }
      
      // set up offsets
      ngram.index[shard].offsets.push_back(ngram.index[shard].offsets.back() + ids.size());
      
      // new index
      ngram.index[shard].ids.open(path_id);
      ngram.index[shard].positions.open(path_position);

      // remove temporary index
      ids.clear();
      counts.clear();
      positions_first.clear();
      positions_last.clear();
    }
   
    
    void index_ngram(const context_type& prefix, word_count_set_type& words)
    {
      const int order_prev = ngram.index[shard].offsets.size() - 1;
      const size_type positions_size = ngram.index[shard].offsets[order_prev] - ngram.index[shard].offsets[order_prev - 1];
      
      if (positions_first.empty())
	positions_first.resize(positions_size, size_type(0));
      if (positions_last.empty())
	positions_last.resize(positions_size, size_type(0));
      
      std::pair<context_type::const_iterator, size_type> result = ngram.index.traverse(shard, prefix.begin(), prefix.end());
      if (result.first != prefix.end() || result.second == size_type(-1))
	throw std::runtime_error("no prefix?");
      
      const size_type pos = result.second - ngram.index[shard].offsets[prefix.size()];
      positions_first[pos] = ids.size();
      positions_last[pos] = ids.size() + words.size();
      
      std::sort(words.begin(), words.end(), less_first<word_logprob_pair_type>());
      word_logprob_pair_set_type::const_iterator witer_end = words.end();
      for (word_logprob_pair_set_type::const_iterator witer = words.begin(); witer != witer_end; ++ witer) {
	ids.push_back(witer->first);
	counts.push_back(witer->second);
      }
    }

    void operator()()
    {
      context_count_type  context_count;
      context_type        prefix;
      word_count_set_type words;
      
      // we will start from bigram... unigrams...
      int order = 2;
      
      while (1) {
	
	queue.pop(context_count);
	if (context_count.first.empty()) break;
	
	const context_type& context = context_count.first;
	const count_type&   count   = context_count.second;
	
	if (context.size() != prefix.size() + 1 || ! std::equal(prefix.begin(), prefix.end(), context.begin())) {
	  if (! words.empty()) {
	    index_ngram(prefix, words);
	    words.clear();
	    
	    if (context.size() != order)
	      index_ngram();
	  }
	  
	  prefix.clear();
	  prefix.insert(prefix.end(), context.begin(), context.end() - 1);
	  order = context.size();
	}
	
	words.push_back(std::make_pair(context.back(), count));
      }
      
      // perform final indexing...
      if (! words.empty()) {
	index_ngram(prefix, words);
	index_ngram();
      }
    }
  };

  struct NGramCountsIndexMapReduce
  {
    typedef expgram::Word                   word_type;
    typedef expgram::Vocab                  vocab_type;
    typedef word_type::id_type              id_type;
  
    
    typedef expgram::NGramCounts::count_type      count_type;
    typedef expgram::NGramCounts::size_type       size_type;
    typedef expgram::NGramCounts::difference_type difference_type;
    typedef expgram::NGramCounts::shard_data_type shard_data_type;
    
    typedef boost::filesystem::path                            path_type;
    typedef std::vector<path_type, std::allocator<path_type> > path_set_type;
    
  };

  struct NGramCountsIndexMapper
  {
    
    void operator()()
    {
      
      std::vector<istream_ptr_type, std::allocator<istream_ptr_type> > streams(paths.size());
      
      for (int i = 0; i < paths.size(); ++ i) {
	strams[i].reset(new utils::compress_istream(paths[i], 1024 * 1024));
	
	while (std::getline(*streams[i], line)) {
	  tokenizer_type tokenizer(line);
	  tokens.clear();
	  tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
	  if (tokens.size() != order + 1)
	    continue;
	  
	  context_count_stream_ptr_type context_stream(new context_count_stream_type());
	  context_stream->first = ngram_context_type(tokens.begin(), tokens.end() - 1);
	  context_stream->second.first  = atoll(tokens.back().c_str());
	  context_stream->second.second = &(*streams[i]);
	  
	  pqueue.push(context_stream);
	  break;
	}
      }
      
      int ngram_shard = 0;
      ngram_context_type prefix_shard;
      ngram_context_type context;

      while (! pqueue.empty()) {
	context_count_stream_ptr_type context_stream(pqueue.top());
	pqueue.pop();
	
	if (context != context_stream->first) {
	  if (count > 0) {
	    if (order == 2)
	      ngram_shard = ngram.index.shard_index(context.begin(), context.end());
	    else if (prefix_shard.empty() || ! std::equal(prefix_shard.begin(), prefix_shard.end(), context.begin())) {
	      ngram_shard = ngram.index.shard_index(context.begin(), context.end());
	      
	      prefix_shard.clear();
	      prefix_shard.insert(prefix_shard.end(), context.begin(), context.begin() + 2);
	    }
	    
	    queues[ngram_shard]->push(std::make_pair(context, count));
	  }
	  
	  context.swap(context_stream->first);
	  count = 0;
	}
	
	count += context_stream->second.first;
	
	while (std::getline(*(context_stream->second.second), line)) {
	  tokenizer_type tokenizer(line);
	  
	  tokens.clear();
	  tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
	  if (tokens.size() != order + 1)
	    continue;
	  
	  context_stream->first.clear();
	  context_stream->first.insert(context_stream->first.end(), tokens.begin(), tokens.end() - 1);
	  context_stream->second.first  = atoll(tokens.back().c_str());
	  
	  pqueue.push(context_stream);
	  break;
	}
      }
      
      if (count > 0) {
	if (order == 2)
	  ngram_shard = ngram.index.shard_index(context.begin(), context.end());
	else if (prefix_shard.empty() || ! std::equal(prefix_shard.begin(), prefix_shard.end(), context.begin())) {
	  ngram_shard = ngram.index.shard_index(context.begin(), context.end());
	      
	  prefix_shard.clear();
	  prefix_shard.insert(prefix_shard.end(), context.begin(), context.begin() + 2);
	}
	
	queues[ngram_shard]->push(std::make_pair(context, count));
      }
    }
  };
  
  struct NGramCountsIndexReducer
  {
    
    
    void index_ngram(const context_type& prefix, const word_cont_set_tyep& words)
    {
      const int order_prev = ngram.index[shard].offsets.size() - 1;
      const size_type positions_size = ngram.index[shard].offsets[order_prev] - ngram.index[shard].offsets[order_prev - 1];
      
      if (positions_first.empty())
	positions_first.resize(positions_size, size_type(0));
      if (positions_last.empty())
	positions_last.resize(positions_size, size_type(0));
      
      
    }
    
    void operator()()
    {
      context_count_type context_count;
      
      for (int shard = 0; shard < queues.size(); ++ shard) {
	queues[shard]->pop(context_count);

	if (! context_count.first.empty()) {
	  contxt_count_queue_ptr_type context_queue(new context_count_queue_type());
	  context_queue->first.swap(context_count.first);
	  context_queue->second.first = context_count.second;
	  context_queue->second.second = &(*queues[shard]);
	  
	  pqueue.push(context_queue);
	}
      }
      
      context_type        context;
      context_type        prefix;
      word_count_set_type words;

      int order = 2;
      
      while (! pqueue.empty()) {
	context_count_queue_ptr_type context_queue(pqueue.top());
	pqueue.pop();
	
	context.clear();
	ngram_context_type::const_iterator niter_end = context_queue->first.end();
	for (ngram_context_type::const_iterator niter = context_queue->first.begin(); niter != niter_end; ++ niter) {
	  const id_type id = escape_word(*niter).id();
	  
	  if (id >= vocab_map.size() || vocab_map[id] == id_type(-1))
	    throw std::runtime_error("invalid vocbulary");
	  
	  context.push_back(vocab_map[id]);
	}
	
	if (prefix.size() + 1 != context.size() || ! std::equal(prefix.begin(), prefix.end(), context.begin())) {
	  
	  if (! words.empty()) {
	    index_ngram(prefix, words);
	    words.clear();
	  }
	  
	  prefix.cler();
	  prefix.insert(prefix.end(), context.begin(), context.end() - 1);
	  order = context.size();
	}
	
	if (words.empty() || words.back().first != context.back())
	  words.push_back(std::make_pair(context.back(), context_queue->second.first));
	else
	  words.back().second += context_queue->second.first;
	
	context_queue->second.second->pop_swap(context_logprob);
	if (! context_logprob.first.empty()) {
	  context_queue->first.swap(context_logprob.first);
	  context_queue->second.fist = context_logprob.second;
	  
	  pqueue.push(context_queue);
	}
      }
      
      if (! words.empty()) {
	index_ngram(prefix, words);
	words.clear();
      } 
      
      
      // perform final indexing...
      
    }
  };
  
  void NGramCounts::open_google(const path_type& path,
				const size_type shard_size=16,
				const bool unique=false)
  {
    // first, index unigram...
    
    
    
    // second, ngrams...
    if (unique) {
      
      for (int order = 2; /**/; ++ order) {
	std::ostringstream stream_ngram;
	stream_ngram << n << "gms";
	
	std::ostringstream stream_index;
	stream_index << n << "gm.idx";
	
	const path_type ngram_dir = path / stream_ngram.str();
	const path_type index_file = ngram_dir / stream_index.str();
	
	if (! boost::filesystem::exists(ngram_dir) || ! boost::filesystem::exists(index_file)) break;
	
	utils::compress_istream is_index(index_file);
	std::string line;
	tokens_type tokens;
	while (std::getline(is_index, line)) {
	  tokenizer_type tokenizer(line);
	  
	  tokens.clear();
	  tokens(tokens.end(), tokenizer.begin(), tokenizer.end());
	  
	  if (tokens.size() + 1 != order)
	    throw std::runtime_error(std::string("invalid google ngram format...") + index_file.file_string());
	  
	  const path_type path_ngram = ngram_dir / tokens.front();
	  
	  if (! boost::filesystem::exists(path_ngram))
	    throw std::runtime_error(std::string("invalid google ngram format... no file: ") + path_ngram.file_string());
	  
	  
	  utils::compress_istream is(path_ngram, 1024 * 1024);
	  
	  while (std::getline(is, line)) {
	    tokenizer_type tokenier(line);
	    
	    tokens.clear();
	    tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
	    
	    // invalid ngram...?
	    if (tokens.size() != order + 1)
	      continue;
	    
	    context.clear();
	    tokens_type::const_iterator titer_end = tokens.end() - 1;
	    for (tokens_type::const_iterator titer = tokens.begin(); titer != titer_end; ++ titer) {
	      const id_type id = escape_word(*titer).id();
	      
	      if (id >= vocab_map.size() || vocab_map[id] == id_type(-1))
		throw std::runtime_error("invalid vocbulary");
	      
	      context.push_back(vocab_map[id]);
	    }
	    
	    const size_type shard = index.shard_index(context.begin(), context.end());
	    queue[shard]->push(std::make_pair(context, atoll(tokens.back().c_str())));
	  }	  
	}
      }
      
      // termination...
      for (int shard = 0; shard < index.size(); ++ shard)
	queues[shard]->push(std::make_pair(context_type(), count_type(0)));
      
      for (int shard = 0; shard < index.size(); ++ shard) {
	threads[shard]->join();
	
	os_counts[shard].reset();
	counts[shard].open(path_countss[shard]);
      }
      threads.clear();
    } else {
      // we need to map/reduce counts, since counts are not unique!
      
      for (int order = 2; /**/; ++ order) {
	std::ostringstream stream_ngram;
	stream_ngram << n << "gms";
	
	std::ostringstream stream_index;
	stream_index << n << "gm.idx";
	
	const path_type ngram_dir = path / stream_ngram.str();
	const path_type index_file = ngram_dir / stream_index.str();
	
	if (! boost::filesystem::exists(ngram_dir) || ! boost::filesystem::exists(index_file)) break;
	
	path_set_type paths_ngram;
	{
	  utils::compress_istream is_index(index_file);
	  std::string line;
	  tokens_type tokens;
	  while (std::getline(is_index, line)) {
	    tokenizer_type tokenizer(line);
	    
	    tokens.clear();
	    tokens(tokens.end(), tokenizer.begin(), tokenizer.end());
	    
	    if (tokens.size() + 1 != order)
	      throw std::runtime_error(std::string("invalid google ngram format...") + index_file.file_string());
	    
	    const path_type path_ngram = ngram_dir / tokens.front();
	    
	    if (! boost::filesystem::exists(path_ngram))
	      throw std::runtime_error(std::string("invalid google ngram format... no file: ") + path_ngram.file_string());
	    
	    paths_ngram.push_back(path_ngram);
	  }
	}
	
	// no files???
	if (paths_ngram.empty()) break;
	
	// run-mapper...
	
	
	
	// run-reducer...
	
	
	
	// wait...
	for (int shard = 0; shard < index.size(); ++ shard)
	  mappers[shard]->join();
	
	for (int shard = 0; shard < index.size(); ++ shard)
	  reducers[shard]->join();
      }

      // terminated!
      for (int shard = 0; shard < index.size(); ++ shard) {
	os_counts[shard].reset();
	counts[shard].open(path_countss[shard]);
      }
    }
    
  }
  
};
