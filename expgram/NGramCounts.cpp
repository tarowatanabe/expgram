
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
  
  struct NGramCountsEstimateMapReduce
  {
    
    
  };
  
  
  void NGramCounts::esimtate(ngram_type& ngram) const
  {
    
    
    
  }
  
  
  
  void NGramCounts::dump(const path_type& path) const
  {
    
    
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
    
    repository_type rep(file, repository_type::read);
    
    utils::filesystem::copy_files(counts.path(), rep.path("counts"));
    
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
  NGram::word_type escape_word(const std::string& word)
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


  struct NGramCountsIndexMapper
  {
    
    
  };
  
  struct NGramCountsIndexReducer
  {
    
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
      
      while (! pqueue.empty()) {
	context_count_queue_ptr_type context_queue(pqueue.top());
	pqueue.pop();
	
	if () {
	  
	}
	
	context_queue->second.second->pop_swap(context_logprob);
	if (! context_logprob.first.empty()) {
	  context_queue->first.swap(context_logprob.first);
	  context_queue->second.fist = context_logprob.second;
	  
	  pqueue.push(context_queue);
	}
      }
      
      
    }
  };
  
  void NGramCounts::open_google(const path_type& path,
				const size_type shard_size=16,
				const bool unique=false)
  {
    
    // first, index unigram...
    
    //
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
