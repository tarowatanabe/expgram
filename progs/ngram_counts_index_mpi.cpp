
#include <expgram/NGramCounts.hpp>
#include <expgram/NGramCountsIndexer.hpp>

#include <utils/mpi.hpp>
#include <utils/mpi_device.hpp>
#include <utils/mpi_device_bcast.hpp>

int main(int argc, char** argv)
{
  utils::mpi_world mpi_world(argc, argv);
  
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  try {
    
    if (MPI::Comm::Get_parent() != MPI::COMM_NULL) {
      // child-prcess...
      
      utils::mpi_intercomm comm_parent(MPI::Comm::Get_parent());
      
      if (getoptions(argc, argv) != 0) 
	return 1;
      
    } else {
      
      std::vector<const char*, std::allocator<const char*> > args;
      args.reserve(argc);
      for (int i = 1; i < argc; ++ i)
	args.push_back(argv[i]);
      args.push_back(0);
      
      // getoptions...
      if (getoptions(argc, argv) != 0) 
	return 1;
      
      if (unique) {
	
	
	
      } else {
	const std::string name = (boost::filesystem::exists(prog_name) ? prog_name.file_string() : std::string(argv[0]));
	utils::mpi_intercomm comm_child(MPI::COMM_WORLD.Spawn(name.c_str(), &(*args.begin()), mpi_size, MPI::INFO_NULL, 0));
	
	
      }
    }
  }
}
  
inline
word_type escape_word(const std::string& word)
{
  static const std::string& __BOS = static_cast<const std::string&>(Vocab::BOS);
  static const std::string& __EOS = static_cast<const std::string&>(Vocab::EOS);
  static const std::string& __UNK = static_cast<const std::string&>(Vocab::UNK);
  
  if (strcasecmp(word.c_str(), __BOS.c_str()) == 0)
    return Vocab::BOS;
  else if (strcasecmp(word.c_str(), __EOS.c_str()) == 0)
    return Vocab::EOS;
  else if (strcasecmp(word.c_str(), __UNK.c_str()) == 0)
    return Vocab::UNK;
  else
    return word;
}

void index_unigram(const path_type& path, const path_type& output, ngram_type& ngram, shard_data_type& shard)
{
  typedef utils::repository repository_type;

  typedef ngram_type::count_type count_type;
  typedef ngram_type::id_type    id_type;
  
  typedef std::vector<std::string, std::allocator<std::string> > tokens_type;
  typedef boost::tokenizer<utils::space_separator>               tokenizer_type;
  
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();

  ngram.index.reserve(mpi_size);
  ngram.counts.reserve(mpi_size);
  
  ngram.index.resize(mpi_size);
  ngram.counts.resize(mpi_size);
  
  if (mpi_rank == 0) {
    
    typedef std::vector<word_type, std::allocator<word_type> > word_set_type;

    {
      // first, create output direcories...
      repository_type repository(output, repository_type::write);
      repository_type rep_index(repository.path("index"), repository_type::write);
      repository_type rep_count(repository.path("count"), repository_type::write);
      
      std::ostringstream stream_shard;
      stream_shard << mpi_size;
      rep_index["shard"] = stream_shard.str();
      rep_count["shard"] = stream_shard.str();
    }

    repository_type repository(output, repository_type::read);
    repository_type rep(repository.path("index"), repository_type::read);
    
    const path_type unigram_dir = path / "1gms";
    const path_type vocab_file = unigram_dir / "vocab.gz";
    const path_type vocab_sorted_file = unigram_dir / "vocab_cs.gz";

    id_type word_id = 0;
    
    utils::compress_istream is(vocab_sorted_file, 1024 * 1024);
    
    word_set_type words;

    std::string line;
    tokens_type tokens;
    while (std::getline(is, line)) {
      tokenizer_type tokenizer(line);
      tokens.clear();
      tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
      
      if (tokens.size() != 2) continue;
      
      const word_type word = escape_word(tokens.front());
      const count_type count = atoll(tokens.back().c_str());
      
      shard.os_counts->write((char*) &count, sizeof(count_type));
      
      words.push_back(word);
      
      ++ word_id;
    }
    
    vocab_type& vocab = index.vocab();
    vocab.open(rep.path("vocab"), 1024 * 1024 * 16);
    
    word_set_type::const_iterator witer_end = words.end();
    for (word_set_type::const_iterator witer = words.begin(); witer != witer_end; ++ witer)
      vocab.insert(*witer);
    
    words.clear();
    word_set_type(words).swap(words);
    
    vocab.close();
    vocab.open(rep.path("vocab"));
    
    int unigram_size = word_id;
    MPI::COMM_WORLD.Bcast(&unigram_size, 1, MPI::INT, 0);
    
    if (debug)
      std::cerr << "\t1-gram size: " << unigram_size << std::endl;
    
    ngram.index[0].offsets.clear();
    ngram.index[0].offsets.push_back(0);
    ngram.index[0].offsets.push_back(unigram_size);
    
    ngram.counts[0].offset = 0;
  } else {
    int unigram_size = 0;
    MPI::COMM_WORLD.Bcast(&unigram_size, 1, MPI::INT, 0);
    
    ngram.index[mpi_rank].offsets.clear();
    ngram.index[mpi_rank].offsets.push_back(0);
    ngram.index[mpi_rank].offsets.push_back(unigram_size);
    
    ngram.counts[mpi_rank].offset = unigram_size;
    
    while (! boost::filesystem::exists(output))
      boost::thread::yield();
    repository_type repository(output, repository_type::read);
    
    while (! boost::filesystem::exists(repository.path("index")))
      boost::thread::yield();
    repository_type rep(repository.path("index"), repository_type::read);
    
    while (! boost::filesystem::exists(rep.path("vocab")))
      boost::thread::yield();
    
    vocab_type& vocab = index.vocab();
    vocab.open(rep.path("vocab"));
  }
}


struct VocabMap
{
  typedef std::vector<id_type, std::allocator<id_type> > cache_type;
  
  id_type operator[](const word_type& word)
  {
    if (word.id() >= cache.size())
      cache.resize(word.id() + 1, id_type(-1));
    if (cache[word.id()] == id_type(-1))
      cache[word.id()] = vocab[word];
    return cache[word.id()];
  }
  
  vocab_type& vocab;
  cache_type  cache;
};

template <typename Tp>
struct greater_pfirst
{
  bool operator()(const Tp* x, const Tp* y) const
  {
    return x->first > y->first;
  }
      
  bool operator()(const boost::shared_ptr<Tp>& x, const boost::shared_ptr<Tp>& y) const
  {
    return x->first > y->first;
  }
};

template <typename NGram, typename VocabMap, typename Context>
int shard_index(const NGram& ngram, VocabMap& vocab_map, const Context& context)
{
  id_type id[2];
  id[0] = vocab_map[escape_word(context[0])];
  id[1] = vocab_map[escape_word(context[1])];
  
  return ngram.index.shard_index(id, id + 2);;
}

template <typename PathSet, typename VocabMap>
void index_ngram_mapper(intercomm_type& reducer, const PathSet& paths, ngram_type& ngram, VocabMap& vocab_map)
{
  typedef boost::iostreams::filtering_istream istream_type;
  typedef boost::iostreams::filtering_ostream ostream_type;
  
  typedef utils::mpi_device_source            idevice_type;
  typedef utils::mpi_device_sink              odevice_type;

  typedef boost::shared_ptr<istream_type> istream_ptr_type;
  typedef boost::shared_ptr<ostream_type> ostream_ptr_type;
  
  typedef boost::shared_ptr<idevice_type> idevice_ptr_type;
  typedef boost::shared_ptr<odevice_type> odevice_ptr_type;
  
  typedef std::vector<istream_ptr_type, std::allocator<istream_ptr_type> > istream_ptr_set_type;
  typedef std::vector<ostream_ptr_type, std::allocator<ostream_ptr_type> > ostream_ptr_set_type;
  
  typedef std::vector<idevice_ptr_type, std::allocator<idevice_ptr_type> > idevice_ptr_set_type;
  typedef std::vector<odevice_ptr_type, std::allocator<odevice_ptr_type> > odevice_ptr_set_type;
  
  typedef std::pair<count_type, istream_type*>                   count_stream_type;
  typedef std::vector<std::string, std::allocator<std::string> > ngram_context_type;
  typedef std::pair<ngram_context_type, count_stream_type>       context_count_stream_type;
  typedef boost::shared_ptr<context_count_stream_type>           context_count_stream_ptr_type;
  
  typedef std::vector<context_count_stream_ptr_type, std::allocator<context_count_stream_ptr_type> > pqueue_base_type;
  typedef std::priority_queue<context_count_stream_ptr_type, pqueue_base_type, greater_pfirst<context_count_stream_type> > pqueue_type;

  typedef std::vector<std::string, std::allocator<std::string> > tokens_type;
  typedef boost::tokenizer<utils::space_separator>               tokenizer_type;

  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  ostream_ptr_set_type stream(mpi_size);
  odevice_ptr_set_type device(mpi_size);
  
  for (int rank = 0; rank < mpi_size; ++ rank) {
    stream[rank].reset(new ostream_type());
    device[rank].reset(new odevice_type(reducer.comm, rank, count_tag, 1024 * 1024, false, true));
    stream[rank]->push(boost::iostreams::gzip_compressor());
    stream[rank]->push(*device[rank]);
  }

  pqueue_type pqueue;
  
  istream_ptr_set_type istreams;
  istreams.reserve(paths.size());
  
  std::string line;
  tokens_type tokens;
  
  for (int i = 0; i < paths.size(); ++ i) {
    
    istreams.push_back(new utils::compress_istream(paths[i], 1024 * 1024));
    
    while (std::getline(*istreams.back(), line)) {
      tokenizer_type tokenizer(line);
      tokens.clear();
      tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
      
      if (tokens.size() < 2) continue;
      
      context_count_stream_ptr_type context_stream(new context_count_stream_type());
      context_stream->first.clear();
      context_stream->first.inset(context_stream->first.end(), tokens.begin(), tokens.end() - 1);
      context_stream->second.first = atoll(tokens.back().c_str());
      context_stream->scond.second = &(*istreams.back());
      
      pqueue.push(context_stream);
      break;
    }
  }
  
  ngram_context_type context;
  count_type         count = 0;
  
  ngram_context_type prefix_shard;
  int                ngram_shard = 0;
  
  const size_t iteration_mask = (1 << 13) - 1;
  for (size_t iteration = 0; ! pqueue.empty(); ++ iteration) {
    context_count_stream_ptr_type context_stream(pqueue.top());
    pqueue.pop();
    
    if (context != context_stram->first) {
      if (count > 0) {
	
	if (context.size() == 2)
	  ngram_shard = shard_index(ngram, vocab_map, context);
	else if (prefix_shard.empty() || ! std::equal(prefix_shard.begin(), prefix_shard.end(), context.begin())) {
	  ngram_shard = shard_index(ngram, vocab_map, context);
	  
	  prefix_shard.clear();
	  prefix_shard.insert(prefix_shard.end(), context.begin(), context.begin() + 2);
	}
	
	std::copy(context.begin(), context.end(), std::ostream_iterator<std::string>(*stream[ngram_shard], " "));
	*stream[ngram_shard] << count << '\n';
      }
      
      context.swap(context_stream->first);
      count = 0;
    }
    
    count += count_stream->second.first;
    
    while (std::getline(*(context_stream->second.second), line)) {
      tokenizer_type tokenizer(line);
      tokens.clear();
      tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
      
      if (tokens.size() < 2) continue;
      
      context_stream->first.clear();
      context_stream->first.inset(context_stream->first.end(), tokens.begin(), tokens.end() - 1);
      context_stream->second.first = atoll(tokens.back().c_str());
      
      pqueue.push(context_stream);
      break;
    }
    
    if ((iteration & iteration_mask) == iteration_mask && mpi_flush_devices(stream, device))
      boost::thread::yield();
  }
  
  istreams.clear();
  
  for (int rank = 0; rank < mpi_size; ++ rank) {
    *stream[rank] << '\n';
    stream[rank].reset();
  }
  
  for (;;) {
    if (std::count(device.begin(), device.end(), odevice_ptr_type()) == mpi_size) break;
    
    if (! utils::mpi_terminate_devices(stream, device))
      boost::thread::yield();
  }
}

void index_ngram_mapper_root(intercomm_type& reducer, const path_type& path, ngram_type& ngram)
{
  typedef boost::iostreams::filtering_istream istream_type;
  typedef boost::iostreams::filtering_ostream ostream_type;
  
  typedef utils::mpi_device_source            idevice_type;
  typedef utils::mpi_device_sink              odevice_type;

  typedef boost::shared_ptr<istream_type> istream_ptr_type;
  typedef boost::shared_ptr<ostream_type> ostream_ptr_type;
  
  typedef boost::shared_ptr<idevice_type> idevice_ptr_type;
  typedef boost::shared_ptr<odevice_type> odevice_ptr_type;
  
  typedef std::vector<istream_ptr_type, std::allocator<istream_ptr_type> > istream_ptr_set_type;
  typedef std::vector<ostream_ptr_type, std::allocator<ostream_ptr_type> > ostream_ptr_set_type;
  
  typedef std::vector<idevice_ptr_type, std::allocator<idevice_ptr_type> > idevice_ptr_set_type;
  typedef std::vector<odevice_ptr_type, std::allocator<odevice_ptr_type> > odevice_ptr_set_type;
  
  typedef std::vector<path_type, std::allocator<path_type> >     path_set_type;
  typedef std::vector<std::string, std::allocator<std::string> > tokens_type;
  typedef boost::tokenizer<utils::space_separator>               tokenizer_type;
  
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  for (int order = 2; /**/; ++ order) {
    std::ostringstream stream_ngram;
    stream_ngram << order << "gms";
    
    std::ostringstream stream_index;
    stream_index << order << "gm.idx";
    
    const path_type ngram_dir = path / stream_ngram.str();
    const path_type index_file = ngram_dir / stream_index.str();

    path_set_type paths_ngram;
    if (boost::filesystem::exists(ngram_dir) && boost::filesystem::exists(index_file)) {
      utils::compress_istream is_index(index_file);
      std::string line;
      tokens_type tokens;
      while (std::getline(is_index, line)) {
	tokenizer_type tokenizer(line);
	tokens.clear();
	tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
	
	if (tokens.empty()) continue;
	if (tokens.size() != order + 1)
	  throw std::runtime_error(std::string("invalid google ngram format...") + index_file.file_string());

	const path_type path_ngram = ngram_dir / tokens.front();
	
	if (! boost::filesystem::exists(path_ngram))
	  throw std::runtime_error(std::string("invalid google ngram format... no file: ") + path_ngram.file_string());
	
	paths_ngram.push_back(path_ngram);
      }
    }
    
    int count_file_size = paths_ngram.size();
    
    // notify reducers
    reducer.comm.Send(&count_file_size, 1, MPI::INT, 0, size_tag);
    
    // notify mappers
    MPI::COMM_WORLD.Bcast(&count_file_size, 1, MPI::INT, 0);
    
    if (count_file_size == 0) break;
    
    if (debug)
      std::cerr << "order: " << order << std::endl;
    
    // distribute files...
    ostream_ptr_set_type stream(mpi_size);
    for (int rank = 1; rank < mpi_size; ++ rank) {
      stream[rank].reset(new ostream_type());
      stream[rank]->push(boost::iostreams::gzip_compressor());
      stream[rank]->push(utils::mpi_device_sink(MPI::COMM_WORLD, rank, file_tag, 1024 * 4));
    }
    path_set_type paths_map;
    for (int i = 0; i < paths_ngram.size(); ++ i) {
      if (i % mpi_size == mpi_rank)
	paths_map.push_back(paths_ngram[i]);
      else
	*stream[rank] << paths_ngram[i].file_string() << '\n';
    }
    for (int rank = 1; rank < mpi_size; ++ rank) {
      *stream[rank] << '\n';
      stream[rank].reset();
    }
    
    index_ngram_mapper(reducer, paths_map, ngram, vocab_map);
  }
}

void index_ngram_mapper_others(intercomm_type& reducer, const path_type& path, ngram_type& ngram)
{
  typedef std::vector<path_type, std::allocator<path_type> >     path_set_type;
  
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  for (int order = 2; /**/; ++ order) {
    int count_file_size = 0;
    MPI::COMM_WORLD.Bcast(&count_file_size, 1, MPI::INT, 0);
    
    if (coun_file_size == 0) break;
    
    path_set_type paths_map;
    boost::iostreams::filtering_istream stream;
    stream.push(boost::iostreams::gzip_decompressor());
    stream.push(utils::mpi_device_source(MPI::COMM_WORLD, 0, file_tag, 1024 * 4));
    
    std::string line;
    while (std::getline(stream, line))
      if (! line.empty()) {
	if (! boost::filesystem::exists(line))
	  throw std::runtime_error(std::string("no file? ") + line);
	paths_map.push_back(line);
      }
    
    index_ngram_mapper(reducer, paths_map, ngram, vocab_map);
  }
}

struct IndexNGramMapReduce
{
  typedef std::vector<id_type, std::allocator<id_type> > context_type;
  typedef std::pair<context_type, count_type> context_count_type;
  
  typedef utils::lockfree_queue<context_count_type, std::allocator<context_count_type> > queue_type;
  typedef boost::shared_ptr<queue_type>                                                  queue_ptr_type;
  typedef std::vector<queue_ptr_type, std::allocator<queue_ptr_type> >                   queue_ptr_set_type;
  
  typedef boost::thread                                                  thread_type;
  typedef boost::shared_ptr<thread_type>                                 thread_ptr_type;
  typedef std::vector<thread_ptr_type, std::allocator<thread_ptr_type> > thread_ptr_set_type;
  
  typedef boost::iostreams::filtering_ostream                            ostream_type;
};

inline
void swap(IndexNGramMapReduce::context_count_type& x,
	  IndexNGramMapReduce::context_count_type& y)
{
  using namespace std;
  using namespace boost;
  
  x.first.swap(y.first);
  swap(x.second, y.second);
}

struct IndexNGramReducer
{
  typedef IndexNGramMapReduce map_reduce_type;
  
  typedef map_reduce_type::context_type       context_type;
  typedef map_reduce_tyep::context_count_type context_count_type;
  
  typedef map_reduce_type::queue_type   queue_type;
  typedef map_reduce_type::ostream_type ostream_type;

  typedef expgram::NGramCountsIndexer<ngram_type> indexer_type;
  
  ngram_type&   ngram;
  queue_type&   queue;
  ostream_type& os_count;
  int           shard;
  int           debug;
  
  IndexNGramReducer(ngram_type&           _ngram,
		    queue_type&           _queue,
		    ostream_type&         _os_count,
		    const int             _shard,
		    const int             _debug)
    : ngram(_ngram),
      queue(_queue),
      os_count(_os_count),
      shard(_shard),
      debug(_debug) {}
  
  void operator()()
  {
    indexer_type indexer;
    
    context_count_type  context_count;
    context_type        prefix;
    word_count_set_type words;

    map_reduce_type shard_data;
    
    int order = 0;
    
    while (1) {
      queue.pop_swap(context_count);
      if (context_count.first.empty()) break;
      
      const context_type& context = context_count.first;
      const count_type&   count   = context_count.second;
      
      if (context.size() != prefix.size() + 1 || ! std::equal(prefix.begin(), prefix.end(), context.begin())) {
	
	if (! words.empty()) {
	  indexer(shard, ngram, prefix, words);
	  words.clear();
	  
	  if (context.size() != order)
	    indexer(shard, ngram, os_count, debug);
	}
	
	prefix.clear();
	prefix.insert(prefix.end(), context.begin(), context.end() - 1);
	order = context.size();
      }
      
      words.push_back(std::make_pair(context.back(), count));
    }
    
    // perform final indexing...
    if (! words.empty()) {
      indexer(shard, ngram, prefix, words);
      indexer(shard, ngram, os_count, debug);
    }
  }
  
};

template <typename ShardData, typename VocabMap>
void index_ngram_reducer(intercomm_type& mapper, ngram_type& ngram, ShardData& shard, VocabMap& vocab_map)
{
  typedef boost::iostreams::filtering_istream istream_type;
  typedef utils::mpi_device_source            idevice_type;
  
  typedef boost::shared_ptr<istream_type> istream_ptr_type;
  typedef boost::shared_ptr<idevice_type> idevice_ptr_type;
  
  typedef std::vector<istream_ptr_type, std::allocator<istream_ptr_type> > istream_ptr_set_type;
  typedef std::vector<idevice_ptr_type, std::allocator<idevice_ptr_type> > idevice_ptr_set_type;
  
  typedef std::pair<count_type, istream_type*> count_stream_type;
  typedef std::vector<std::string, std::allocator<std::string> > ngram_context_type;
  typedef std::pair<ngram_context_type, count_stream_type>       context_count_stream_type;
  typedef boost::shared_ptr<context_count_stream_type>           context_count_stream_ptr_type;
  
  typedef std::vector<context_count_stream_ptr_type, std::allocator<context_count_stream_ptr_type> > pqueue_base_type;
  typedef std::priority_queue<context_count_stream_ptr_type, pqueue_base_type, greater_pfirst<context_count_stream_type> > pqueue_type;
  
  typedef std::vector<std::string, std::allocator<std::string> > tokens_type;
  typedef boost::tokenizer<utils::space_separator>               tokenizer_type;

  typedef std::vector<id_type, std::allocator<id_type> > context_type;
  typedef std::pair<id_type, count_type> word_count_type;
  typedef std::vector<word_count_type, std::allocator<word_count_type> > word_count_set_type;
  
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  istream_ptr_type stream(mpi_size);
  idevice_ptr_type device(mpi_size);

  pqueue_type pqueue;
  
  std::string line;
  tokens_type tokens;
  
  for (int rank = 0; rank < mpi_size; ++ rank) {
    stream[rank].reset(new istream_type());
    device[rank].reset(new idevice_type(mapper.comm, rank, count_tag, 1024 * 1024));
    
    stream[rank]->push(boost::iostreams::gzip_decompressor());
    stream[rank]->push(*device[rank]);
    
    while (std::getline(*stream[rank], line)) {
      tokenizer_type tokenizer(line);
      tokens.clear();
      tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
      
      if (tokens.size() < 2) continue;
      
      context_count_stream_ptr_type context_stream(new context_count_stream_type());
      context_stream->first.clear();
      context_stream->first.inset(context_stream->first.end(), tokens.begin(), tokens.end() - 1);
      context_stream->second.first = atoll(tokens.back().c_str());
      context_stream->scond.second = &(*istreams.back());
      
      pqueue.push(context_stream);
      break;
    }
  }
  
  context_type        context;
  context_type        prefix;
  word_count_set_type words;
  
  const size_t iteration_mask = (1 << 13) - 1;
  for (size_t iteration = 0; ! pqueue.empty(); ++ iteration) {
    context_count_stream_ptr_type context_stream(pqueue.top());
    pqueue.pop();
    
    context.clear();
    ngram_context_type::const_iterator niter_end = context_queue->first.end();
    for (ngram_context_type::const_iterator niter = context_queue->first.begin(); niter != niter_end; ++ niter)
      context.push_back(vocab_map[escape_word(*niter)]);
    
  }
  
  
  
  
}

void index_ngram_unique(const path_type& path, ngram_type& ngram, shard_data_type& shard)
{
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  //
  // run a thread....
  //

  for (int order = 2; /**/; ++ order) {
    
    if (mpi_rank == 0 && debug)
      std::cerr << "order: " << order << std::endl;
    
    std::ostringstream stream_ngram;
    stream_ngram << order << "gms";
    
    std::ostringstream stream_index;
    stream_index << order << "gm.idx";
    
    const path_type ngram_dir = path / stream_ngram.str();
    const path_type index_file = ngram_dir / stream_index.str();
    
    if (! boost::filesystem::exists(ngram_dir) || ! boost::filesystem::exists(index_file)) break;


    ngram_context_type ngram_prefix;
    int                ngram_rank = 0;
    
    utils::compress_istream is_index(index_file);
    std::string line;
    tokens_type tokens;
    context_type context;
    while (std::getline(is_index, line)) {
      tokenizer_type tokenizer(line);
      
      tokens.clear();
      tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
      
      if (tokens.size() != order + 1)
	throw std::runtime_error(std::string("invalid google ngram format...") + index_file.file_string());
      
      const path_type path_ngram = ngram_dir / tokens.front();
      
      if (! boost::filesystem::exists(path_ngram))
	throw std::runtime_error(std::string("invalid google ngram format... no file: ") + path_ngram.file_string());
      
      
      if (debug >= 2)
	std::cerr << "\tfile: " << path_ngram.file_string() << std::endl;

      utils::compress_istream is(path_ngram, 1024 * 1024);
      while (std::getline(is, line)) {
	tokenizer_type tokenizer(line);
	
	tokens.clear();
	tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
	
	// invalid ngram...?
	if (tokens.size() != order + 1)
	  continue;
	
	// check if the rank of this ngram data is mpi_rank...
	
	if (order == 2) {
	  context.clear();
	  tokens_type::const_iterator titer_end = tokens.end() - 1;
	  for (tokens_type::const_iterator titer = tokens.begin(); titer != titer_end; ++ titer)
	    context.push_back(vocab_map[escape_word(*titer)]);
	  
	  if (ngram.shard_index(context.begin(), context.end()) == mpi_rank)
	    queue.push(std::make_pair(context, atol(tokens.back().c_str())));
	  
	} else {
	  
	  if (ngram_prefix.empty() || ! std::equal(ngram_prefix.begin(), ngram_prefix.end(), tokens.begin())) {
	    ngram_rank = shard_index(ngram, vocab_map, tokens);
	    
	    ngram_prefix.clear();
	    ngram_prefix.insert(ngram_prefix.end(), tokens.begin(), tokens.begin() + 2);
	  }
	  
	  if (ngram_rank == mpi_rank) {
	    context.clear();
	    tokens_type::const_iterator titer_end = tokens.end() - 1;
	    for (tokens_type::const_iterator titer = tokens.begin(); titer != titer_end; ++ titer)
	      context.push_back(vocab_map[escape_word(*titer)]);
	    
	    queue.push(std::make_pair(context, atol(tokens.back().c_str())));
	  }
	}
      }
    }
  }
  
  // termination!
  queue.push(std::make_pair(context_type(), count_type(0)));
  thread->join();
}
