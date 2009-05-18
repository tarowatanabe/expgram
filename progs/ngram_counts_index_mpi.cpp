

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
    
    vocab_type& vocab = index.vocab();
    vocab.open(rep.path("vocab"), 1024 * 1024 * 16);
    
    const path_type unigram_dir = path / "1gms";
    const path_type vocab_file = unigram_dir / "vocab.gz";
    const path_type vocab_sorted_file = unigram_dir / "vocab_cs.gz";

    id_type word_id = 0;
    
    utils::compress_istream is(vocab_sorted_file, 1024 * 1024);
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
      
      vocab.insert(word);
      ++ word_id;
    }
    // indexing...
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

void index_ngram_mapper_root(MPI::Comm& children, const path_type& path, ngram_type& ngram)
{
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  for (int order = 2; /**/; ++ order) {
    
    // collect all the files
    
    // distribute # of files

    int count_file_size = ;
    MPI::COMM_WORLD.Bcast(&count_file_size, 1, MPI::INT, 0);
    if (con_file_size == 0) break;
    
    // distribute files...
    
  }
}

void index_ngram_mapper_others(MPI::Comm& children, const path_type& path, ngram_type& ngram)
{
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  for (int order = 2; /**/; ++ order) {
    
    int count_file_size = 0;
    MPI::COMM_WORLD.Bcast(&count_file_size, 1, MPI::INT, 0);
    if (con_file_size == 0) break;
    
    // collect files...
    
    // perform reduction...
    
  }
}

struct IndexNGramReducer
{
  
  
};

void index_ngram_reducer(MPI::Comm& children, ngram_type& ngram, shard_data_type& shard)
{
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  
}


struct IndexNGramReducerUnique
{
  
  typedef expgram::NGramCountsIndexer indexer_type;
  
  
  void operator()()
  {
    context_count_type  context_count;
    context_type        prefix;
    word_count_set_type words;
    
    int order = 2;
    
    while (1) {
      
      queue.pop(context_count);
      if (context_count.first.empty()) break;
	
      const context_type& context = context_count.first;
      const count_type&   count   = context_count.second;
      
      if (context.size() != prefix.size() + 1 || ! std::equal(prefix.begin(), prefix.end(), context.begin())) {
	if (! words.empty()) {
	  indexer_type::index_ngram(shard, ngram, *this, prefix, words);
	  words.clear();
	  
	  if (context.size() != order)
	    indexer_type::index_ngram(shard, ngram, *this, debug);
	}
	
	prefix.clear();
	prefix.insert(prefix.end(), context.begin(), context.end() - 1);
	order = context.size();
      }
      
      words.push_back(std::make_pair(context.back(), count));
    }
    
    // perform final indexing...
    if (! words.empty()) {
      indexer_type::index_ngram(shard, ngram, *this, prefix, words);
      indexer_type::index_ngram(shard, ngram, *this, debug);
    }
  }
};

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
	  for (tokens_type::const_iterator titer = tokens.begin(); titer != titer_end; ++ titer) {
	    const id_type id = escape_word(*titer).id();
	    
	    if (id >= vocab_map.size() || vocab_map[id] == id_type(-1))
	      throw std::runtime_error("invalid vocbulary");
	    
	    context.push_back(vocab_map[id]);
	  }
	  
	  if (ngram.shard_index(context.begin(), context.end()) == mpi_rank)
	    queue.push(std::make_pair(context, atol(tokens.back().c_str())));
	  
	} else {
	  
	  if (ngram_prefix.empty() || ! std::equal(ngram_prefix.begin(), ngram_prefix.end(), tokens.begin())) {
	    id_type ids[2];
	    
	    ids[0] = escape_word(tokens[0]).id();
	    ids[1] = escape_word(tokens[1]).id();
	    
	    ngram_rank = ngram.shard_index(ids, ids + 2);
	    
	    ngram_prefix.clear();
	    ngram_prefix.insert(ngram_prefix.end(), tokens.begin(), tokens.begin() + 2);
	  }
	  
	  if (ngram_rank == mpi_rank) {
	    context.clear();
	    tokens_type::const_iterator titer_end = tokens.end() - 1;
	    for (tokens_type::const_iterator titer = tokens.begin(); titer != titer_end; ++ titer) {
	      const id_type id = escape_word(*titer).id();
	      
	      if (id >= vocab_map.size() || vocab_map[id] == id_type(-1))
		throw std::runtime_error("invalid vocbulary");
	      
	      context.push_back(vocab_map[id]);
	    }
	    
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
