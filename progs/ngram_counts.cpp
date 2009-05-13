


typedef boost::filesystem::path                                     path_type;
typedef boost::filesystem::directory_iterator                       directory_iterator;
typedef std::vector<path_type, std::allocator<path_type> >          path_set_type;
typedef std::vector<path_set_type, std::allocator<path_set_type> >  path_map_type;

typedef uint64_t                                                    count_type;
typedef std::string                                                 word_type;
typedef utils::simple_vector<word_type, std::allocator<word_type> > ngram_type;
struct ngram_hash
{
  utils::hashmurmur<size_t> hasher;
  size_t operator()(const ngram_type& x) const {
    size_t seed = 0;
    ngram_type::const_iterator iter_end = x.end();
    for (ngram_type::const_iterator iter = x.begin(); iter != iter_end; ++ iter)
      seed = hasher(iter->begin(), iter->end(), seed);
    return seed;
  }
};

#ifdef HAVE_TR1_UNORDERED_MAP
typedef std::tr1::unordered_map<ngram_type, count_type, ngram_hash, std::equal_to<ngram_type>,
				std::allocator<std::pair<const ngram_type, count_type> > > ngram_count_set_type;
#else
typedef sgi::hash_map<ngram_type, count_type, ngram_hash, std::equal_to<ngram_type>,
		      std::allocator<std::pair<const ngram_type, count_type> > > ngram_count_set_type;
#endif
typedef std::vector<ngram_count_set_type, std::allocator<ngram_count_set_type> > ngram_count_map_type;


path_type corpus_file;
path_type counts_file;

path_type corpus_list_file;
path_type counts_list_file;

path_type output_file;

int order = 5;

bool map_line = false;
int threads = 2;
double max_counts = 1.0; // 1G bytes

int debug = 0;

int main(int argc, char** argv)
{
  try {
    
    if (output_file.empty())
      throw std::runtime_error("no output file");

    path_set_type corpus_files;
    path_set_type counts_files;
    
    if (corpus_file == "-" || boost::filesystem::exists(corpus_file))
      corpus_files.push_back(corpus_file);
    if (counts_file == "-" || boost::filesystem::exists(counts_file))
      counts_files.push_back(counts_file);
    
    if (corpus_list_file == "-" || boost::filesystem::exists(corpus_list_file)) {
      utils::compress_istream is(corpus_list_file);
      std::string line;
      while (std::getline(is, line))
	if (boost::filesystem::exists(line))
	  corpus_files.push_back(line);
    }
    if (counts_list_file == "-" || boost::filesystem::exists(counts_list_file)) {
      utils::compress_istream is(counts_list_file);
      std::string line;
      while (std::getline(is, line))
	if (boost::filesystem::exists(line))
	  counts_files.push_back(line);
    }
    
    if (counts_files.empty() && corpus_files.empty()) 
      throw std::runtime_error("no corpus files nor counts files");
    
    preprocess(output_file, order);
    
    ngram_count_map_type counts(order);
    path_map_type        paths_counts(order);
    
    if (! counts_files.empty())
      accumulate_counts(counts_files, counts, output_file, paths_counts, map_line, max_counts);
    
    if (! corpus_files.empty())
      accumulate_corpus(corpus_files, counts, output_file, paths_counts, map_line, max_counts);
    
    postprocess(output_file, paths_counts);
  }
  catch (std::exception& err) {
    std::cerr << "error: " << err.what() << std::endl;
    return 1;
  }
  return 0;
}


void preprocess(const path_type& path, const int order)
{
  if (boost::filesystem::exists(path))
    if (! boost::filesystem::is_directory(path))
      boost::filesystem::remove(path);
  
  if (! boost::filesystem::exists(path))
    boost::filesystem::create_directory(path);
  
  for (int n = 1; n <= order; ++ n) {
    std::ostringstream stream;
    stream << n << "gms";
    
    const path_type ngram_dir = path / stream.str();
    
    if (boost::filesystem::exists(ngram_dir))
      utils::filesystem::remove_all(ngram_dir);
    boost::filesystem::create_directory(ngram_dir);
    
    // tempfile
    utils::tempfile::insert(ngram_dir);
  }
  
  // tempfile
  utils::tempfile::insert(path);
}

void postprocess(const path_type& path, const path_map_type& paths_counts)
{
  if (paths_counts.empty())
    throw std::runtime_error("no counts?");

  const int order = paths_counts.size();
  
  // process unigrams...
  {
    typedef std::map<std::string, count_type, std::less<std::string>, std::allocator<std::pair<const std::string, count_type> > > word_set_type;
    typedef std::multimap<count_type, std::string, std::greater<count_type>, std::allocator<std::pair<const count_type, std::string> > > count_set_type;

    const path_type ngram_dir         = path / "1gms";
    const path_type vocab_file        = ngram_dir / "vocab.gz";
    const path_type vocab_sorted_file = ngram_dir / "vocab_cs.gz";
    
    word_set_type words;
    path_set_type::const_iterator piter_end = paths_counts.front().end();
    for (path_set_type::const_iterator piter = paths_counts.front().begin(); piter != piter_end; ++ piter) {
      if (! boost::filesystem::exists(*piter))
	throw std::runtime_error(std::string("no unigramcounts? ") + piter->file_string());
      
      utils::compress_istream is(*piter);
      std::string line;
      tokens_type tokens;
      while (std::getline(is, line)) {
	tokenizer_type tokenizer(line);
	
	tokens.clear();
	tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
	
	if (tokens.size() != 2) continue;
	
	words[tokens.front()] += atoll(tokens.back().c_str());
      }
      
      boost::filesystem::remove(*piter);
      utils::tempfile::erase(*piter);
    }
    
    count_set_type counts;
    
    {
      utils::compress_ostream os(vocab_file, 1024 * 1024);
      word_set_type::const_iterator witer_end = words.end();
      for (word_set_type::const_iterator witer = words.begin(); witer != witer_end; ++ witer) {
	os << witer->first << '\t' << witer->second << '\n';
	counts.insert(std::make_pair(witer->second, witer->first));
      }
    }
    
    {
      utils::compress_ostream os(vocab_sorted_file, 1024 * 1024);
      count_set_type::const_iterator citer_end = counts.end();
      for (count_set_type::const_iterator citer = counts.begin(); citer != citer_end; ++ citer)
	os << citer->second << '\t' << citer->first << '\n';
    }
    
    utils::tempfile::erase(ngram_dir);
  }
  
  // process others...
  for (int n = 2; n <= order; ++ n) {
    std::ostringstream stream_ngram;
    stream_ngram << n << "gms";
    
    std::ostringstream stream_index;
    stream_index << n << "gm.idx";
    
    const path_type ngram_dir = path / stream_ngram.str();
    const path_type index_file = ngram_dir / stream_index.str();
    
    utils::compress_ostream os(index_file);
    
    std::string line;
    tokens_type tokens;
    
    path_set_type::const_iterator piter_end = paths_counts[n - 1].end();
    for (path_set_type::const_iterator piter = paths_counts[n - 1].begin(); piter != piter_end; ++ piter) {
      if (! boost::filesystem::exists(*piter))
	throw std::runtime_error(std::string("no unigramcounts? ") + piter->file_string());
      
      utils::compress_istream is(*piter);
      
      if (! std::getline(is, line)) continue;
      
      tokenizer_type tokenizer(line);
      tokens.clear();
      tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
      
      if (tokens.size() != n + 1)
	throw std::runtime_error("invalid google's ngram structure...");
      
      os << piter->leaf() << '\t' << ngram_type(tokens.begin(), tokens.end() - 1) << '\n';
      
      utils::tempfile::erase(*piter);
    }
    
    utils::tempfile::erase(ngram_dir);
  }
  
  utils::tempfile::erase(path);
}


template <typename Counts, typename Path, typename Paths>
inline
void dump_counts(Counts& counts, const Path& path, Paths& paths)
{
  
  typedef typename Counts::value_type ngram_count_set_type;
  typedef typename ngram_count_set_type::value_type ngram_count_type;
  typedef std::vector<const ngram_count_type*, std::allocator<const ntram_count_type*> > queue_type;
  
  queue_type queue;
  
  typename Counts::iterator citer       = counts.begin();
  typename Counts::iterator citer_end   = counts.end();
  for (int order = 1; citer != citer_end; ++ citer, ++ order) 
    if (! citer->empty()) {
      
      // set up files...
      std::ostringstream stream_ngram_dir;
      std::ostringstream stream_ngram_file;
      stream_ngram_dir << order << "gms";
      stream_ngram_file << order << "gm-XXXXXX";
      
      const path_type ngram_dir = path / stream_ngram_dir.str();
      path_type ngram_file_tmp;
      path_type ngram_file;
      do {
	ngram_file_tmp = utils::tempfile::file_name(ngram_dir / stream_ngram_file.str());
	ngram_file = ngram_file_tmp.file_string() + ".gz";
      } while (boost::filesystem::exists(ngram_file));
      
      utils::tempfile::insert(ngram_file_tmp);
      utils::tempfile::insert(ngram_file);
      
      // sorting...
      queue.clear();
      typename ngram_count_set_type::const_iterator niter_end = citer->end();
      for (typename ngram_count_set_type::const_iterator niter = citer->begin(); niter != niter_end; ++ niter)
	queue.push_back(&(*niter));
      std::sort(queue.begin(), queue.end(), less_first_p<ngram_count_type>());
      
      // dumping...
      utils::compress_ostream os(ngrm_file, 1024 * 1024);
      queue_type::const_iterator qiter_end = queue.end();
      for (queue_type::const_iterator qiter = queue.begin(); qiter != qiter_end; ++ qiter)
	os << (*qiter)->first << '\t' << (*qiter)->second << '\n';
      
      paths[order - 1].push_back(ngram_file);
      
      // clear...
      citer->clear();
      ngram_count_set_type(*citer).swap(*citer);
    }
}

template <typename Iterator, typename Counts, typename Path, typename Paths>
inline
void accumulate_sentence(Iterator first, Iterator last, Counts& counts, const Path& path, Paths& paths, const double max_malloc)
{
  typedef std::vector<std::string, std::allocator<std::string> > tokens_type;
  
  tokens_type tokens;

  const std::string bos = static_cast<const std::string&>(vocab_type::BOS);
  const std::string eos = static_cast<const std::string&>(vocab_type::EOS);

  // every 4096 iterations, we will check for memory boundary
  const size_t iter_mask = (1 << 12) - 1;
  
  size_t iter = 0;
  for (/**/; first != last; ++ first, ++ iter) {
    tokenizer_type tokenizer(*first);
    
    tokens.clear();
    tokens.push_back(bos);
    tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
    tokens.push_back(eos);
    
    tokens_type::const_iterator titer_begin = tokens.begin();
    tokens_type::const_iterator titer_begin = tokens.end();
    
    typename Counts::iterator citer_begin = counts.begin();
    typename Counts::iterator citer_end   = counts.end();
    for (typename Counts::iterator citer = citer_begin; citer != citer_end; ++ citer) {
      const int order = (citer - citer_begin) + 1;
      
      for (tokens_type::const_iterator titer = titer_begin; titer + order <= titer_end; ++ titer)
	++ citer->operator[](ngram_type(titer, titer + order));
    }
    
    if (iter & iter_mask == iter_mask) {
      size_t num_allocated = 0;
      MallocExtension::instance()->GetNumericProperty("generic.current_allocated_bytes", &num_allocated);
      
      if (num_allocated > size_t(max_malloc * 1024 * 1024 * 1024))
	dump_counts(counts, path, paths);
    }
  }
  
  size_t num_allocated = 0;
  MallocExtension::instance()->GetNumericProperty("generic.current_allocated_bytes", &num_allocated);
  if (num_allocated > size_t(max_malloc * 1024 * 1024 * 1024))
    dump_counts(counts, path, paths);
}

template <typename Iterator, typename Counts, typename Path, typename Paths>
inline
void accumulate_counts(Iterator first, Iterator last, Counts& counts, const Path& path, Paths& paths, const double max_malloc)
{
  typedef std::vector<std::string, std::allocator<std::string> > tokens_type;
  
  tokens_type tokens;
  
  const int max_order = counts.size();
  
  // every 1024 * 8 iterations, we will check for memory boundary
  const size_t iter_mask = (1 << 13) - 1;
  
  size_t iter = 0;
  for (/**/; first != last; ++ first, ++ iter) {
    tokenizer_type tokenizer(*first);
    
    tokens.clear();
    tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
    
    if (tokens.size() >= 2 && tokens.size() - 1 <= max_order)
      counts[tokens.size() - 1][ngram_type(tokens.begin(), tokens.end() - 1)] += atoll(tokens.back().c_str());
    
    if (iter & iter_mask == iter_mask) {
      size_t num_allocated = 0;
      MallocExtension::instance()->GetNumericProperty("generic.current_allocated_bytes", &num_allocated);
      
      if (num_allocated > size_t(max_malloc * 1024 * 1024 * 1024))
	dump_counts(counts, path, paths);
    }
  }
  
  size_t num_allocated = 0;
  MallocExtension::instance()->GetNumericProperty("generic.current_allocated_bytes", &num_allocated);
  if (num_allocated > size_t(max_malloc * 1024 * 1024 * 1024))
    dump_counts(counts, path, paths);
}


void accumulate_corpus(const path_set_type& paths,
		       ngram_count_map_type& counts,
		       const path_type& output_path,
		       path_map_type&   paths_counts,
		       const bool map_line,
		       const double max_counts)
{
  
  
  
}

