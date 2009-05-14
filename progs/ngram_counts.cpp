
#include <iostream>
#include <sstream>

#include <string>
#include <vector>
#include <stdexcept>
#include <iterator>
#include <algorithm>

#include <expgram/Word.hpp>
#include <expgram/Vocab.hpp>
#include <expgram/Sentence.hpp>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <boost/tokenizer.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/functional/hash.hpp>

#include <utils/simple_vector.hpp>
#include <utils/sgi_hash_map.hpp>
#include <utils/hashmurmur.hpp>
#include <utils/space_separator.hpp>
#include <utils/compact_trie.hpp>
#include <utils/istream_line_iterator.hpp>
#include <utils/bithack.hpp>
#include <utils/lockfree_queue.hpp>
#include <utils/compress_stream.hpp>
#include <utils/tempfile.hpp>

#include <google/malloc_extension.h>

typedef boost::filesystem::path                                     path_type;
typedef boost::filesystem::directory_iterator                       directory_iterator;

typedef std::vector<path_type, std::allocator<path_type> >          path_set_type;
typedef std::vector<path_set_type, std::allocator<path_set_type> >  path_map_type;

typedef uint64_t          count_type;
typedef expgram::Word     word_type;
typedef expgram::Vocab    vocab_type;
typedef expgram::Sentence ngram_type;

typedef utils::compact_trie<word_type, count_type, boost::hash<word_type>, std::equal_to<word_type>,
			    std::allocator<std::pair<const word_type, count_type> > > ngram_count_set_type;

path_type corpus_file;
path_type counts_file;

path_type corpus_list_file;
path_type counts_list_file;

path_type output_file;

int max_order = 5;

bool map_line = false;
int threads = 2;
double max_malloc = 1.0; // 1G bytes

int debug = 0;


void accumulate_counts(const path_set_type& paths,
		       const path_type& output_path,
		       path_map_type&   paths_counts,
		       const bool map_line,
		       const double max_malloc,
		       const int num_threads);
void accumulate_corpus(const path_set_type& paths,
		       const path_type& output_path,
		       path_map_type&   paths_counts,
		       const bool map_line,
		       const double max_malloc,
		       const int num_threads);
void preprocess(const path_type& path, const int max_order);
void postprocess(const path_type& path, const path_map_type& paths_counts);
int getoptions(int argc, char** argv);

int main(int argc, char** argv)
{
  try {
    if (getoptions(argc, argv) != 0) 
      return 1;
    
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
      while (std::getline(is, line)) {
	boost::algorithm::trim(line);
	if (! line.empty()) {
	  if (boost::filesystem::exists(line))
	    corpus_files.push_back(line);
	  else if (boost::filesystem::exists(corpus_list_file.parent_path() / line))
	    corpus_files.push_back(corpus_list_file.parent_path() / line);
	  else
	    throw std::runtime_error(std::string("no file? ") + line);
	}
      }
    }
    if (counts_list_file == "-" || boost::filesystem::exists(counts_list_file)) {
      utils::compress_istream is(counts_list_file);
      std::string line;
      while (std::getline(is, line)) {
	boost::algorithm::trim(line);
	if (! line.empty()) {
	  if (boost::filesystem::exists(line))
	    counts_files.push_back(line);
	  else if (boost::filesystem::exists(counts_list_file.parent_path() / line))
	    counts_files.push_back(counts_list_file.parent_path() / line);
	  else
	    throw std::runtime_error(std::string("no file? ") + line);
	}
      }
    }
    
    if (counts_files.empty() && corpus_files.empty()) 
      throw std::runtime_error("no corpus files nor counts files");
    
    preprocess(output_file, max_order);
    
    path_map_type paths_counts(max_order);
    
    if (! counts_files.empty())
      accumulate_counts(counts_files, output_file, paths_counts, map_line, max_malloc, threads);
    
    if (! corpus_files.empty())
      accumulate_corpus(corpus_files, output_file, paths_counts, map_line, max_malloc, threads);
    
    postprocess(output_file, paths_counts);
  }
  catch (std::exception& err) {
    std::cerr << "error: " << err.what() << std::endl;
    return 1;
  }
  return 0;
}


void preprocess(const path_type& path, const int max_order)
{
  if (boost::filesystem::exists(path) && ! boost::filesystem::is_directory(path))
    utils::filesystem::remove_all(path);
  
  if (! boost::filesystem::exists(path))
    boost::filesystem::create_directories(path);
  
  // remove all...
  boost::filesystem::directory_iterator iter_end;
  for (boost::filesystem::directory_iterator iter(path); iter != iter_end; ++ iter)
    boost::filesystem::remove_all(*iter);
  
  for (int order = 1; order <= max_order; ++ order) {
    std::ostringstream stream;
    stream << order << "gms";
    
    const path_type ngram_dir = path / stream.str();
    
    boost::filesystem::create_directory(ngram_dir);
    
    // tempfile
    utils::tempfile::insert(ngram_dir);
  }
  
  // tempfile
  utils::tempfile::insert(path);
}

template <typename Tp>
struct greater_secondp
{
  bool operator()(const Tp* x, const Tp* y) const
  {
    return x->second > y->second;
  }
};

void postprocess(const path_type& path, const path_map_type& paths_counts)
{
  typedef std::vector<std::string, std::allocator<std::string> > tokens_type;
  typedef boost::tokenizer<utils::space_separator>               tokenizer_type;
  
  if (paths_counts.empty())
    throw std::runtime_error("no counts?");

  const int max_order = paths_counts.size();
  
  // process unigrams...
  {
    typedef std::map<std::string, count_type, std::less<std::string>, std::allocator<std::pair<const std::string, count_type> > > word_set_type;
    typedef word_set_type::value_type value_type;
    typedef std::vector<const value_type*, std::allocator<const value_type*> > sorted_type;

    const path_type ngram_dir         = path / "1gms";
    const path_type vocab_file        = ngram_dir / "vocab.gz";
    const path_type vocab_sorted_file = ngram_dir / "vocab_cs.gz";
    const path_type total_file        = ngram_dir / "total";
    
    word_set_type words;
    path_set_type::const_iterator piter_end = paths_counts.front().end();
    for (path_set_type::const_iterator piter = paths_counts.front().begin(); piter != piter_end; ++ piter) {
      if (! boost::filesystem::exists(*piter))
	throw std::runtime_error(std::string("no unigramcounts? ") + piter->file_string());
      
      utils::compress_istream is(*piter, 1024 * 1024);
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
    
    sorted_type sorted;
    sorted.reserve(words.size());
    
    count_type total = 0;
    {
      utils::compress_ostream os(vocab_file, 1024 * 1024);
      word_set_type::const_iterator witer_end = words.end();
      for (word_set_type::const_iterator witer = words.begin(); witer != witer_end; ++ witer) {
	os << witer->first << '\t' << witer->second << '\n';
	
	sorted.push_back(&(*witer));
	
	total += witer->second;
      }
    }
    
    std::sort(sorted.begin(), sorted.end(), greater_secondp<value_type>());
    
    {
      utils::compress_ostream os(vocab_sorted_file, 1024 * 1024);
      
      sorted_type::const_iterator siter_end = sorted.end();
      for (sorted_type::const_iterator siter = sorted.begin(); siter != siter_end; ++ siter)
	os << (*siter)->second << '\t' << (*siter)->first << '\n';
    }
    
    {
      utils::compress_ostream os(total_file);
      os << total << '\n';
    }
    
    utils::tempfile::erase(ngram_dir);
  }
  
  // process others...
  for (int order = 2; order <= max_order; ++ order) {
    std::ostringstream stream_ngram;
    stream_ngram << order << "gms";
    
    std::ostringstream stream_index;
    stream_index << order << "gm.idx";    
    const path_type ngram_dir = path / stream_ngram.str();
    const path_type index_file = ngram_dir / stream_index.str();
    
    utils::compress_ostream os(index_file);
    
    std::string line;
    tokens_type tokens;
    
    path_set_type::const_iterator piter_end = paths_counts[order - 1].end();
    for (path_set_type::const_iterator piter = paths_counts[order - 1].begin(); piter != piter_end; ++ piter) {
      
      if (! boost::filesystem::exists(*piter))
	throw std::runtime_error(std::string("no unigramcounts? ") + piter->file_string());
      
      utils::compress_istream is(*piter);

      if (! std::getline(is, line)) continue;
      
      tokenizer_type tokenizer(line);
      
      tokens.clear();
      tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
      
      if (tokens.size() != order + 1)
	throw std::runtime_error("invalid google's ngram structure...");
      
      os << piter->leaf() << '\t' << ngram_type(tokens.begin(), tokens.end() - 1) << '\n';
      
      utils::tempfile::erase(*piter);
    }
    
    utils::tempfile::erase(ngram_dir);
  }
  
  utils::tempfile::erase(path);
}


template <typename Tp, typename VocabMap>
struct less_pfirst_vocab
{
  VocabMap& vocab_map;
  less_pfirst_vocab(VocabMap& _vocab_map) : vocab_map(_vocab_map) {}
  
  bool operator()(const Tp* x, const Tp* y) const
  {
    if (utils::bithack::max(x->first.id(), y->first.id()) >= vocab_map.size())
      const_cast<VocabMap&>(vocab_map).resize(utils::bithack::max(x->first.id(), y->first.id()) + 1, 0);
    
    if (! vocab_map[x->first.id()])
      vocab_map[x->first.id()] = &static_cast<const std::string&>(x->first);
    if (! vocab_map[y->first.id()])
      vocab_map[y->first.id()] = &static_cast<const std::string&>(y->first);
    
    return *vocab_map[x->first.id()] < *vocab_map[y->first.id()];
  }
};

template <typename Prefix, typename Counts, typename Iterator, typename Path, typename PathIterator, typename StreamIterator, typename VocabMap>
void dump_counts(const Prefix& prefix, const Counts& counts, Iterator first, Iterator last, const Path& path, PathIterator path_iter, StreamIterator stream_iter, VocabMap& vocab_map)
{
  typedef typename std::iterator_traits<Iterator>::value_type value_type;
  typedef std::vector<const value_type*, std::allocator<const value_type*> > value_set_type;

  value_set_type values;
  for (Iterator iter = first; iter != last; ++ iter)
    values.push_back(&(*iter));
  
  std::sort(values.begin(), values.end(), less_pfirst_vocab<value_type, VocabMap>(vocab_map));

  const int order = prefix.size() + 1;

  if (! (*stream_iter)) {
    std::ostringstream stream_dir;
    stream_dir << order << "gms";
    
    std::ostringstream stream_name;
    stream_name << order << "gm-XXXXXX";
    
    const Path ngram_dir = path / stream_dir.str();
    const Path counts_file_tmp = utils::tempfile::file_name(ngram_dir / stream_name.str());
    utils::tempfile::insert(counts_file_tmp);
    const Path counts_file = counts_file_tmp.file_string() + ".gz";
    utils::tempfile::insert(counts_file);
    
    stream_iter->reset(new utils::compress_ostream(counts_file, 1024 * 1024));
    path_iter->push_back(counts_file);
  }
  
  Prefix prefix_new(prefix.size() + 1);
  std::copy(prefix.begin(), prefix.end(), prefix_new.begin());
  
  // dump counts...
  typename value_set_type::const_iterator iter_end = values.end();
  for (typename value_set_type::const_iterator iter = values.begin(); iter != iter_end; ++ iter) {
    
    typename Prefix::const_iterator piter_end = prefix.end();
    for (typename Prefix::const_iterator piter = prefix.begin(); piter != piter_end ; ++ piter)
      *(*stream_iter) << *vocab_map[piter->id()] << ' ';
    
    *(*stream_iter) << *(vocab_map[(*iter)->first.id()]) << '\t' << counts[(*iter)->second] << '\n';
    
    // recursive call...
    if (! counts.empty((*iter)->second)) {
      prefix_new.back() = (*iter)->first;
      dump_counts(prefix_new, counts, counts.begin((*iter)->second), counts.end((*iter)->second), path, path_iter + 1, stream_iter + 1, vocab_map);
    }
  }
}


template <typename Counts, typename Path, typename Paths>
inline
void dump_counts(const Counts& counts, const Path& path, Paths& paths)
{
  typedef boost::shared_ptr<utils::compress_ostream> ostream_ptr_type;
  typedef std::vector<ostream_ptr_type, std::allocator<ostream_ptr_type> > ostream_ptr_set_type;

  typedef std::vector<const std::string*, std::allocator<const std::string*> > vocab_map_type;
  typedef ngram_type prefix_type;

  const int max_order = paths.size();
  
  ostream_ptr_set_type ostreams(max_order);
  vocab_map_type vocab_map;
  prefix_type prefix;
  
  // recursive call....
  dump_counts(prefix,
	      counts, 
	      counts.begin(counts.root()), counts.end(counts.root()),
	      path,
	      paths.begin(),
	      ostreams.begin(),
	      vocab_map);
}


struct TaskCorpus
{
  template <typename Iterator, typename Counts, typename Path, typename Paths>
  inline
  void operator()(Iterator first, Iterator last, Counts& counts, const Path& path, Paths& paths, const double max_malloc)
  {
    typedef boost::tokenizer<utils::space_separator> tokenizer_type;
    
    ngram_type sentence;
    
    // every 4096 iterations, we will check for memory boundary
    const size_t iteration_mask = (1 << 12) - 1;

    const int max_orde = paths.size();
  
    size_t iteration = 0;
    for (/**/; first != last; ++ first, ++ iteration) {
      tokenizer_type tokenizer(*first);

      sentence.clear();
      sentence.push_back(vocab_type::BOS);
      sentence.insert(sentence.end(), tokenizer.begin(), tokenizer.end());
      sentence.push_back(vocab_type::EOS);

      if (sentence.size() == 2) continue;
    
      ngram_type::const_iterator siter_begin = sentence.begin();
      ngram_type::const_iterator siter_end   = sentence.end();
    
      for (ngram_type::const_iterator siter = siter_begin; siter != siter_end; ++ siter) {
	typename Counts::id_type id = counts.root();
	for (ngram_type::const_iterator iter = siter; iter != std::min(siter + max_order, siter_end); ++ iter) {
	  id = counts.insert(id, *iter);
	  ++ counts[id];
	}
      }
    
      if (iteration & iteration_mask == iteration_mask) {
	size_t num_allocated = 0;
	MallocExtension::instance()->GetNumericProperty("generic.current_allocated_bytes", &num_allocated);
      
	if (num_allocated > size_t(max_malloc * 1024 * 1024 * 1024)) {
	  dump_counts(counts, path, paths);
	  counts.clear();
	}
      }
    }
  }
};

struct TaskCounts
{
  template <typename Iterator, typename Counts, typename Path, typename Paths>
  inline
  void operator()(Iterator first, Iterator last, Counts& counts, const Path& path, Paths& paths, const double max_malloc)
  {
    typedef std::vector<std::string, std::allocator<std::string> > tokens_type;
    typedef boost::tokenizer<utils::space_separator>               tokenizer_type;
    
    tokens_type tokens;
    
    const int max_order = paths.size();
    
    // every 1024 * 8 iterations, we will check for memory boundary
    const size_t iter_mask = (1 << 13) - 1;
    
    size_t iter = 0;
    for (/**/; first != last; ++ first, ++ iter) {
      tokenizer_type tokenizer(*first);
      
      tokens.clear();
      tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
      
      if (tokens.size() < 2) continue;
      if (tokens.size() - 1 > max_order) continue;
      
      counts[counts.insert(tokens.begin(), tokens.end() - 1)] += atoll(tokens.back().c_str());
      
      if (iter & iter_mask == iter_mask) {
	size_t num_allocated = 0;
	MallocExtension::instance()->GetNumericProperty("generic.current_allocated_bytes", &num_allocated);
	
	if (num_allocated > size_t(max_malloc * 1024 * 1024 * 1024)) {
	  dump_counts(counts, path, paths);
	  counts.clear();
	}
      }
    }
  }
};

template <typename Task>
struct TaskLine
{
  typedef std::string line_type;
  typedef std::vector<line_type, std::allocator<line_type> > line_set_type;
  
  typedef utils::lockfree_queue<line_set_type, std::allocator<line_set_type> >  queue_type;
  
  typedef boost::thread                                                  thread_type;
  typedef boost::shared_ptr<thread_type>                                 thread_ptr_type;
  typedef std::vector<thread_ptr_type, std::allocator<thread_ptr_type> > thread_ptr_set_type;
  
  typedef Task task_type;
  
  queue_type&    queue;
  path_type      path;
  path_map_type& paths;
  double         max_malloc;
  
  TaskLine(queue_type&      _queue,
	   const path_type& _path,
	   path_map_type&   _paths,
	   const double     _max_malloc)
    : queue(_queue),
      path(_path),
      paths(_paths),
      max_malloc(_max_malloc) {}
  
  void operator()()
  {
    task_type __task;
    ngram_count_set_type counts;
    line_set_type lines;
    
    while (1) {
      queue.pop_swap(lines);
      if (lines.empty()) break;
      
      __task(lines.begin(), lines.end(), counts, path, paths, max_malloc);
      
      if (! counts.empty()) {
	size_t num_allocated = 0;
	MallocExtension::instance()->GetNumericProperty("generic.current_allocated_bytes", &num_allocated);
	
	if (num_allocated > size_t(max_malloc * 1024 * 1024 * 1024)) {
	  dump_counts(counts, path, paths);
	  counts.clear();
	}
      }
    }
    
    if (! counts.empty()) {
      dump_counts(counts, path, paths);
      counts.clear();
    }
  }
  
};

template <typename Task>
struct TaskFile
{
  typedef utils::lockfree_queue<path_type, std::allocator<path_type> >   queue_type;
  
  typedef boost::thread                                                  thread_type;
  typedef boost::shared_ptr<thread_type>                                 thread_ptr_type;
  typedef std::vector<thread_ptr_type, std::allocator<thread_ptr_type> > thread_ptr_set_type;

  typedef Task task_type;
  
  queue_type&    queue;
  path_type      path;
  path_map_type& paths;
  double         max_malloc;
  
  TaskFile(queue_type&      _queue,
	   const path_type& _path,
	   path_map_type&   _paths,
	   const double     _max_malloc)
    : queue(_queue),
      path(_path),
      paths(_paths),
      max_malloc(_max_malloc) {}
  
  void operator()()
  {
    task_type __task;
    ngram_count_set_type counts;
    path_type file;

    while (1) {
      queue.pop(file);
      if (file.empty()) break;
      
      if (! boost::filesystem::exists(file))
	throw std::runtime_error(std::string("no file? ") + file.file_string());
      
      utils::compress_istream is(file, 1024 * 1024);
      
      __task(utils::istream_line_iterator(is), utils::istream_line_iterator(), counts, path, paths, max_malloc);
      
      if (! counts.empty()) {
	size_t num_allocated = 0;
	MallocExtension::instance()->GetNumericProperty("generic.current_allocated_bytes", &num_allocated);
	
	if (num_allocated > size_t(max_malloc * 1024 * 1024 * 1024)) {
	  dump_counts(counts, path, paths);
	  counts.clear();
	}
      }
    }
    
    if (! counts.empty()) {
      dump_counts(counts, path, paths);
      counts.clear();
    }
  }
};

void expand_google_counts(const path_type& path,
			  path_set_type& paths,
			  const int max_order)
{
  typedef std::vector<std::string, std::allocator<std::string> > tokens_type;
  typedef boost::tokenizer<utils::space_separator>               tokenizer_type;

  if (! boost::filesystem::exists(path))
    throw std::runtime_error(std::string("no file? ") + path.file_string());
  if (! boost::filesystem::is_directory(path))
    throw std::runtime_error(std::string("no a directory? ") + path.directory_string());
  
  for (int order = 1; order <= max_order; ++ order) {
    
    std::ostringstream stream_ngram;
    stream_ngram << order << "gms";
    
    std::ostringstream stream_index;
    stream_index << order << "gm.idx";

    const path_type ngram_dir = path / stream_ngram.str();
    const path_type index_file = ngram_dir / stream_index.str();

    if (! boost::filesystem::exists(ngram_dir)) break;
    if (! boost::filesystem::is_directory(ngram_dir))
      throw std::runtime_error(std::string("no a directory? ") + ngram_dir.directory_string());
    
    if (order == 1) {
      const path_type vocab_file        = ngram_dir / "vocab.gz";
      const path_type vocab_sorted_file = ngram_dir / "vocab_cs.gz";
      
      if (! boost::filesystem::exists(vocab_file))
	throw std::runtime_error(std::string("no vocab.gz? ") + vocab_file.file_string());
      if (! boost::filesystem::exists(vocab_sorted_file))
	throw std::runtime_error(std::string("no vocab_cs.gz? ") + vocab_sorted_file.file_string());
      
      paths.push_back(vocab_file);
    } else {
      
      if (! boost::filesystem::exists(index_file))
	throw std::runtime_error(std::string("no index file? ") + index_file.file_string());
      
      utils::compress_istream is(index_file);
      std::string line;
      tokens_type tokens;
      
      while (std::getline(is, line)) {
	tokenizer_type tokenizer(line);
	
	tokens.clear();
	tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
	
	if (tokens.empty()) continue;
	
	if (tokens.size() != order + 1)
	  throw std::runtime_error(std::string("invalid google ngram format...") + index_file.file_string());
	
	const path_type path_ngram = ngram_dir / tokens.front();
	
	if (! boost::filesystem::exists(path_ngram))
	  throw std::runtime_error(std::string("invalid google ngram format... no file: ") + path_ngram.file_string());
	
	paths.push_back(path_ngram);
      }
    }
  }
}


void accumulate_counts(const path_set_type& __paths,
		       const path_type& output_path,
		       path_map_type&   paths_counts,
		       const bool map_line,
		       const double max_malloc,
		       const int num_threads)
{
  typedef std::vector<path_map_type, std::allocator<path_map_type> > path_thread_type;
  
  //
  // we will iterate over paths and try to locate count-files in google ngram format...
  //
  
  const int max_order = paths_counts.size();
  path_set_type paths;
  
  path_set_type::const_iterator piter_end = __paths.end();
  for (path_set_type::const_iterator piter = __paths.begin(); piter != piter_end; ++ piter) {
    if (! boost::filesystem::exists(*piter))
      throw std::runtime_error(std::string("no file? ") + piter->file_string());
    
    if (boost::filesystem::is_directory(*piter))
      expand_google_counts(*piter, paths, max_order);
    else
      paths.push_back(*piter);
  }
  
  if (map_line) {
    typedef TaskLine<TaskCounts> task_type;
    
    typedef task_type::line_set_type       line_set_type;
    typedef task_type::queue_type          queue_type;
    typedef task_type::thread_type         thread_type;
    typedef task_type::thread_ptr_set_type thread_ptr_set_type;
    
    thread_ptr_set_type threads(num_threads);
    queue_type          queue(num_threads * 8);
    path_thread_type    paths_thread(num_threads, path_map_type(max_order));
    
    for (int shard = 0; shard < num_threads; ++ shard)
      threads[shard].reset(new thread_type(task_type(queue, output_path, paths_thread[shard], max_malloc)));
    
    line_set_type lines;
    
    path_set_type::const_iterator piter_end = paths.end();
    for (path_set_type::const_iterator piter = paths.begin(); piter != piter_end; ++ piter) {
      if (! boost::filesystem::exists(*piter))
	throw std::runtime_error(std::string("no file? ") + piter->file_string());
      
      utils::compress_istream is(*piter, 1024 * 1024);
      std::string line;
      while (std::getline(is, line))
	if (! line.empty()) {
	  lines.push_back(line);
	  
	  if (lines.size() >= 1024 * 4) {
	    queue.push_swap(lines);
	    lines.clear();
	  }
	}
    }
    
    if (! lines.empty())
      queue.push_swap(lines);
    for (int shard = 0; shard < num_threads; ++ shard) {
      lines.clear();
      queue.push_swap(lines);
    }
    
    for (int shard = 0; shard < num_threads; ++ shard) {
      threads[shard]->join();
      
      path_map_type& paths_shard = paths_thread[shard];
      for (int order = 1; order <= max_order; ++ order)
	paths_counts[order - 1].insert(paths_counts[order - 1].end(), paths_shard[order - 1].begin(), paths_shard[order - 1].end());
      paths_shard.clear();
    }
    
  } else {
    typedef TaskFile<TaskCounts> task_type;
    
    typedef task_type::queue_type          queue_type;
    typedef task_type::thread_type         thread_type;
    typedef task_type::thread_ptr_set_type thread_ptr_set_type;
    
    thread_ptr_set_type threads(num_threads);
    queue_type          queue(num_threads);
    path_thread_type    paths_thread(num_threads, path_map_type(max_order));
    
    for (int shard = 0; shard < num_threads; ++ shard)
      threads[shard].reset(new thread_type(task_type(queue, output_path, paths_thread[shard], max_malloc)));
    
    path_set_type::const_iterator piter_end = paths.end();
    for (path_set_type::const_iterator piter = paths.begin(); piter != piter_end; ++ piter) {
      if (! boost::filesystem::exists(*piter))
	throw std::runtime_error(std::string("no file? ") + piter->file_string());
      queue.push(*piter);
    }
    
    for (int shard = 0; shard < num_threads; ++ shard)
      queue.push(path_type());
    
    for (int shard = 0; shard < num_threads; ++ shard) {
      threads[shard]->join();
      
      path_map_type& paths_shard = paths_thread[shard];
      for (int order = 1; order <= max_order; ++ order)
	paths_counts[order - 1].insert(paths_counts[order - 1].end(), paths_shard[order - 1].begin(), paths_shard[order - 1].end());
      paths_shard.clear();
    }
  }
}

void accumulate_corpus(const path_set_type& paths,
		       const path_type& output_path,
		       path_map_type&   paths_counts,
		       const bool map_line,
		       const double max_malloc,
		       const int num_threads)
{
  typedef std::vector<path_map_type, std::allocator<path_map_type> > path_thread_type;

  const int max_order = paths_counts.size();
  
  if (map_line) {
    typedef TaskLine<TaskCorpus> task_type;
    
    typedef task_type::line_set_type       line_set_type;
    typedef task_type::queue_type          queue_type;
    typedef task_type::thread_type         thread_type;
    typedef task_type::thread_ptr_set_type thread_ptr_set_type;
    
    thread_ptr_set_type threads(num_threads);
    queue_type          queue(num_threads * 8);
    path_thread_type    paths_thread(num_threads, path_map_type(max_order));
    
    for (int shard = 0; shard < num_threads; ++ shard)
      threads[shard].reset(new thread_type(task_type(queue, output_path, paths_thread[shard], max_malloc)));

    line_set_type lines;
    
    path_set_type::const_iterator piter_end = paths.end();
    for (path_set_type::const_iterator piter = paths.begin(); piter != piter_end; ++ piter) {
      if (! boost::filesystem::exists(*piter))
	throw std::runtime_error(std::string("no file? ") + piter->file_string());
      
      utils::compress_istream is(*piter, 1024 * 1024);
      std::string line;
      while (std::getline(is, line))
	if (! line.empty()) {
	  lines.push_back(line);
	  
	  if (lines.size() >= 1024 * 4) {
	    queue.push_swap(lines);
	    lines.clear();
	  }
	}
    }
    
    if (! lines.empty())
      queue.push_swap(lines);
    for (int shard = 0; shard < num_threads; ++ shard) {
      lines.clear();
      queue.push_swap(lines);
    }
    
    for (int shard = 0; shard < num_threads; ++ shard) {
      threads[shard]->join();
      
      path_map_type& paths_shard = paths_thread[shard];
      for (int order = 1; order <= max_order; ++ order)
	paths_counts[order - 1].insert(paths_counts[order - 1].end(), paths_shard[order - 1].begin(), paths_shard[order - 1].end());
      paths_shard.clear();
    }
    
  } else {
    typedef TaskFile<TaskCorpus> task_type;
    
    typedef task_type::queue_type          queue_type;
    typedef task_type::thread_type         thread_type;
    typedef task_type::thread_ptr_set_type thread_ptr_set_type;
    
    thread_ptr_set_type threads(num_threads);
    queue_type          queue(num_threads);
    path_thread_type    paths_thread(num_threads, path_map_type(max_order));
    
    for (int shard = 0; shard < num_threads; ++ shard)
      threads[shard].reset(new thread_type(task_type(queue, output_path, paths_thread[shard], max_malloc)));
    
    path_set_type::const_iterator piter_end = paths.end();
    for (path_set_type::const_iterator piter = paths.begin(); piter != piter_end; ++ piter) {
      if (! boost::filesystem::exists(*piter))
	throw std::runtime_error(std::string("no file? ") + piter->file_string());
      queue.push(*piter);
    }
    
    for (int shard = 0; shard < num_threads; ++ shard)
      queue.push(path_type());
    
    for (int shard = 0; shard < num_threads; ++ shard) {
      threads[shard]->join();
      
      path_map_type& paths_shard = paths_thread[shard];
      for (int order = 1; order <= max_order; ++ order)
	paths_counts[order - 1].insert(paths_counts[order - 1].end(), paths_shard[order - 1].begin(), paths_shard[order - 1].end());
      paths_shard.clear();
    }
  }
}


int getoptions(int argc, char** argv)
{
  namespace po = boost::program_options;
  
  po::options_description desc("options");
  desc.add_options()
    ("corpus",       po::value<path_type>(&corpus_file),  "corpus file")
    ("counts",       po::value<path_type>(&counts_file),  "counts file")
    
    ("corpus-list",  po::value<path_type>(&corpus_list_file),  "corpus list file")
    ("counts-list",  po::value<path_type>(&counts_list_file),  "counts list file")
    
    ("output",       po::value<path_type>(&output_file), "output directory")
    
    ("order",      po::value<int>(&max_order),     "ngram order")
    ("map-line",   po::bool_switch(&map_line),     "map by lines, not by files")
    ("threads",    po::value<int>(&threads),       "# of threads")
    ("max-malloc", po::value<double>(&max_malloc), "maximum malloc in GB")
    
    ("debug", po::value<int>(&debug)->implicit_value(1), "debug level")
    ("help", "help message");
  
  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);
  
  if (vm.count("help")) {
    std::cout << argv[0] << " [options]" << '\n' << desc << '\n';
    return 1;
  }
  
  return 0;
}
