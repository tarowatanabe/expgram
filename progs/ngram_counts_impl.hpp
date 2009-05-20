// -*- mode: c++ -*-

#ifndef __NGRAM_COUNTS_IMPL__HPP__
#define __NGRAM_COUNTS_IMPL__HPP__ 1

#include <sstream>
#include <iostream>
#include <vector>
#include <string>
#include <iterator>
#include <algorithm>

#include <map>

#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>
#include <boost/filesystem.hpp>
#include <boost/tokenizer.hpp>
#include <boost/functional/hash.hpp>

#include <utils/space_separator.hpp>
#include <utils/compress_stream.hpp>
#include <utils/compact_trie.hpp>
#include <utils/bithack.hpp>
#include <utils/lockfree_queue.hpp>
#include <utils/istream_line_iterator.hpp>
#include <utils/tempfile.hpp>

#include <expgram/Word.hpp>
#include <expgram/Vocab.hpp>
#include <expgram/Sentence.hpp>

#include <google/malloc_extension.h>

struct GoogleNGramCounts
{
  typedef uint64_t          count_type;
  typedef expgram::Word     word_type;
  typedef expgram::Vocab    vocab_type;
  typedef expgram::Sentence ngram_type;

  typedef boost::filesystem::path                                     path_type;
  typedef std::vector<path_type, std::allocator<path_type> >          path_set_type;
  typedef std::vector<path_set_type, std::allocator<path_set_type> >  path_map_type;
  
  typedef utils::compact_trie<word_type, count_type, boost::hash<word_type>, std::equal_to<word_type>,
			      std::allocator<std::pair<const word_type, count_type> > > ngram_count_set_type;

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
	    GoogleNGramCounts::dump_counts(counts, path, paths);
	    counts.clear();
	  }
	}
      }
      
      if (! counts.empty()) {
	GoogleNGramCounts::dump_counts(counts, path, paths);
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
	    GoogleNGramCounts::dump_counts(counts, path, paths);
	    counts.clear();
	  }
	}
      }
      
      if (! counts.empty()) {
	GoogleNGramCounts::dump_counts(counts, path, paths);
	counts.clear();
      }
    }
  };

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
  static inline
  void dump_counts(const Prefix& prefix,
		   const Counts& counts,
		   Iterator first, Iterator last,
		   const Path& path,
		   PathIterator path_iter, StreamIterator stream_iter,
		   VocabMap& vocab_map)
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
  static inline
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
  
  template <typename Path>
  static inline
  void preprocess(const Path& path, const int max_order)
  {
    typedef Path path_type;
    
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
  
  template <typename Path, typename PathMap>
  static inline
  void postprocess(const Path& path, const PathMap& paths_counts)
  {
    typedef Path path_type;
    typedef typename PathMap::value_type path_set_type;
    
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
      typename path_set_type::const_iterator piter_end = paths_counts.front().end();
      for (typename path_set_type::const_iterator piter = paths_counts.front().begin(); piter != piter_end; ++ piter) {
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
	  os << (*siter)->first << '\t' << (*siter)->second << '\n';
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
      
      typename path_set_type::const_iterator piter_end = paths_counts[order - 1].end();
      for (typename path_set_type::const_iterator piter = paths_counts[order - 1].begin(); piter != piter_end; ++ piter) {
	
	if (! boost::filesystem::exists(*piter))
	  throw std::runtime_error(std::string("no unigramcounts? ") + piter->file_string());
	
	utils::compress_istream is(*piter);
	
	if (! std::getline(is, line)) continue;
	
	tokenizer_type tokenizer(line);
	
	tokens.clear();
	tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
	
	if (tokens.size() != order + 1)
	  throw std::runtime_error("invalid google's ngram structure...");
	
	os << piter->leaf() << '\t';
	std::copy(tokens.begin(), tokens.end() - 2, std::ostream_iterator<std::string>(os, " "));
	os << *(tokens.end() - 2) << '\n';
	
	utils::tempfile::erase(*piter);
      }
      
      utils::tempfile::erase(ngram_dir);
    }
    
    utils::tempfile::erase(path);
  }  
  
  template <typename Path, typename PathSet>
  static inline
  void expand(const Path& path,
	      PathSet& paths,
	      const int max_order)
  {
    typedef Path path_type;

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

  
  struct TaskCorpus
  {
    template <typename Iterator, typename Counts, typename Path, typename Paths>
    inline
    void operator()(Iterator first, Iterator last, Counts& counts, const Path& path, Paths& paths, const double max_malloc)
    {
      typedef boost::tokenizer<utils::space_separator> tokenizer_type;
      
      ngram_type sentence;

      const int max_order = paths.size();
      
      // every 4096 iterations, we will check for memory boundary
      const size_t iteration_mask = (1 << 13) - 1;
      for (size_t iteration = 0; first != last; ++ first, ++ iteration) {
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
	
	if ((iteration & iteration_mask) == iteration_mask) {
	  size_t num_allocated = 0;
	  MallocExtension::instance()->GetNumericProperty("generic.current_allocated_bytes", &num_allocated);
	  
	  if (num_allocated > size_t(max_malloc * 1024 * 1024 * 1024)) {
	    GoogleNGramCounts::dump_counts(counts, path, paths);
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
      const size_t iteration_mask = (1 << 13) - 1;
      
      for (size_t iteration = 0; first != last; ++ first, ++ iteration) {
	tokenizer_type tokenizer(*first);
	
	tokens.clear();
	tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
	
	if (tokens.size() < 2) continue;
	if (tokens.size() - 1 > max_order) continue;
	
	counts[counts.insert(tokens.begin(), tokens.end() - 1)] += atoll(tokens.back().c_str());
	
	if ((iteration & iteration_mask) == iteration_mask) {
	  size_t num_allocated = 0;
	  MallocExtension::instance()->GetNumericProperty("generic.current_allocated_bytes", &num_allocated);
	  
	  if (num_allocated > size_t(max_malloc * 1024 * 1024 * 1024)) {
	    GoogleNGramCounts::dump_counts(counts, path, paths);
	    counts.clear();
	  }
	}
      }
    }
  };
};

#endif
