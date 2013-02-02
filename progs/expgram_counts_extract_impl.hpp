// -*- mode: c++ -*-
//
//  Copyright(C) 2009-2012 Taro Watanabe <taro.watanabe@nict.go.jp>
//

#ifndef __NGRAM_COUNTS_EXTRACT_IMPL__HPP__
#define __NGRAM_COUNTS_EXTRACT_IMPL__HPP__ 1

#include <fcntl.h>

#include <cstring>
#include <cerrno>

#include <memory>
#include <sstream>
#include <iostream>
#include <vector>
#include <string>
#include <iterator>
#include <algorithm>

#include <map>

#include <boost/version.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>
#include <boost/filesystem.hpp>
#include <boost/tokenizer.hpp>
#include <boost/functional/hash.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/device/file_descriptor.hpp>

#include <utils/lexical_cast.hpp>
#include <utils/space_separator.hpp>
#include <utils/compress_stream.hpp>
#include <utils/trie_compact.hpp>
#include <utils/bithack.hpp>
#include <utils/lockfree_queue.hpp>
#include <utils/lockfree_list_queue.hpp>
#include <utils/istream_line_iterator.hpp>
#include <utils/tempfile.hpp>
#include <utils/subprocess.hpp>
#include <utils/async_device.hpp>
#include <utils/malloc_stats.hpp>
#include <utils/hashmurmur.hpp>
#include <utils/piece.hpp>
#include <utils/unordered_set.hpp>

#include <expgram/Word.hpp>
#include <expgram/Vocab.hpp>
#include <expgram/Sentence.hpp>

struct GoogleNGramCounts
{
  typedef uint64_t          count_type;
  typedef expgram::Word     word_type;
  typedef expgram::Vocab    vocab_type;
  typedef expgram::Sentence ngram_type;

  typedef boost::filesystem::path                                     path_type;
  typedef std::vector<path_type, std::allocator<path_type> >          path_set_type;
  typedef std::vector<path_set_type, std::allocator<path_set_type> >  path_map_type;
  
  struct unassigned
  {
    word_type operator()() const { return word_type(); }
  };

  typedef utils::trie_compact<word_type, count_type, unassigned, boost::hash<word_type>, std::equal_to<word_type>,
			      std::allocator<std::pair<const word_type, count_type> > > ngram_count_set_type;

  typedef utils::subprocess subprocess_type;


  struct string_hash : public utils::hashmurmur<size_t>
  {
    typedef utils::hashmurmur<size_t> hasher_type;

    size_t operator()(const std::string& word) const
    {
      return hasher_type::operator()(word.begin(), word.end(), 0);
    }
  };
  
  typedef utils::unordered_set<std::string, string_hash, std::equal_to<std::string> >::type vocabulary_type;
  
  template <typename Task>
  struct TaskLine
  {
    typedef std::string line_type;
    typedef std::vector<line_type, std::allocator<line_type> > line_set_type;
    
    typedef utils::lockfree_list_queue<line_set_type, std::allocator<line_set_type> >  queue_type;
    
    typedef boost::thread                                                  thread_type;
    typedef boost::shared_ptr<thread_type>                                 thread_ptr_type;
    typedef std::vector<thread_ptr_type, std::allocator<thread_ptr_type> > thread_ptr_set_type;
    
    typedef utils::subprocess subprocess_type;
    
    typedef Task task_type;
    
    queue_type&      queue;
    subprocess_type* subprocess;
    const vocabulary_type& vocabulary;
    path_type        path;
    path_map_type&   paths;
    double           max_malloc;
    
    TaskLine(queue_type&      _queue,
	     subprocess_type& _subprocess,
	     const vocabulary_type& _vocabulary,
	     const path_type& _path,
	     path_map_type&   _paths,
	     const double     _max_malloc)
      : queue(_queue),
	subprocess(&_subprocess),
	vocabulary(_vocabulary),
	path(_path),
	paths(_paths),
	max_malloc(_max_malloc) {}

    TaskLine(queue_type&      _queue,
	     const vocabulary_type& _vocabulary,
	     const path_type& _path,
	     path_map_type&   _paths,
	     const double     _max_malloc)
      : queue(_queue),
	subprocess(0),
	vocabulary(_vocabulary),	
	path(_path),
	paths(_paths),
	max_malloc(_max_malloc) {}

    struct SubTask
    {
      utils::subprocess& subprocess;
      queue_type& queue;
      
      SubTask(utils::subprocess& _subprocess,
	      queue_type& _queue)
	: subprocess(_subprocess), queue(_queue) {}
      
      void operator()()
      {
	line_set_type lines;

	boost::iostreams::filtering_ostream os;
#if BOOST_VERSION >= 104400
	os.push(boost::iostreams::file_descriptor_sink(subprocess.desc_write(), boost::iostreams::close_handle));
#else
	os.push(boost::iostreams::file_descriptor_sink(subprocess.desc_write(), true));
#endif
	
	while (1) {
	  queue.pop_swap(lines);
	  if (lines.empty()) break;
	  
	  std::copy(lines.begin(), lines.end(), std::ostream_iterator<std::string>(os, "\n"));
	}
	
	os.pop();
      }
    };
    
    void operator()()
    {
      task_type __task;
      ngram_count_set_type counts;
      line_set_type lines;
      
      if (subprocess) {
	thread_type thread(SubTask(*subprocess, queue));
	
	boost::iostreams::filtering_istream is;
#if BOOST_VERSION >= 104400
	is.push(boost::iostreams::file_descriptor_source(subprocess->desc_read(), boost::iostreams::close_handle));
#else
	is.push(boost::iostreams::file_descriptor_source(subprocess->desc_read(), true));
#endif
	
	__task(utils::istream_line_iterator(is), utils::istream_line_iterator(), vocabulary, counts, path, paths, max_malloc);
	
	thread.join();
      } else {
	const size_t iteration_mask = (1 << 2) - 1;
	for (size_t iteration = 0;/**/; ++ iteration)  {
	  queue.pop_swap(lines);
	  if (lines.empty()) break;
	  
	  __task(lines.begin(), lines.end(), vocabulary, counts, path, paths, max_malloc);
	  
	  if ((iteration & iteration_mask) == iteration_mask
	      && ! counts.empty()
	      && utils::malloc_stats::used() > size_t(max_malloc * 1024 * 1024 * 1024)) {
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
    typedef utils::lockfree_list_queue<path_type, std::allocator<path_type> >   queue_type;
    
    typedef boost::thread                                                  thread_type;
    typedef boost::shared_ptr<thread_type>                                 thread_ptr_type;
    typedef std::vector<thread_ptr_type, std::allocator<thread_ptr_type> > thread_ptr_set_type;

    typedef utils::subprocess subprocess_type;
    
    typedef Task task_type;
    
    queue_type&      queue;
    subprocess_type* subprocess;
    const vocabulary_type& vocabulary;    
    path_type        path;
    path_map_type&   paths;
    double           max_malloc;
    
    TaskFile(queue_type&      _queue,
	     subprocess_type& _subprocess,
	     const vocabulary_type& _vocabulary,
	     const path_type& _path,
	     path_map_type&   _paths,
	     const double     _max_malloc)
      : queue(_queue),
	subprocess(&_subprocess),
	vocabulary(_vocabulary),
	path(_path),
	paths(_paths),
	max_malloc(_max_malloc) {}

    TaskFile(queue_type&      _queue,
	     const vocabulary_type& _vocabulary,
	     const path_type& _path,
	     path_map_type&   _paths,
	     const double     _max_malloc)
      : queue(_queue),
	subprocess(0),
	vocabulary(_vocabulary),
	path(_path),
	paths(_paths),
	max_malloc(_max_malloc) {}

    struct SubTask
    {
      utils::subprocess& subprocess;
      queue_type& queue;
      
      SubTask(utils::subprocess& _subprocess,
	      queue_type& _queue)
	: subprocess(_subprocess), queue(_queue) {}
      
      void operator()()
      {
	path_type file;
	
	boost::iostreams::filtering_ostream os;
#if BOOST_VERSION >= 104400
	os.push(boost::iostreams::file_descriptor_sink(subprocess.desc_write(), boost::iostreams::close_handle));
#else
	os.push(boost::iostreams::file_descriptor_sink(subprocess.desc_write(), true));
#endif
	
	while (1) {
	  queue.pop(file);
	  if (file.empty()) break;
	  
	  if (file != "-" && ! boost::filesystem::exists(file))
	    throw std::runtime_error(std::string("no file? ") + file.string());
	  
	  char buffer[4096];
	  utils::compress_istream is(file, 1024 * 1024);
	  
	  do {
	    is.read(buffer, 4096);
	    if (is.gcount() > 0)
	      os.write(buffer, is.gcount());
	  } while(is);
	}
	
	os.pop();
      }
    };
    
    void operator()()
    {
      task_type __task;
      ngram_count_set_type counts;
      path_type file;
      
      if (subprocess) {
	thread_type thread(SubTask(*subprocess, queue));
	
	boost::iostreams::filtering_istream is;
#if BOOST_VERSION >= 104400
	is.push(boost::iostreams::file_descriptor_source(subprocess->desc_read(), boost::iostreams::close_handle));
#else
	is.push(boost::iostreams::file_descriptor_source(subprocess->desc_read(), true));
#endif
	
	__task(utils::istream_line_iterator(is), utils::istream_line_iterator(), vocabulary, counts, path, paths, max_malloc);
	
	thread.join();
      } else {
	while (1) {
	  queue.pop(file);
	  if (file.empty()) break;
	  
	  if (file != "-" && ! boost::filesystem::exists(file))
	    throw std::runtime_error(std::string("no file? ") + file.string());

	  utils::compress_istream is(file, 1024 * 1024);
	  
	  __task(utils::istream_line_iterator(is), utils::istream_line_iterator(), vocabulary, counts, path, paths, max_malloc);
	  
	  if (! counts.empty() && utils::malloc_stats::used() > size_t(max_malloc * 1024 * 1024 * 1024)) {
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

  template <typename Tp>
  struct less_pfirst_vocab
  {
    bool operator()(const Tp* x, const Tp* y) const
    {
      const std::string& wordx = static_cast<const std::string&>(x->first);
      const std::string& wordy = static_cast<const std::string&>(y->first);

      return wordx < wordy;
    }
  };

  template <typename Prefix, typename Counts, typename Iterator, typename Path, typename PathIterator, typename StreamIterator>
  static inline
  void dump_counts(const Prefix& prefix,
		   const Counts& counts,
		   Iterator first, Iterator last,
		   const Path& path,
		   PathIterator path_iter, StreamIterator stream_iter)
  {
    namespace karma = boost::spirit::karma;
    namespace standard = boost::spirit::standard;

    typedef typename std::iterator_traits<Iterator>::value_type value_type;
    typedef std::vector<const value_type*, std::allocator<const value_type*> > value_set_type;
    
    karma::uint_generator<count_type> count_generator;
    
    value_set_type values;
    for (Iterator iter = first; iter != last; ++ iter)
      values.push_back(&(*iter));
    
    std::sort(values.begin(), values.end(), less_pfirst_vocab<value_type>());
    
    const int order = prefix.size() + 1;
    
    if (! (*stream_iter)) {
      std::ostringstream stream_dir;
      stream_dir << order << "gms";
      
      std::ostringstream stream_name;
      stream_name << order << "gm-XXXXXX";
      
      const Path ngram_dir = path / stream_dir.str();
      const Path counts_file_tmp = utils::tempfile::file_name(ngram_dir / stream_name.str());
      utils::tempfile::insert(counts_file_tmp);
      const Path counts_file = counts_file_tmp.string() + ".gz";
      utils::tempfile::insert(counts_file);
      
      stream_iter->reset(new utils::compress_ostream(counts_file, 1024 * 1024));
      path_iter->push_back(counts_file);
    }
    
    Prefix prefix_new(prefix.size() + 1);
    std::copy(prefix.begin(), prefix.end(), prefix_new.begin());
    
    // dump counts...
    typename value_set_type::const_iterator iter_end = values.end();
    for (typename value_set_type::const_iterator iter = values.begin(); iter != iter_end; ++ iter) {
      
      const count_type count = counts[(*iter)->second];
      
      if (count > 0) {
	std::ostream_iterator<char> streamiter(*(*stream_iter));
	
	if (! karma::generate(streamiter,
			      *(standard::string << ' ') << standard::string << '\t' << count_generator << '\n',
			      prefix,
			      (*iter)->first,
			      count))
	  throw std::runtime_error("generation failed");
	
#if 0
	typename Prefix::const_iterator piter_end = prefix.end();
	for (typename Prefix::const_iterator piter = prefix.begin(); piter != piter_end ; ++ piter)
	  *(*stream_iter) << *piter << ' ';
	
	*(*stream_iter) << (*iter)->first << '\t' << count << '\n';
#endif
      }
      
      // recursive call...
      if (! counts.empty((*iter)->second)) {
	prefix_new.back() = (*iter)->first;
	dump_counts(prefix_new, counts, counts.begin((*iter)->second), counts.end((*iter)->second), path, path_iter + 1, stream_iter + 1);
      }
    }
  }
  
  template <typename Counts, typename Path, typename Paths>
  static inline
  void dump_counts(const Counts& counts, const Path& path, Paths& paths)
  {
    typedef boost::shared_ptr<utils::compress_ostream> ostream_ptr_type;
    typedef std::vector<ostream_ptr_type, std::allocator<ostream_ptr_type> > ostream_ptr_set_type;
    
    
    typedef ngram_type prefix_type;
    
    const int max_order = paths.size();
    
    ostream_ptr_set_type ostreams(max_order);
    prefix_type    prefix;
    
    // recursive call....
    dump_counts(prefix,
		counts, 
		counts.begin(), counts.end(),
		path,
		paths.begin(),
		ostreams.begin());
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
    
    typedef std::vector<utils::piece, std::allocator<utils::piece> > tokens_type;
    typedef boost::tokenizer<utils::space_separator, utils::piece::const_iterator, utils::piece> tokenizer_type;
    
    if (paths_counts.empty())
      throw std::runtime_error("no counts?");
    
    const int max_order = paths_counts.size();

    // if no unigram counts, return...
    if (paths_counts.front().empty()) return;
    
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
	  throw std::runtime_error(std::string("no unigramcounts? ") + piter->string());
	
	utils::compress_istream is(*piter, 1024 * 1024);
	std::string line;
	tokens_type tokens;
	while (std::getline(is, line)) {
	  utils::piece line_piece(line);
	  tokenizer_type tokenizer(line_piece);
	  
	  tokens.clear();
	  tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
	  
	  if (tokens.size() != 2) continue;
	  
	  words[static_cast<std::string>(tokens.front())] += utils::lexical_cast<count_type>(tokens.back());
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
	  throw std::runtime_error(std::string("no unigramcounts? ") + piter->string());
	
	utils::compress_istream is(*piter);
	
	tokens.clear();
	while (std::getline(is, line)) {
	  utils::piece line_piece(line);
	  tokenizer_type tokenizer(line_piece);
	  
	  tokens.clear();
	  tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
	  
	  if (tokens.empty()) continue;
	  
	  if (static_cast<int>(tokens.size()) != order + 1)
	    throw std::runtime_error("invalid google's ngram structure...");
	  
	  break;
	}

	if (! tokens.empty()) {
	  os << path_type(piter->leaf()).string() << '\t';
	  std::copy(tokens.begin(), tokens.end() - 2, std::ostream_iterator<utils::piece>(os, " "));
	  os << *(tokens.end() - 2) << '\n';
	  
	  utils::tempfile::erase(*piter);
	}
	
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
    
    typedef std::vector<utils::piece, std::allocator<utils::piece> > tokens_type;
    typedef boost::tokenizer<utils::space_separator, utils::piece::const_iterator, utils::piece> tokenizer_type;

    if (! boost::filesystem::exists(path))
      throw std::runtime_error(std::string("no file? ") + path.string());
    if (! boost::filesystem::is_directory(path))
      throw std::runtime_error(std::string("no a directory? ") + path.string());
  
    for (int order = 1; order <= max_order; ++ order) {
    
      std::ostringstream stream_ngram;
      stream_ngram << order << "gms";
    
      std::ostringstream stream_index;
      stream_index << order << "gm.idx";

      const path_type ngram_dir = path / stream_ngram.str();
      const path_type index_file = ngram_dir / stream_index.str();

      if (! boost::filesystem::exists(ngram_dir)) break;
      if (! boost::filesystem::is_directory(ngram_dir))
	throw std::runtime_error(std::string("no a directory? ") + ngram_dir.string());
    
      if (order == 1) {
	const path_type vocab_file        = ngram_dir / "vocab.gz";
	const path_type vocab_sorted_file = ngram_dir / "vocab_cs.gz";
      
	if (! boost::filesystem::exists(vocab_file))
	  throw std::runtime_error(std::string("no vocab.gz? ") + vocab_file.string());
	if (! boost::filesystem::exists(vocab_sorted_file))
	  throw std::runtime_error(std::string("no vocab_cs.gz? ") + vocab_sorted_file.string());
      
	paths.push_back(vocab_file);
      } else {
      
	if (! boost::filesystem::exists(index_file))
	  throw std::runtime_error(std::string("no index file? ") + index_file.string());
      
	utils::compress_istream is(index_file);
	std::string line;
	tokens_type tokens;
      
	while (std::getline(is, line)) {
	  utils::piece line_piece(line);
	  tokenizer_type tokenizer(line_piece);
	
	  tokens.clear();
	  tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
	
	  if (tokens.empty()) continue;
	
	  if (static_cast<int>(tokens.size()) != order + 1)
	    throw std::runtime_error(std::string("invalid google ngram format...") + index_file.string());
	
	  const path_type path_ngram = ngram_dir / static_cast<std::string>(tokens.front());
	
	  if (! boost::filesystem::exists(path_ngram))
	    throw std::runtime_error(std::string("invalid google ngram format... no file: ") + path_ngram.string());
	
	  paths.push_back(path_ngram);
	}
      }
    }
  }

  
  struct TaskCorpus
  {
    template <typename Iterator, typename Counts, typename Path, typename Paths>
    inline
    void operator()(Iterator first, Iterator last, const vocabulary_type& vocabulary, Counts& counts, const Path& path, Paths& paths, const double max_malloc)
    {
      typedef boost::tokenizer<utils::space_separator, utils::piece::const_iterator, utils::piece> tokenizer_type;
      
      ngram_type sentence;
      
      const int max_order = paths.size();
      
      // every 1024 * 8 iterations, we will check for memory boundary
      const size_t iteration_mask = (1 << 13) - 1;
      for (size_t iteration = 0; first != last; ++ first, ++ iteration) {
	utils::piece line_piece(*first);
	tokenizer_type tokenizer(line_piece);
	
	if (! vocabulary.empty()) {
	  sentence.clear();
	  sentence.push_back(vocab_type::BOS);
	  
	  tokenizer_type::iterator titer_end = tokenizer.end();
	  for (tokenizer_type::iterator titer = tokenizer.begin(); titer != titer_end; ++ titer) {
	    const std::string token = *titer;
	    if (vocabulary.find(token) == vocabulary.end())
	      sentence.push_back(vocab_type::UNK);
	    else
	      sentence.push_back(token);
	  }
	  
	  sentence.push_back(vocab_type::EOS);
	} else {
	  sentence.clear();
	  sentence.push_back(vocab_type::BOS);
	  sentence.insert(sentence.end(), tokenizer.begin(), tokenizer.end());
	  sentence.push_back(vocab_type::EOS);
	}
	
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
	
	if ((iteration & iteration_mask) == iteration_mask && utils::malloc_stats::used() > size_t(max_malloc * 1024 * 1024 * 1024)) {
	  GoogleNGramCounts::dump_counts(counts, path, paths);
	  counts.clear();
	}
      }
    }
  };
  
  struct TaskCounts
  {
    
    expgram::Word escape_word(const utils::piece& __word)
    {
      static const std::string& __BOS = static_cast<const std::string&>(vocab_type::BOS);
      static const std::string& __EOS = static_cast<const std::string&>(vocab_type::EOS);
      static const std::string& __UNK = static_cast<const std::string&>(vocab_type::UNK);
      
      const utils::ipiece word(__word);
      
      if (word == __BOS)
	return vocab_type::BOS;
      else if (word == __EOS)
	return vocab_type::EOS;
      else if (word == __UNK)
	return vocab_type::UNK;
      else
	return __word;
    }

    template <typename Iterator, typename Counts, typename Path, typename Paths>
    inline
    void operator()(Iterator first, Iterator last, const vocabulary_type& vocabulary, Counts& counts, const Path& path, Paths& paths, const double max_malloc)
    {
      typedef std::vector<utils::piece, std::allocator<utils::piece> > tokens_type;
      typedef boost::tokenizer<utils::space_separator, utils::piece::const_iterator, utils::piece> tokenizer_type;
      
      tokens_type    tokens;
      
      const int max_order = paths.size();
      
      // every 1024 * 32 iterations, we will check for memory boundary
      const size_t iteration_mask = (1 << 15) - 1;
      
      for (size_t iteration = 0; first != last; ++ first, ++ iteration) {
	utils::piece line_piece(*first);
	tokenizer_type tokenizer(line_piece);
	
	tokens.clear();
	tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
	
	if (static_cast<int>(tokens.size()) < 2) continue;
	if (static_cast<int>(tokens.size()) - 1 > max_order) continue;
	
	
	ngram_count_set_type::id_type id = counts.root();
	
	if (! vocabulary.empty()) {
	  tokens_type::iterator titer_end = tokens.end() - 1;
	  for (tokens_type::iterator titer = tokens.begin(); titer != titer_end; ++ titer) {
	    expgram::Word word = escape_word(*titer);
	    if (vocabulary.find(word) == vocabulary.end())
	      word = vocab_type::UNK;
	    id = counts.insert(id, word);
	  }
	} else {
	  tokens_type::iterator titer_end = tokens.end() - 1;
	  for (tokens_type::iterator titer = tokens.begin(); titer != titer_end; ++ titer)
	    id = counts.insert(id, escape_word(*titer));
	}
	
	counts[id] += utils::lexical_cast<count_type>(tokens.back());
	
	if ((iteration & iteration_mask) == iteration_mask && utils::malloc_stats::used() > size_t(max_malloc * 1024 * 1024 * 1024)) {
	  GoogleNGramCounts::dump_counts(counts, path, paths);
	  counts.clear();
	}
      }
    }
  };
};

#endif
