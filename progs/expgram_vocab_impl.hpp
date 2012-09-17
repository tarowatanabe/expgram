// -*- mode: c++ -*-
//
//  Copyright(C) 2009-2012 Taro Watanabe <taro.watanabe@nict.go.jp>
//

#ifndef __NGRAM_VOCAB_IMPL__HPP__
#define __NGRAM_VOCAB_IMPL__HPP__ 1

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

#include <utils/space_separator.hpp>
#include <utils/compress_stream.hpp>
#include <utils/compact_trie.hpp>
#include <utils/bithack.hpp>
#include <utils/lockfree_queue.hpp>
#include <utils/lockfree_list_queue.hpp>
#include <utils/istream_line_iterator.hpp>
#include <utils/tempfile.hpp>
#include <utils/subprocess.hpp>
#include <utils/async_device.hpp>
#include <utils/malloc_stats.hpp>
#include <utils/lexical_cast.hpp>

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
  
  typedef std::vector<count_type, std::allocator<count_type> > ngram_count_set_type;
  
  typedef utils::subprocess subprocess_type;
  
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
    ngram_count_set_type& counts;
    
    TaskLine(queue_type&      _queue,
	     subprocess_type& _subprocess,
	     ngram_count_set_type& _counts)
      : queue(_queue),
	subprocess(&_subprocess),
	counts(_counts) {}

    TaskLine(queue_type&      _queue,
	     ngram_count_set_type& _counts)
      : queue(_queue),
	subprocess(0),
	counts(_counts) {}

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
      line_set_type lines;
      
      if (subprocess) {
	thread_type thread(SubTask(*subprocess, queue));
	
	boost::iostreams::filtering_istream is;
#if BOOST_VERSION >= 104400
	is.push(boost::iostreams::file_descriptor_source(subprocess->desc_read(), boost::iostreams::close_handle));
#else
	is.push(boost::iostreams::file_descriptor_source(subprocess->desc_read(), true));
#endif
	
	__task(utils::istream_line_iterator(is), utils::istream_line_iterator(), counts);
	
	thread.join();
      } else {
	while (1) {
	  queue.pop_swap(lines);
	  if (lines.empty()) break;
	  
	  __task(lines.begin(), lines.end(), counts);
	}
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
    ngram_count_set_type& counts;
    
    TaskFile(queue_type&      _queue,
	     subprocess_type& _subprocess,
	     ngram_count_set_type& _counts)
      : queue(_queue),
	subprocess(&_subprocess),
	counts(_counts) {}

    TaskFile(queue_type&      _queue,
	     ngram_count_set_type& _counts)
      : queue(_queue),
	subprocess(0),
	counts(_counts) {}

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
      path_type file;
      
      if (subprocess) {
	thread_type thread(SubTask(*subprocess, queue));
	
	boost::iostreams::filtering_istream is;
#if BOOST_VERSION >= 104400
	is.push(boost::iostreams::file_descriptor_source(subprocess->desc_read(), boost::iostreams::close_handle));
#else
	is.push(boost::iostreams::file_descriptor_source(subprocess->desc_read(), true));
#endif
	
	__task(utils::istream_line_iterator(is), utils::istream_line_iterator(), counts);
	
	thread.join();
      } else {
	while (1) {
	  queue.pop(file);
	  if (file.empty()) break;
	  
	  if (file != "-" && ! boost::filesystem::exists(file))
	    throw std::runtime_error(std::string("no file? ") + file.string());

	  utils::compress_istream is(file, 1024 * 1024);
	  
	  __task(utils::istream_line_iterator(is), utils::istream_line_iterator(), counts);
	}
      }
    }
  };
  
  struct TaskCorpus
  {
    template <typename Iterator, typename Counts>
    inline
    void operator()(Iterator first, Iterator last, Counts& counts)
    {
      typedef boost::tokenizer<utils::space_separator, utils::piece::const_iterator, utils::piece> tokenizer_type;
      
      ngram_type sentence;
      
      // every 4096 iterations, we will check for memory boundary
      for (/**/; first != last; ++ first) {
	utils::piece line_piece(*first);
	tokenizer_type tokenizer(line_piece);
	
	sentence.clear();
	sentence.push_back(vocab_type::BOS);
	sentence.insert(sentence.end(), tokenizer.begin(), tokenizer.end());
	sentence.push_back(vocab_type::EOS);
	
	if (sentence.size() == 2) continue;
	
	ngram_type::const_iterator siter_begin = sentence.begin();
	ngram_type::const_iterator siter_end   = sentence.end();
	for (ngram_type::const_iterator siter = siter_begin; siter != siter_end; ++ siter) {
	  if (siter->id() >= counts.size())
	    counts.resize(siter->id() + 1, 0);
	  
	  ++ counts[siter->id()];
	}
      }
    }
  };
  
  struct TaskCounts
  {
    
    const std::string& escape_word(const std::string& word)
    {
      static const std::string& __BOS = static_cast<const std::string&>(vocab_type::BOS);
      static const std::string& __EOS = static_cast<const std::string&>(vocab_type::EOS);
      static const std::string& __UNK = static_cast<const std::string&>(vocab_type::UNK);
      
      if (strcasecmp(word.c_str(), __BOS.c_str()) == 0)
	return __BOS;
      else if (strcasecmp(word.c_str(), __EOS.c_str()) == 0)
	return __EOS;
      else if (strcasecmp(word.c_str(), __UNK.c_str()) == 0)
	return __UNK;
      else
	return word;
    }

    template <typename Iterator, typename Counts>
    inline
    void operator()(Iterator first, Iterator last, Counts& counts)
    {
      typedef std::vector<utils::piece, std::allocator<utils::piece> > tokens_type;
      typedef boost::tokenizer<utils::space_separator, utils::piece::const_iterator, utils::piece> tokenizer_type;
      
      tokens_type    tokens;
      
      for (/**/; first != last; ++ first) {
	utils::piece line_piece(*first);
	tokenizer_type tokenizer(line_piece);
	
	tokens.clear();
	tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
	
	if (tokens.size() != 2) continue;
	
	const word_type word = escape_word(tokens.front());
	
	if (word.id() >= counts.size())
	  counts.resize(word.id() + 1, 0);
	
	counts[word.id()] += utils::lexical_cast<count_type>(tokens.back());
      }
    }
  };
};

#endif
