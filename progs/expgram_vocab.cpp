//
//  Copyright(C) 2009-2013 Taro Watanabe <taro.watanabe@nict.go.jp>
//

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
#include <boost/algorithm/string/trim.hpp>
#include <boost/functional/hash.hpp>

#include <utils/compress_stream.hpp>

#include "expgram_vocab_impl.hpp"

typedef GoogleNGramCounts::path_type     path_type;
typedef GoogleNGramCounts::path_set_type path_set_type;

typedef GoogleNGramCounts::count_type    count_type;
typedef GoogleNGramCounts::word_type     word_type;
typedef GoogleNGramCounts::vocab_type    vocab_type;
typedef GoogleNGramCounts::ngram_type    ngram_type;

typedef GoogleNGramCounts::ngram_count_set_type count_set_type;

path_set_type corpus_files;
path_set_type counts_files;

path_type corpus_list_file;
path_type counts_list_file;

path_type output_file = "-";

path_type filter_file;

bool map_line = false;
int threads = 2;

int debug = 0;


void accumulate_counts(const path_set_type& paths,
		       const path_type& path_filter,
		       count_set_type& counts,
		       const bool map_line,
		       const int num_threads);
void accumulate_corpus(const path_set_type& paths,
		       const path_type& path_filter,
		       count_set_type& counts,
		       const bool map_line,
		       const int num_threads);
int getoptions(int argc, char** argv);

struct greaterp
{
  greaterp(const count_set_type::const_iterator& __first) : first(__first) {}

  bool operator()(const count_set_type::const_iterator& x, const count_set_type::const_iterator& y) const
  {
    return (*x > *y
	    || (*x == *y
		&& (static_cast<const std::string&>(word_type(word_type::id_type(x - first)))
		    < static_cast<const std::string&>(word_type(word_type::id_type(y - first))))));
  }

  count_set_type::const_iterator first;
};

int main(int argc, char** argv)
{
  try {
    if (getoptions(argc, argv) != 0) 
      return 1;
    
    
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

    count_set_type counts;
    
    if (! counts_files.empty()) {
      if (debug)
	std::cerr << "collect counts from counts" << std::endl;
      accumulate_counts(counts_files, filter_file, counts, map_line, threads);
    }
    
    if (! corpus_files.empty()) {
      if (debug)
	std::cerr << "collect counts from corpus" << std::endl;
      accumulate_corpus(corpus_files, filter_file, counts, map_line, threads);
    }
    
    // sorted output...
    typedef std::vector<count_set_type::const_iterator, std::allocator<count_set_type::const_iterator> > sorted_type;
    
    sorted_type sorted;
    sorted.reserve(counts.size());
    count_set_type::const_iterator citer_begin = counts.begin();
    count_set_type::const_iterator citer_end = counts.end();
    for (count_set_type::const_iterator citer = citer_begin; citer != citer_end; ++ citer)
      sorted.push_back(citer);
    
    std::sort(sorted.begin(), sorted.end(), greaterp(counts.begin()));

    
    utils::compress_ostream os(output_file, 1024 * 1024);
    
    sorted_type::const_iterator siter_end = sorted.end();
    for (sorted_type::const_iterator siter = sorted.begin(); siter != siter_end; ++ siter)
      if (*(*siter))
	os << word_type(word_type::id_type((*siter) - citer_begin)) << '\t' << *(*siter) << '\n';
  }
  catch (std::exception& err) {
    std::cerr << "error: " << err.what() << std::endl;
    return 1;
  }
  return 0;
}

void accumulate_counts(const path_set_type& __paths,
		       const path_type& path_filter,
		       count_set_type& counts,
		       const bool map_line,
		       const int num_threads)
{
  typedef std::vector<count_set_type, std::allocator<count_set_type> > counts_thread_type;
  
  //
  // we will iterate over paths and try to locate count-files in google ngram format...
  //
  
  path_set_type paths;
  
  path_set_type::const_iterator piter_end = __paths.end();
  for (path_set_type::const_iterator piter = __paths.begin(); piter != piter_end; ++ piter) {
    if (*piter != "-" && ! boost::filesystem::exists(*piter))
      throw std::runtime_error(std::string("no file? ") + piter->string());
    
    if (boost::filesystem::is_directory(*piter)) {
      const path_type path = *piter;
      const path_type ngram_dir = path / "1gms";
      
      if (! boost::filesystem::exists(ngram_dir))
	throw std::runtime_error("no directory? " + ngram_dir.string());
      if (! boost::filesystem::is_directory(ngram_dir))
	throw std::runtime_error(std::string("no at directory? ") + ngram_dir.string());
      
      const path_type vocab_file        = ngram_dir / "vocab.gz";
      const path_type vocab_sorted_file = ngram_dir / "vocab_cs.gz";
      
      if (! boost::filesystem::exists(vocab_file))
	throw std::runtime_error(std::string("no vocab? ") + vocab_file.string());
      if (! boost::filesystem::exists(vocab_sorted_file))
	throw std::runtime_error(std::string("no vocab_cs? ") + vocab_sorted_file.string());
      
      paths.push_back(vocab_sorted_file);
    } else
      paths.push_back(*piter);
  }
  
  if (map_line) {
    typedef GoogleNGramCounts::TaskLine<GoogleNGramCounts::TaskCounts> task_type;
    
    typedef task_type::line_set_type       line_set_type;
    typedef task_type::queue_type          queue_type;
    typedef task_type::thread_type         thread_type;
    typedef task_type::thread_ptr_set_type thread_ptr_set_type;
    typedef task_type::subprocess_type     subprocess_type;
    
    thread_ptr_set_type threads(num_threads);
    queue_type          queue(num_threads * 8);
    counts_thread_type  counts_thread(num_threads);
    
    std::vector<boost::shared_ptr<subprocess_type> > subprocess(path_filter.empty() ? 0 : num_threads);
    for (size_t shard = 0; shard != subprocess.size(); ++ shard)
      subprocess[shard].reset(new subprocess_type(path_filter));
    
    for (int shard = 0; shard < num_threads; ++ shard) {
      if (subprocess.empty())
	threads[shard].reset(new thread_type(task_type(queue, counts_thread[shard])));
      else
	threads[shard].reset(new thread_type(task_type(queue, *subprocess[shard], counts_thread[shard])));
    }
    
    line_set_type lines;
    
    path_set_type::const_iterator piter_end = paths.end();
    for (path_set_type::const_iterator piter = paths.begin(); piter != piter_end; ++ piter) {
      if (*piter != "-" && ! boost::filesystem::exists(*piter))
	throw std::runtime_error(std::string("no file? ") + piter->string());

      if (debug)
	std::cerr << "file: " << piter->string() << std::endl;
      
      utils::compress_istream is(*piter, 1024 * 1024);
      std::string line;
      while (std::getline(is, line))
	if (! line.empty()) {
	  lines.push_back(line);
	  
	  if (lines.size() >= 1024 * 32) {
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
      
      count_set_type& counts_shard = counts_thread[shard];

      if (counts.empty())
	counts.swap(counts_shard);
      else {
	counts.resize(utils::bithack::max(counts.size(), counts_shard.size()), 0);
	
	std::transform(counts.begin(), counts.begin() + counts_shard.size(), counts_shard.begin(), counts.begin(), std::plus<count_type>());
      }
    }
    
  } else {
    typedef GoogleNGramCounts::TaskFile<GoogleNGramCounts::TaskCounts> task_type;
    
    typedef task_type::queue_type          queue_type;
    typedef task_type::thread_type         thread_type;
    typedef task_type::thread_ptr_set_type thread_ptr_set_type;
    typedef task_type::subprocess_type     subprocess_type;
    
    thread_ptr_set_type threads(num_threads);
    queue_type          queue(num_threads);
    counts_thread_type  counts_thread(num_threads);
    
    std::vector<boost::shared_ptr<subprocess_type> > subprocess(path_filter.empty() ? 0 : num_threads);
    for (size_t shard = 0; shard != subprocess.size(); ++ shard)
      subprocess[shard].reset(new subprocess_type(path_filter));
    
    for (int shard = 0; shard < num_threads; ++ shard) {
      if (subprocess.empty())
	threads[shard].reset(new thread_type(task_type(queue, counts_thread[shard])));
      else
	threads[shard].reset(new thread_type(task_type(queue, *subprocess[shard], counts_thread[shard])));
    }
  
    path_set_type::const_iterator piter_end = paths.end();
    for (path_set_type::const_iterator piter = paths.begin(); piter != piter_end; ++ piter) {
      if (*piter != "-" && ! boost::filesystem::exists(*piter))
	throw std::runtime_error(std::string("no file? ") + piter->string());
      
      if (debug)
	std::cerr << "file: " << piter->string() << std::endl;

      queue.push(*piter);
    }
    
    for (int shard = 0; shard < num_threads; ++ shard)
      queue.push(path_type());
    
    for (int shard = 0; shard < num_threads; ++ shard) {
      threads[shard]->join();
      
      count_set_type& counts_shard = counts_thread[shard];

      if (counts.empty())
	counts.swap(counts_shard);
      else {
	counts.resize(utils::bithack::max(counts.size(), counts_shard.size()), 0);
	
	std::transform(counts.begin(), counts.begin() + counts_shard.size(), counts_shard.begin(), counts.begin(), std::plus<count_type>());
      }
    }
  }
}

void accumulate_corpus(const path_set_type& paths,
		       const path_type& path_filter,
		       count_set_type& counts,
		       const bool map_line,
		       const int num_threads)
{
  typedef std::vector<count_set_type, std::allocator<count_set_type> > counts_thread_type;
  
  if (map_line) {
    typedef GoogleNGramCounts::TaskLine<GoogleNGramCounts::TaskCorpus> task_type;
    
    typedef task_type::line_set_type       line_set_type;
    typedef task_type::queue_type          queue_type;
    typedef task_type::thread_type         thread_type;
    typedef task_type::thread_ptr_set_type thread_ptr_set_type;
    typedef task_type::subprocess_type     subprocess_type;
    
    thread_ptr_set_type threads(num_threads);
    queue_type          queue(num_threads * 8);
    counts_thread_type  counts_thread(num_threads);

    std::vector<boost::shared_ptr<subprocess_type> > subprocess(path_filter.empty() ? 0 : num_threads);
    for (size_t shard = 0; shard != subprocess.size(); ++ shard)
      subprocess[shard].reset(new subprocess_type(path_filter));
    
    for (int shard = 0; shard < num_threads; ++ shard) {
      if (subprocess.empty())
	threads[shard].reset(new thread_type(task_type(queue, counts_thread[shard])));
      else
	threads[shard].reset(new thread_type(task_type(queue, *subprocess[shard], counts_thread[shard])));
    }

    line_set_type lines;
    
    path_set_type::const_iterator piter_end = paths.end();
    for (path_set_type::const_iterator piter = paths.begin(); piter != piter_end; ++ piter) {
      if (*piter != "-" && ! boost::filesystem::exists(*piter))
	throw std::runtime_error(std::string("no file? ") + piter->string());
      
      if (debug)
	std::cerr << "file: " << piter->string() << std::endl;
      
      utils::compress_istream is(*piter, 1024 * 1024);
      std::string line;
      while (std::getline(is, line))
	if (! line.empty()) {
	  lines.push_back(line);
	  
	  if (lines.size() >= 1024 * 8) {
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
      
      count_set_type& counts_shard = counts_thread[shard];

      if (counts.empty())
	counts.swap(counts_shard);
      else {
	counts.resize(utils::bithack::max(counts.size(), counts_shard.size()), 0);
	
	std::transform(counts.begin(), counts.begin() + counts_shard.size(), counts_shard.begin(), counts.begin(), std::plus<count_type>());
      }
    }
  } else {
    typedef GoogleNGramCounts::TaskFile<GoogleNGramCounts::TaskCorpus> task_type;
    
    typedef task_type::queue_type          queue_type;
    typedef task_type::thread_type         thread_type;
    typedef task_type::thread_ptr_set_type thread_ptr_set_type;
    typedef task_type::subprocess_type     subprocess_type;
    
    thread_ptr_set_type threads(num_threads);
    queue_type          queue(num_threads);
    counts_thread_type  counts_thread(num_threads);
    
    std::vector<boost::shared_ptr<subprocess_type> > subprocess(path_filter.empty() ? 0 : num_threads);
    for (size_t shard = 0; shard != subprocess.size(); ++ shard)
      subprocess[shard].reset(new subprocess_type(path_filter));
    
    for (int shard = 0; shard < num_threads; ++ shard) {
      if (subprocess.empty())
	threads[shard].reset(new thread_type(task_type(queue, counts_thread[shard])));
      else
	threads[shard].reset(new thread_type(task_type(queue, *subprocess[shard], counts_thread[shard])));
    }
    
    path_set_type::const_iterator piter_end = paths.end();
    for (path_set_type::const_iterator piter = paths.begin(); piter != piter_end; ++ piter) {
      if (*piter != "-" && ! boost::filesystem::exists(*piter))
	throw std::runtime_error(std::string("no file? ") + piter->string());
      
      if (debug)
	std::cerr << "file: " << piter->string() << std::endl;

      queue.push(*piter);
    }
    
    for (int shard = 0; shard < num_threads; ++ shard)
      queue.push(path_type());
    
    for (int shard = 0; shard < num_threads; ++ shard) {
      threads[shard]->join();
      
      count_set_type& counts_shard = counts_thread[shard];

      if (counts.empty())
	counts.swap(counts_shard);
      else {
	counts.resize(utils::bithack::max(counts.size(), counts_shard.size()), 0);
	
	std::transform(counts.begin(), counts.begin() + counts_shard.size(), counts_shard.begin(), counts.begin(), std::plus<count_type>());
      }
    }
  }
}


int getoptions(int argc, char** argv)
{
  namespace po = boost::program_options;
  
  po::options_description desc("options");
  desc.add_options()
    ("corpus",       po::value<path_set_type>(&corpus_files)->multitoken(),  "corpus file(s)")
    ("counts",       po::value<path_set_type>(&counts_files)->multitoken(),  "counts file(s)")
    
    ("corpus-list",  po::value<path_type>(&corpus_list_file),  "corpus list file")
    ("counts-list",  po::value<path_type>(&counts_list_file),  "counts list file")
    
    ("output",       po::value<path_type>(&output_file)->default_value(output_file), "output file")
    
    ("filter", po::value<path_type>(&filter_file), "filtering script")
    
    ("map-line",   po::bool_switch(&map_line),     "map by lines, not by files")
    ("threads",    po::value<int>(&threads),       "# of threads")
    
    ("debug", po::value<int>(&debug)->implicit_value(1), "debug level")
    ("help", "help message");
  
  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc, po::command_line_style::unix_style & (~po::command_line_style::allow_guessing)), vm);
  po::notify(vm);
  
  if (vm.count("help")) {
    std::cout << argv[0] << " [options]" << '\n' << desc << '\n';
    return 1;
  }
  
  return 0;
}
