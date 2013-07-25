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

#include "expgram_counts_extract_impl.hpp"

typedef GoogleNGramCounts::path_type     path_type;
typedef GoogleNGramCounts::path_set_type path_set_type;
typedef GoogleNGramCounts::path_map_type path_map_type;

typedef GoogleNGramCounts::count_type    count_type;
typedef GoogleNGramCounts::word_type     word_type;
typedef GoogleNGramCounts::vocab_type    vocab_type;
typedef GoogleNGramCounts::ngram_type    ngram_type;

typedef GoogleNGramCounts::vocabulary_type vocabulary_type;

path_set_type corpus_files;
path_set_type counts_files;

path_type corpus_list_file;
path_type counts_list_file;

path_type vocab_file;

path_type output_file;

path_type filter_file;

int max_order = 5;

bool map_line = false;
int threads = 2;
double max_malloc = 1.0; // 1G bytes

int debug = 0;


void accumulate_counts(const path_set_type& paths,
		       const path_type& path_filter,
		       const vocabulary_type& vocabulary,
		       const path_type& output_path,
		       path_map_type&   paths_counts,
		       const bool map_line,
		       const double max_malloc,
		       const int num_threads);
void accumulate_corpus(const path_set_type& paths,
		       const path_type& path_filter,
		       const vocabulary_type& vocabulary,
		       const path_type& output_path,
		       path_map_type&   paths_counts,
		       const bool map_line,
		       const double max_malloc,
		       const int num_threads);
int getoptions(int argc, char** argv);

int main(int argc, char** argv)
{
  try {
    if (getoptions(argc, argv) != 0) 
      return 1;
    
    if (output_file.empty())
      throw std::runtime_error("no output file");
    
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

    vocabulary_type vocabulary;

    if (! vocab_file.empty()) {
      if (vocab_file != "-" && ! boost::filesystem::exists(vocab_file))
	throw std::runtime_error("no vocabulary file? " + vocab_file.string());
      
      utils::compress_istream is(vocab_file, 1024 * 1024);
      std::string word;
      while (is >> word)
	vocabulary.insert(word);
    }
    
    
    GoogleNGramCounts::preprocess(output_file, max_order);
        
    path_map_type paths_counts(max_order);
    
    if (! counts_files.empty()) {
      if (debug)
	std::cerr << "collect counts from counts" << std::endl;
      accumulate_counts(counts_files, filter_file, vocabulary, output_file, paths_counts, map_line, max_malloc, threads);
    }
    
    if (! corpus_files.empty()) {
      if (debug)
	std::cerr << "collect counts from corpus" << std::endl;
      accumulate_corpus(corpus_files, filter_file, vocabulary, output_file, paths_counts, map_line, max_malloc, threads);
    }
    
    GoogleNGramCounts::postprocess(output_file, paths_counts);
  }
  catch (std::exception& err) {
    std::cerr << "error: " << err.what() << std::endl;
    return 1;
  }
  return 0;
}

void accumulate_counts(const path_set_type& __paths,
		       const path_type& path_filter,
		       const vocabulary_type& vocabulary,
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
    if (*piter != "-" && ! boost::filesystem::exists(*piter))
      throw std::runtime_error(std::string("no file? ") + piter->string());
    
    if (boost::filesystem::is_directory(*piter))
      GoogleNGramCounts::expand(*piter, paths, max_order);
    else
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
    path_thread_type    paths_thread(num_threads, path_map_type(max_order));
    
    std::vector<boost::shared_ptr<subprocess_type> > subprocess(path_filter.empty() ? 0 : num_threads);
    for (size_t shard = 0; shard < subprocess.size(); ++ shard)
      subprocess[shard].reset(new subprocess_type(path_filter));
    
    for (int shard = 0; shard < num_threads; ++ shard) {
      if (subprocess.empty())
	threads[shard].reset(new thread_type(task_type(queue, vocabulary, output_path, paths_thread[shard], max_malloc)));
      else
	threads[shard].reset(new thread_type(task_type(queue, *subprocess[shard], vocabulary, output_path, paths_thread[shard], max_malloc)));
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
      
      path_map_type& paths_shard = paths_thread[shard];
      for (int order = 1; order <= max_order; ++ order)
	paths_counts[order - 1].insert(paths_counts[order - 1].end(), paths_shard[order - 1].begin(), paths_shard[order - 1].end());
      paths_shard.clear();
    }
    
  } else {
    typedef GoogleNGramCounts::TaskFile<GoogleNGramCounts::TaskCounts> task_type;
    
    typedef task_type::queue_type          queue_type;
    typedef task_type::thread_type         thread_type;
    typedef task_type::thread_ptr_set_type thread_ptr_set_type;
    typedef task_type::subprocess_type     subprocess_type;
    
    thread_ptr_set_type threads(num_threads);
    queue_type          queue(num_threads);
    path_thread_type    paths_thread(num_threads, path_map_type(max_order));
    
    std::vector<boost::shared_ptr<subprocess_type> > subprocess(path_filter.empty() ? 0 : num_threads);
    for (size_t shard = 0; shard < subprocess.size(); ++ shard)
      subprocess[shard].reset(new subprocess_type(path_filter));
    
    for (int shard = 0; shard < num_threads; ++ shard) {
      if (subprocess.empty())
	threads[shard].reset(new thread_type(task_type(queue, vocabulary, output_path, paths_thread[shard], max_malloc)));
      else
	threads[shard].reset(new thread_type(task_type(queue, *subprocess[shard], vocabulary, output_path, paths_thread[shard], max_malloc)));
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
      
      path_map_type& paths_shard = paths_thread[shard];
      for (int order = 1; order <= max_order; ++ order)
	paths_counts[order - 1].insert(paths_counts[order - 1].end(), paths_shard[order - 1].begin(), paths_shard[order - 1].end());
      paths_shard.clear();
    }
  }
}

void accumulate_corpus(const path_set_type& paths,
		       const path_type& path_filter,
		       const vocabulary_type& vocabulary,
		       const path_type& output_path,
		       path_map_type&   paths_counts,
		       const bool map_line,
		       const double max_malloc,
		       const int num_threads)
{
  typedef std::vector<path_map_type, std::allocator<path_map_type> > path_thread_type;

  const int max_order = paths_counts.size();
  
  if (map_line) {
    typedef GoogleNGramCounts::TaskLine<GoogleNGramCounts::TaskCorpus> task_type;
    
    typedef task_type::line_set_type       line_set_type;
    typedef task_type::queue_type          queue_type;
    typedef task_type::thread_type         thread_type;
    typedef task_type::thread_ptr_set_type thread_ptr_set_type;
    typedef task_type::subprocess_type     subprocess_type;
    
    thread_ptr_set_type threads(num_threads);
    queue_type          queue(num_threads * 8);
    path_thread_type    paths_thread(num_threads, path_map_type(max_order));

    std::vector<boost::shared_ptr<subprocess_type> > subprocess(path_filter.empty() ? 0 : num_threads);
    for (size_t shard = 0; shard < subprocess.size(); ++ shard)
      subprocess[shard].reset(new subprocess_type(path_filter));
    
    for (int shard = 0; shard < num_threads; ++ shard) {
      if (subprocess.empty())
	threads[shard].reset(new thread_type(task_type(queue, vocabulary, output_path, paths_thread[shard], max_malloc)));
      else
	threads[shard].reset(new thread_type(task_type(queue, *subprocess[shard], vocabulary, output_path, paths_thread[shard], max_malloc)));
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
      
      path_map_type& paths_shard = paths_thread[shard];
      for (int order = 1; order <= max_order; ++ order)
	paths_counts[order - 1].insert(paths_counts[order - 1].end(), paths_shard[order - 1].begin(), paths_shard[order - 1].end());
      paths_shard.clear();
    }
  } else {
    typedef GoogleNGramCounts::TaskFile<GoogleNGramCounts::TaskCorpus> task_type;
    
    typedef task_type::queue_type          queue_type;
    typedef task_type::thread_type         thread_type;
    typedef task_type::thread_ptr_set_type thread_ptr_set_type;
    typedef task_type::subprocess_type     subprocess_type;
    
    thread_ptr_set_type threads(num_threads);
    queue_type          queue(num_threads);
    path_thread_type    paths_thread(num_threads, path_map_type(max_order));
    
    std::vector<boost::shared_ptr<subprocess_type> > subprocess(path_filter.empty() ? 0 : num_threads);
    for (size_t shard = 0; shard < subprocess.size(); ++ shard)
      subprocess[shard].reset(new subprocess_type(path_filter));
    
    for (int shard = 0; shard < num_threads; ++ shard) {
      if (subprocess.empty())
	threads[shard].reset(new thread_type(task_type(queue, vocabulary, output_path, paths_thread[shard], max_malloc)));
      else
	threads[shard].reset(new thread_type(task_type(queue, *subprocess[shard], vocabulary, output_path, paths_thread[shard], max_malloc)));
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
    ("corpus",       po::value<path_set_type>(&corpus_files)->multitoken(),  "corpus file(s)")
    ("counts",       po::value<path_set_type>(&counts_files)->multitoken(),  "counts file(s)")
    
    ("corpus-list",  po::value<path_type>(&corpus_list_file),  "corpus list file")
    ("counts-list",  po::value<path_type>(&counts_list_file),  "counts list file")

    ("vocab",        po::value<path_type>(&vocab_file),        "vocabulary file (list of words)")
    
    ("output",       po::value<path_type>(&output_file), "output directory")
    
    ("filter", po::value<path_type>(&filter_file), "filtering script")
    
    ("order",      po::value<int>(&max_order)->default_value(max_order),     "ngram order")
    ("map-line",   po::bool_switch(&map_line),     "map by lines, not by files")
    ("threads",    po::value<int>(&threads),       "# of threads")
    ("max-malloc", po::value<double>(&max_malloc), "maximum malloc in GB")
    
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
