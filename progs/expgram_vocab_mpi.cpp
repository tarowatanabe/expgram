//
//  Copyright(C) 2009-2012 Taro Watanabe <taro.watanabe@nict.go.jp>
//

#include <iostream>
#include <sstream>

#include <string>
#include <vector>
#include <stdexcept>
#include <iterator>
#include <algorithm>
#include <memory>

#include <expgram/Word.hpp>
#include <expgram/Vocab.hpp>
#include <expgram/Sentence.hpp>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/functional/hash.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filter/bzip2.hpp>

#include <utils/compress_stream.hpp>

#include "expgram_vocab_impl.hpp"

#include <utils/mpi.hpp>
#include <utils/mpi_stream.hpp>
#include <utils/mpi_device.hpp>
#include <utils/mpi_device_bcast.hpp>

typedef GoogleNGramCounts::path_type     path_type;
typedef GoogleNGramCounts::path_set_type path_set_type;

typedef GoogleNGramCounts::count_type    count_type;
typedef GoogleNGramCounts::word_type     word_type;
typedef GoogleNGramCounts::vocab_type    vocab_type;
typedef GoogleNGramCounts::ngram_type    ngram_type;

typedef GoogleNGramCounts::ngram_count_set_type count_set_type;

path_type corpus_file;
path_type counts_file;

path_type corpus_list_file;
path_type counts_list_file;

path_type output_file = "-";

path_type filter_file;

path_type prog_name;

bool map_line = false;

int debug = 0;

void accumulate_counts_root(const path_set_type& paths,
			    const path_type& path_filter,
			    count_set_type& counts,
			    const bool map_line);
void accumulate_counts_others(const path_type& path_filter,
			      count_set_type& counts,
			      const bool map_line);
void accumulate_corpus_root(const path_set_type& paths,
			    const path_type& path_filter,
			    count_set_type& counts,
			    const bool map_line);
void accumulate_corpus_others(const path_type& path_filter,
			      count_set_type& counts,
			      const bool map_line);

void reduce_counts_root(count_set_type& counts);
void reduce_counts_others(const count_set_type& counts);

int getoptions(int argc, char** argv);

struct greaterp
{
  template <typename Tp>
  bool operator()(const Tp& x, const Tp& y) const
  {
    return *x > *y;
  }
};

int main(int argc, char** argv)
{
  utils::mpi_world mpi_world(argc, argv);
  
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  try {
    if (getoptions(argc, argv) != 0) 
      return 1;
    
    if (mpi_rank == 0) {
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
      
      count_set_type counts;

      int counts_files_size = counts_files.size();
      MPI::COMM_WORLD.Bcast(&counts_files_size, 1, MPI::INT, 0);
      if (! counts_files.empty())
	accumulate_counts_root(counts_files, filter_file, counts, map_line);
      
      int corpus_files_size = corpus_files.size();
      MPI::COMM_WORLD.Bcast(&corpus_files_size, 1, MPI::INT, 0);
      if (! corpus_files.empty())
	accumulate_corpus_root(corpus_files, filter_file, counts, map_line);
      
      reduce_counts_root(counts);
      
      // sorted output...
      typedef std::vector<count_set_type::const_iterator, std::allocator<count_set_type::const_iterator> > sorted_type;
    
      sorted_type sorted;
      sorted.reserve(counts.size());
      count_set_type::const_iterator citer_begin = counts.begin();
      count_set_type::const_iterator citer_end = counts.end();
      for (count_set_type::const_iterator citer = citer_begin; citer != citer_end; ++ citer)
	sorted.push_back(citer);
    
      std::sort(sorted.begin(), sorted.end(), greaterp());
      
      utils::compress_ostream os(output_file, 1024 * 1024);
    
      sorted_type::const_iterator siter_end = sorted.end();
      for (sorted_type::const_iterator siter = sorted.begin(); siter != siter_end; ++ siter)
	if (*(*siter))
	  os << word_type(word_type::id_type((*siter) - citer_begin)) << '\t' << *(*siter) << '\n';
      
    } else {
      count_set_type counts;

      int counts_files_size = 0;
      MPI::COMM_WORLD.Bcast(&counts_files_size, 1, MPI::INT, 0);
      if (counts_files_size > 0)
	accumulate_counts_others(filter_file, counts, map_line);
      
      int corpus_files_size = 0;
      MPI::COMM_WORLD.Bcast(&corpus_files_size, 1, MPI::INT, 0);
      if (corpus_files_size > 0)
	accumulate_corpus_others(filter_file, counts, map_line);
      
      reduce_counts_others(counts);
    }
  }
  catch (std::exception& err) {
    std::cerr << "error: " << err.what() << std::endl;
    MPI::COMM_WORLD.Abort(1);
    return 1;
  }
  return 0;
}

enum {
  line_tag = 5000,
  file_tag,
  path_tag,
  count_tag,
};

void reduce_counts_root(count_set_type& counts)
{
  typedef std::vector<utils::piece, std::allocator<utils::piece> > tokens_type;
  typedef boost::tokenizer<utils::space_separator, utils::piece::const_iterator, utils::piece> tokenizer_type;
  
  typedef boost::iostreams::filtering_istream istream_type;
  typedef utils::mpi_device_source            idevice_type;
  
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  tokens_type tokens;
  
  for (int rank = 1; rank < mpi_size; ++ rank) {
    istream_type is;
    is.push(boost::iostreams::zlib_decompressor());
    is.push(idevice_type(rank, count_tag, 1024 * 1024));
    
    std::string line;
    while (std::getline(is, line)) {
      if (line.empty()) continue;
      
      utils::piece line_piece(line);
      tokenizer_type tokenizer(line_piece);
      
      tokens.clear();
      tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
      
      if (tokens.size() != 2) continue;
      
      const word_type word = tokens.front();
      
      if (word.id() >= counts.size())
	counts.resize(word.id() + 1, 0);
      
      counts[word.id()] += utils::lexical_cast<count_type>(tokens.back());
    }
  }
}

void reduce_counts_others(const count_set_type& counts)
{
  typedef boost::iostreams::filtering_ostream ostream_type;
  typedef utils::mpi_device_sink              odevice_type;
  
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  ostream_type os;
  os.push(boost::iostreams::zlib_compressor());
  os.push(odevice_type(0, count_tag, 1024 * 1024));
  
  for (word_type::id_type id = 0; id != counts.size(); ++ id)
    if (counts[id])
      os << word_type(id) << '\t' << counts[id] << '\n';
}

// line-based map-reduce...
template <typename Task>
struct MapReduceLine
{
  typedef boost::iostreams::filtering_ostream ostream_type;
  typedef boost::iostreams::filtering_istream istream_type;
  
  typedef utils::mpi_device_sink   odevice_type;
  typedef utils::mpi_device_source idevice_type;
  
  typedef boost::shared_ptr<ostream_type> ostream_ptr_type;
  typedef boost::shared_ptr<istream_type> istream_ptr_type;
  
  typedef boost::shared_ptr<odevice_type> odevice_ptr_type;
  typedef boost::shared_ptr<idevice_type> idevice_ptr_type;

  typedef Task task_type;
  
  typedef typename task_type::line_set_type       line_set_type;
  typedef typename task_type::queue_type          queue_type;
  typedef typename task_type::thread_type         thread_type;
  typedef typename task_type::thread_ptr_set_type thread_ptr_set_type;

  typedef typename task_type::subprocess_type     subprocess_type;

  typedef size_t size_type;
  
  static const size_type max_lines = 1024 * 8;
  
  static inline
  int loop_sleep(bool found, int non_found_iter)
  {
    if (! found) {
      boost::thread::yield();
      ++ non_found_iter;
    } else
      non_found_iter = 0;
    
    if (non_found_iter >= 50) {
      struct timespec tm;
      tm.tv_sec = 0;
      tm.tv_nsec = 2000001;
      nanosleep(&tm, NULL);
      
      non_found_iter = 0;
    }
    return non_found_iter;
  }

  static inline
  void mapper_root(const path_set_type& paths,
		   const path_type& path_filter,
		   count_set_type& counts,
		   const int debug)
  {
    const int mpi_rank = MPI::COMM_WORLD.Get_rank();
    const int mpi_size = MPI::COMM_WORLD.Get_size();
    
    std::auto_ptr<subprocess_type> subprocess(path_filter.empty() ? 0 : new subprocess_type(path_filter));
    
    std::vector<ostream_ptr_type, std::allocator<ostream_ptr_type> > stream(mpi_size);
    std::vector<odevice_ptr_type, std::allocator<odevice_ptr_type> > device(mpi_size);
    
    for (int rank = 1; rank < mpi_size; ++ rank) {
      stream[rank].reset(new ostream_type());
      device[rank].reset(new odevice_type(rank, line_tag, 1024 * 1024, false, true));
    
      stream[rank]->push(boost::iostreams::zlib_compressor());
      stream[rank]->push(*device[rank]);
    }
    
    queue_type queue(1);
    std::auto_ptr<thread_type> thread(subprocess.get()
				      ? new thread_type(task_type(queue, *subprocess, counts))
				      : new thread_type(task_type(queue, counts)));
  
    std::string line;
    line_set_type lines;

    int non_found_iter = 0;
    
    path_set_type::const_iterator piter_end = paths.end();
    for (path_set_type::const_iterator piter = paths.begin(); piter != piter_end; ++ piter) {
      if (*piter != "-" && ! boost::filesystem::exists(*piter))
	throw std::runtime_error(std::string("no file? ") + piter->string());
      
      if (debug)
	std::cerr << "file: " << piter->string() << std::endl;
    
      utils::compress_istream is(*piter, 1024 * 1024);
      
      while (is) {
	bool found = false;
	
	for (int rank = 1; rank < mpi_size && is; ++ rank)
	  if (device[rank]->test() && std::getline(is, line)) {
	    *stream[rank] << line << '\n';
	    found = true;
	  }
	
	if (is && lines.size() < max_lines && queue.empty() && std::getline(is, line)) {
	  lines.push_back(line);
	  found = true;
	}
	
	if (lines.size() >= max_lines && queue.push_swap(lines, true)) {
	  lines.clear();
	  found = true;
	}
	
	non_found_iter = loop_sleep(found, non_found_iter);
      }
    }
  
    for (int rank = 1; rank < mpi_size; ++ rank)
      if (stream[rank]) {
	*stream[rank] << '\n';
	stream[rank].reset();
      }
  
    while (! lines.empty()) {
      bool found = false;
    
      if (! lines.empty() && queue.push_swap(lines, true)) {
	lines.clear();
	found = true;
      }
      
      found |= utils::mpi_terminate_devices(stream, device);
      
      non_found_iter = loop_sleep(found, non_found_iter);
    }
  
    for (;;) {
      bool found = false;
      
      if (queue.push_swap(lines, true)) {
	lines.clear();
	found = true;
      }
    
      found |= utils::mpi_terminate_devices(stream, device);
      
      if (std::count(device.begin(), device.end(), odevice_ptr_type()) == mpi_size) break;
      
      non_found_iter = loop_sleep(found, non_found_iter);
    }
  
    thread->join();
  }
  
  static inline
  void mapper_others(const path_type& path_filter,
		     count_set_type& counts)
  {
    const int mpi_rank = MPI::COMM_WORLD.Get_rank();
    const int mpi_size = MPI::COMM_WORLD.Get_size();
    
    std::auto_ptr<subprocess_type> subprocess(path_filter.empty() ? 0 : new subprocess_type(path_filter));
    
    istream_type stream;
    stream.push(boost::iostreams::zlib_decompressor());
    stream.push(idevice_type(0, line_tag, 1024 * 1024));
    
    queue_type queue(1);
    std::auto_ptr<thread_type> thread(subprocess.get()
				      ? new thread_type(task_type(queue, *subprocess, counts))
				      : new thread_type(task_type(queue, counts)));
    
    std::string line;
    line_set_type lines;
    
    while (std::getline(stream, line)) {
      lines.push_back(line);
      if (lines.size() >= max_lines) {
	queue.push_swap(lines);
	lines.clear();
      }
    }
    
    if (! lines.empty())
      queue.push_swap(lines);
    
    lines.clear();
    queue.push_swap(lines);
    
    thread->join();
  }
};

// file-based map-reduce
template <typename Task>
struct MapReduceFile
{
  typedef size_t size_type;
  
  typedef utils::mpi_ostream ostream_type;
  typedef utils::mpi_istream istream_type;
  
  typedef boost::shared_ptr<ostream_type> ostream_ptr_type;
  typedef boost::shared_ptr<istream_type> istream_ptr_type;
  
  typedef Task task_type;
  
  typedef typename task_type::queue_type          queue_type;
  typedef typename task_type::thread_type         thread_type;
  typedef typename task_type::thread_ptr_set_type thread_ptr_set_type;
  
  typedef typename task_type::subprocess_type     subprocess_type;
  

  static inline
  int loop_sleep(bool found, int non_found_iter)
  {
    if (! found) {
      boost::thread::yield();
      ++ non_found_iter;
    } else
      non_found_iter = 0;
    
    if (non_found_iter >= 50) {
      struct timespec tm;
      tm.tv_sec = 0;
      tm.tv_nsec = 2000001;
      nanosleep(&tm, NULL);
      
      non_found_iter = 0;
    }
    return non_found_iter;
  }

  static inline
  void mapper_root(const path_set_type& paths,
		   const path_type& path_filter,
		   count_set_type& counts,
		   const int debug)
  {
    const int mpi_rank = MPI::COMM_WORLD.Get_rank();
    const int mpi_size = MPI::COMM_WORLD.Get_size();
    
    std::auto_ptr<subprocess_type> subprocess(path_filter.empty() ? 0 : new subprocess_type(path_filter));
    
    std::vector<ostream_ptr_type, std::allocator<ostream_ptr_type> > stream(mpi_size);
    
    for (int rank = 1; rank < mpi_size; ++ rank)
      stream[rank].reset(new ostream_type(rank, file_tag, 4096));

    std::vector<int, std::allocator<int> >                   num_file(mpi_size, 0);
    std::vector<MPI::Request, std::allocator<MPI::Request> > requests(mpi_size);
    for (int rank = 1; rank < mpi_size; ++ rank)
      requests[rank] = MPI::COMM_WORLD.Irecv(&num_file[rank], 1, MPI::INT, rank, count_tag);
    
    queue_type queue(1);
    std::auto_ptr<thread_type> thread(subprocess.get()
				      ? new thread_type(task_type(queue, *subprocess, counts))
				      : new thread_type(task_type(queue, counts)));
    
    int non_found_iter = 0;

    path_set_type::const_iterator piter_end = paths.end();
    path_set_type::const_iterator piter = paths.begin();
    while (piter != piter_end) {
      bool found = false;
      
      for (int rank = 1; rank < mpi_size && piter != piter_end; ++ rank) 
	if (stream[rank]->test()) {
	  if (*piter != "-" && ! boost::filesystem::exists(*piter))
	    throw std::runtime_error(std::string("no file? ") + piter->string());
	  
	  if (debug)
	    std::cerr << "file: " << piter->string() << std::endl;
	  
	  stream[rank]->write(piter->string());
	  ++ piter;
	  found = true;
	}
      
      if (piter != piter_end && queue.empty()) {
	if (*piter != "-" && ! boost::filesystem::exists(*piter))
	  throw std::runtime_error(std::string("no file? ") + piter->string());
	
	if (debug)
	  std::cerr << "file: " << piter->string() << std::endl;
	
	queue.push(*piter);
	++ num_file[0];
	++ piter;
	found = true;
      }
      
      non_found_iter = loop_sleep(found, non_found_iter);
    }
    
    // send termination flag...    
    
    bool terminated = false;
    while (1) {
      bool found = false;
      
      if (! terminated && queue.push(path_type(), true)) {
	terminated = true;
	found = true;
      }
      
      for (int rank = 1; rank < mpi_size; ++ rank) 
	if (stream[rank] && stream[rank]->test()) {
	  if (! stream[rank]->terminated())
	    stream[rank]->terminate();
	  else if (requests[rank].Test())
	    stream[rank].reset();
	  
	  found = true;
	}
      
      if (terminated && std::count(stream.begin(), stream.end(), ostream_ptr_type()) == mpi_size) {
	bool waiting = false;
	for (int rank = 1; rank < mpi_size; ++ rank) {
	  waiting |= (! requests[rank].Test());
	  utils::atomicop::memory_barrier();
	}
	if (! waiting)
	  break;
      }
      
      non_found_iter = loop_sleep(found, non_found_iter);
    }    
    
    thread->join();

    if (debug) {
      for (int rank = 0; rank < mpi_size; ++ rank)
	std::cerr << "rank: " << rank << " files: " << num_file[rank] << std::endl;
    }
  }
  
  static inline
  void mapper_others(const path_type& path_filter,
		     count_set_type& counts)
  {
    const int mpi_rank = MPI::COMM_WORLD.Get_rank();
    const int mpi_size = MPI::COMM_WORLD.Get_size();
    
    std::auto_ptr<subprocess_type> subprocess(path_filter.empty() ? 0 : new subprocess_type(path_filter));
    
    istream_type stream(0, file_tag, 4096);
    
    queue_type queue(1);
    std::auto_ptr<thread_type> thread(subprocess.get()
				      ? new thread_type(task_type(queue, *subprocess, counts))
				      : new thread_type(task_type(queue, counts)));
    
    int num_file = 0;
    std::string file;
    while (stream.read(file)) {
      if (debug >= 2)
	std::cerr << "rank: " << mpi_rank << " file: " << file << std::endl;
      
      queue.push(file);
      ++ num_file;
      queue.wait_empty();
    }
    
    file.clear();
    queue.push(file);
    
    thread->join();
    
    MPI::COMM_WORLD.Send(&num_file, 1, MPI::INT, 0, count_tag);
  }
};


void accumulate_corpus_root(const path_set_type& paths,
			    const path_type& path_filter,
			    count_set_type& counts,
			    const bool map_line)
{
  if (map_line) {
    typedef GoogleNGramCounts::TaskLine<GoogleNGramCounts::TaskCorpus> task_type;
    typedef MapReduceLine<task_type> map_reduce_type;
    
    map_reduce_type::mapper_root(paths, path_filter, counts, debug);
    
  } else {
    typedef GoogleNGramCounts::TaskFile<GoogleNGramCounts::TaskCorpus> task_type;
    typedef MapReduceFile<task_type> map_reduce_type;
    
    map_reduce_type::mapper_root(paths, path_filter, counts, debug);
  }
}

void accumulate_counts_root(const path_set_type& paths,
			    const path_type& path_filter,
			    count_set_type& counts,
			    const bool map_line)
{
  if (map_line) {
    typedef GoogleNGramCounts::TaskLine<GoogleNGramCounts::TaskCounts> task_type;
    typedef MapReduceLine<task_type> map_reduce_type;
    
    map_reduce_type::mapper_root(paths, path_filter, counts, debug);
    
  } else {
    typedef GoogleNGramCounts::TaskFile<GoogleNGramCounts::TaskCounts> task_type;
    typedef MapReduceFile<task_type> map_reduce_type;
    
    map_reduce_type::mapper_root(paths, path_filter, counts, debug);
  }
}

void accumulate_corpus_others(const path_type& path_filter,
			      count_set_type& counts,
			      const bool map_line)
{
  if (map_line) {
    typedef GoogleNGramCounts::TaskLine<GoogleNGramCounts::TaskCorpus> task_type;
    typedef MapReduceLine<task_type> map_reduce_type;
    
    map_reduce_type::mapper_others(path_filter, counts);
    
  } else {
    typedef GoogleNGramCounts::TaskFile<GoogleNGramCounts::TaskCorpus> task_type;
    typedef MapReduceFile<task_type> map_reduce_type;
    
    map_reduce_type::mapper_others(path_filter, counts);
  }
}

void accumulate_counts_others(const path_type& path_filter,
			      count_set_type& counts,
			      const bool map_line)
{
  if (map_line) {
    typedef GoogleNGramCounts::TaskLine<GoogleNGramCounts::TaskCounts> task_type;
    typedef MapReduceLine<task_type> map_reduce_type;
    
    map_reduce_type::mapper_others(path_filter, counts);
    
  } else {
    typedef GoogleNGramCounts::TaskFile<GoogleNGramCounts::TaskCounts> task_type;
    typedef MapReduceFile<task_type> map_reduce_type;
    
    map_reduce_type::mapper_others(path_filter, counts);
  }
}


int getoptions(int argc, char** argv)
{
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();

  namespace po = boost::program_options;
  
  po::options_description desc("options");
  desc.add_options()
    ("corpus",       po::value<path_type>(&corpus_file),  "corpus file")
    ("counts",       po::value<path_type>(&counts_file),  "counts file")
    
    ("corpus-list",  po::value<path_type>(&corpus_list_file),  "corpus list file")
    ("counts-list",  po::value<path_type>(&counts_list_file),  "counts list file")
    
    ("output",       po::value<path_type>(&output_file)->default_value(output_file), "output directory")
    
    ("filter", po::value<path_type>(&filter_file), "filtering script")

    ("prog",   po::value<path_type>(&prog_name),   "this binary")
    
    ("map-line",   po::bool_switch(&map_line),     "map by lines, not by files")
    
    ("debug", po::value<int>(&debug)->implicit_value(1), "debug level")
    ("help", "help message");
  
  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc, po::command_line_style::unix_style & (~po::command_line_style::allow_guessing)), vm);
  po::notify(vm);
  
  if (vm.count("help")) {

    if (mpi_rank == 0)
      std::cout << argv[0] << " [options]" << '\n' << desc << '\n';
    return 1;
  }
  
  return 0;
}
