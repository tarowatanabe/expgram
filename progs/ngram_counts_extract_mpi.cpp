
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
#include <boost/algorithm/string.hpp>
#include <boost/functional/hash.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filter/bzip2.hpp>

#include <utils/compress_stream.hpp>

#include "ngram_counts_extract_impl.hpp"

#include <utils/mpi.hpp>
#include <utils/mpi_stream.hpp>
#include <utils/mpi_device.hpp>
#include <utils/mpi_device_bcast.hpp>

typedef GoogleNGramCounts::path_type     path_type;
typedef GoogleNGramCounts::path_set_type path_set_type;
typedef GoogleNGramCounts::path_map_type path_map_type;

typedef GoogleNGramCounts::count_type    count_type;
typedef GoogleNGramCounts::word_type     word_type;
typedef GoogleNGramCounts::vocab_type    vocab_type;
typedef GoogleNGramCounts::ngram_type    ngram_type;

path_type corpus_file;
path_type counts_file;

path_type corpus_list_file;
path_type counts_list_file;

path_type output_file;

path_type filter_file;

path_type prog_name;

int max_order = 5;

bool map_line = false;
double max_malloc = 1.0; // 1G bytes

int debug = 0;

void accumulate_counts_root(const path_set_type& paths,
			    const path_type& path_filter,
			    const path_type& output_path,
			    path_map_type&   paths_counts,
			    const bool map_line,
			    const double max_malloc);
void accumulate_counts_others(const path_type& path_filter,
			      const path_type& output_path,
			      path_map_type&   paths_counts,
			      const bool map_line,
			      const double max_malloc);
void accumulate_corpus_root(const path_set_type& paths,
			    const path_type& path_filter,
			    const path_type& output_path,
			    path_map_type&   paths_counts,
			    const bool map_line,
			    const double max_malloc);
void accumulate_corpus_others(const path_type& path_filter,
			      const path_type& output_path,
			      path_map_type&   paths_counts,
			      const bool map_line,
			      const double max_malloc);

void reduce_counts_root(path_map_type& paths_counts);
void reduce_counts_others(const path_map_type& paths_counts);

int getoptions(int argc, char** argv);

int main(int argc, char** argv)
{
  utils::mpi_world mpi_world(argc, argv);
  
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  try {
    if (getoptions(argc, argv) != 0) 
      return 1;
    
    if (output_file.empty())
      throw std::runtime_error("no output file");

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
      
      GoogleNGramCounts::preprocess(output_file, max_order);
      
      path_map_type paths_counts(max_order);

      int counts_files_size = counts_files.size();
      MPI::COMM_WORLD.Bcast(&counts_files_size, 1, MPI::INT, 0);
      if (! counts_files.empty())
	accumulate_counts_root(counts_files, filter_file, output_file, paths_counts, map_line, max_malloc);
      
      int corpus_files_size = corpus_files.size();
      MPI::COMM_WORLD.Bcast(&corpus_files_size, 1, MPI::INT, 0);
      if (! corpus_files.empty())
	accumulate_corpus_root(corpus_files, filter_file, output_file, paths_counts, map_line, max_malloc);
      
      reduce_counts_root(paths_counts);
      
      GoogleNGramCounts::postprocess(output_file, paths_counts);
      
    } else {
      path_map_type paths_counts(max_order);

      int counts_files_size = 0;
      MPI::COMM_WORLD.Bcast(&counts_files_size, 1, MPI::INT, 0);
      if (counts_files_size > 0)
	accumulate_counts_others(filter_file, output_file, paths_counts, map_line, max_malloc);
      
      int corpus_files_size = 0;
      MPI::COMM_WORLD.Bcast(&corpus_files_size, 1, MPI::INT, 0);
      if (corpus_files_size > 0)
	accumulate_corpus_others(filter_file, output_file, paths_counts, map_line, max_malloc);
      
      reduce_counts_others(paths_counts);
    }
  }
  catch (std::exception& err) {
    std::cerr << "error: " << err.what() << std::endl;
    return 1;
  }
  return 0;
}

enum {
  line_tag = 5000,
  file_tag,
  path_tag,
};

void reduce_counts_root(path_map_type& paths_counts)
{
  typedef boost::iostreams::filtering_istream istream_type;
  typedef utils::mpi_device_source            idevice_type;

  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  const int max_order = paths_counts.size();
  
  for (int order = 1; order <= max_order; ++ order)
    for (int rank = 1; rank < mpi_size; ++ rank) {
      istream_type is;
      is.push(boost::iostreams::gzip_decompressor());
      is.push(idevice_type(rank, path_tag, 4096));
      
      std::string line;
      while (std::getline(is, line)) {
	if (line.empty()) continue;

	if (! boost::filesystem::exists(line))
	  throw std::runtime_error(std::string("no counts file? ") + line);
	
	paths_counts[order - 1].push_back(line);
	utils::tempfile::insert(line);
      }
    }
}

void reduce_counts_others(const path_map_type& paths_counts)
{
  typedef boost::iostreams::filtering_ostream ostream_type;
  typedef utils::mpi_device_sink              odevice_type;
  
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  const int max_order = paths_counts.size();
  
  for (int order = 1; order <= max_order; ++ order) {

    {
      ostream_type os;
      os.push(boost::iostreams::gzip_compressor());
      os.push(odevice_type(0, path_tag, 4096));
      
      path_set_type::const_iterator piter_end = paths_counts[order - 1].end();
      for (path_set_type::const_iterator piter = paths_counts[order - 1].begin(); piter != piter_end; ++ piter) {
	if (! boost::filesystem::exists(*piter))
	  throw std::runtime_error(std::string("no count file? ") + piter->file_string());
	
	if (debug >= 2)
	  std::cerr << "order: " << order << " rank: " << mpi_rank << " file: " << piter->file_string() << std::endl;
	
	os << piter->file_string() << '\n';
      }
      
      os << '\n';
    }
    
    path_set_type::const_iterator piter_end = paths_counts[order - 1].end();
    for (path_set_type::const_iterator piter = paths_counts[order - 1].begin(); piter != piter_end; ++ piter)
      utils::tempfile::erase(*piter);
  }
  
  
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
  void mapper_root(const path_set_type& paths,
		   const path_type& path_filter,
		   const path_type& output_path,
		   path_map_type&   paths_counts,
		   const double max_malloc,
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
    
      stream[rank]->push(boost::iostreams::gzip_compressor());
      stream[rank]->push(*device[rank]);
    }
    
    queue_type queue(1);
    std::auto_ptr<thread_type> thread(subprocess.get()
				      ? new thread_type(task_type(queue, *subprocess, output_path, paths_counts, max_malloc))
				      : new thread_type(task_type(queue, output_path, paths_counts, max_malloc)));
  
    std::string line;
    line_set_type lines;
    
    path_set_type::const_iterator piter_end = paths.end();
    for (path_set_type::const_iterator piter = paths.begin(); piter != piter_end; ++ piter) {
      if (! boost::filesystem::exists(*piter))
	throw std::runtime_error(std::string("no file? ") + piter->file_string());

      if (debug)
	std::cerr << "file: " << piter->file_string() << std::endl;
    
      utils::compress_istream is(*piter, 1024 * 1024);
    
      int non_found_iter = 0;

      while (is) {
	bool found = false;
      
	for (int rank = 1; rank < mpi_size && is; ++ rank)
	  if (device[rank]->test() && std::getline(is, line)) {
	    *stream[rank] << line << '\n';
	    found = true;
	  }
      
	if (is && lines.size() < max_lines && std::getline(is, line)) {
	  lines.push_back(line);
	  found = true;
	}
	
	if (lines.size() >= max_lines && queue.push_swap(lines, true)) {
	  lines.clear();
	  found = true;
	}
      
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
    
      if (utils::mpi_terminate_devices(stream, device))
	found = true;
    
      if (! found)
	boost::thread::yield();
    }
  
    for (;;) {
      bool found = false;
    
      if (queue.push_swap(lines, true)) {
	lines.clear();
	found = true;
      }
    
      if (utils::mpi_terminate_devices(stream, device))
	found = true;

      if (std::count(device.begin(), device.end(), odevice_ptr_type()) == mpi_size) break;
      
      if (! found)
	boost::thread::yield();
    }
  
    thread->join();
  }
  
  static inline
  void mapper_others(const path_type& path_filter,
		     const path_type& output_path,
		     path_map_type&   paths_counts,
		     const double     max_malloc)
  {
    const int mpi_rank = MPI::COMM_WORLD.Get_rank();
    const int mpi_size = MPI::COMM_WORLD.Get_size();
    
    std::auto_ptr<subprocess_type> subprocess(path_filter.empty() ? 0 : new subprocess_type(path_filter));
    
    istream_type stream;
    stream.push(boost::iostreams::gzip_decompressor());
    stream.push(idevice_type(0, line_tag, 1024 * 1024));
    
    queue_type queue(1);
    std::auto_ptr<thread_type> thread(subprocess.get()
				      ? new thread_type(task_type(queue, *subprocess, output_path, paths_counts, max_malloc))
				      : new thread_type(task_type(queue, output_path, paths_counts, max_malloc)));
    
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
  void mapper_root(const path_set_type& paths,
		   const path_type& path_filter,
		   const path_type& output_path,
		   path_map_type&   paths_counts,
		   const double max_malloc,
		   const int debug)
  {
    const int mpi_rank = MPI::COMM_WORLD.Get_rank();
    const int mpi_size = MPI::COMM_WORLD.Get_size();
    
    std::auto_ptr<subprocess_type> subprocess(path_filter.empty() ? 0 : new subprocess_type(path_filter));
    
    std::vector<ostream_ptr_type, std::allocator<ostream_ptr_type> > stream(mpi_size);
    
    for (int rank = 1; rank < mpi_size; ++ rank)
      stream[rank].reset(new ostream_type(rank, file_tag, 4096));
    
    queue_type queue(1);
    std::auto_ptr<thread_type> thread(subprocess.get()
				      ? new thread_type(task_type(queue, *subprocess, output_path, paths_counts, max_malloc))
				      : new thread_type(task_type(queue, output_path, paths_counts, max_malloc)));
    
    int non_found_iter = 0;

    path_set_type::const_iterator piter_end = paths.end();
    path_set_type::const_iterator piter = paths.begin();
    while (piter != piter_end) {
      bool found = false;
      
      for (int rank = 1; rank < mpi_size && piter != piter_end; ++ rank) 
	if (stream[rank]->test()) {
	  if (! boost::filesystem::exists(*piter))
	    throw std::runtime_error(std::string("no file? ") + piter->file_string());
	  
	  if (debug)
	    std::cerr << "file: " << piter->file_string() << std::endl;
	  
	  stream[rank]->write(piter->file_string());
	  ++ piter;
	  found = true;
	}
      
      if (piter != piter_end) {
	if (! boost::filesystem::exists(*piter))
	  throw std::runtime_error(std::string("no file? ") + piter->file_string());
	
	if (queue.push(*piter, true)) {
	  
	  if (debug)
	    std::cerr << "file: " << piter->file_string() << std::endl;
	  
	  ++ piter;
	  found = true;
	}
      }
      
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
      
    }
    
    // send termination flag...    
    
    bool terminated = false;
    while (1) {
      bool found = false;
      
      if (queue.push(path_type(), true)) {
	terminated = true;
	found = true;
      }
      
      for (int rank = 1; rank < mpi_size; ++ rank) 
	if (stream[rank]) {
	  if (! stream[rank]->terminated())
	    stream[rank]->terminate();
	  else
	    stream[rank].reset();
	  found = true;
	}
      
      if (terminated && std::count(stream.begin(), stream.end(), ostream_ptr_type()) == mpi_size) break;
      
      if (! found)
	boost::thread::yield();
    }
    
    thread->join();
  }
  
  static inline
  void mapper_others(const path_type& path_filter,
		     const path_type& output_path,
		     path_map_type&   paths_counts,
		     const double     max_malloc)
  {
    const int mpi_rank = MPI::COMM_WORLD.Get_rank();
    const int mpi_size = MPI::COMM_WORLD.Get_size();
    
    std::auto_ptr<subprocess_type> subprocess(path_filter.empty() ? 0 : new subprocess_type(path_filter));
    
    istream_type stream(0, file_tag, 4096);
    
    queue_type queue(1);
    std::auto_ptr<thread_type> thread(subprocess.get()
				      ? new thread_type(task_type(queue, *subprocess, output_path, paths_counts, max_malloc))
				      : new thread_type(task_type(queue, output_path, paths_counts, max_malloc)));
    
    std::string file;
    while (stream.read(file)) {
      
      if (debug >= 2)
	std::cerr << "rank: " << mpi_rank << " file: " << file << std::endl;

      queue.push(file);
    }
    file.clear();
    queue.push(file);
    
    thread->join();
    
  }
};


void accumulate_corpus_root(const path_set_type& paths,
			    const path_type& path_filter,
			    const path_type& output_path,
			    path_map_type&   paths_counts,
			    const bool map_line,
			    const double max_malloc)
{
  if (map_line) {
    typedef GoogleNGramCounts::TaskLine<GoogleNGramCounts::TaskCorpus> task_type;
    typedef MapReduceLine<task_type> map_reduce_type;
    
    map_reduce_type::mapper_root(paths, path_filter, output_path, paths_counts, max_malloc, debug);
    
  } else {
    typedef GoogleNGramCounts::TaskFile<GoogleNGramCounts::TaskCorpus> task_type;
    typedef MapReduceFile<task_type> map_reduce_type;
    
    map_reduce_type::mapper_root(paths, path_filter, output_path, paths_counts, max_malloc, debug);
  }
}

void accumulate_counts_root(const path_set_type& paths,
			    const path_type& path_filter,
			    const path_type& output_path,
			    path_map_type&   paths_counts,
			    const bool map_line,
			    const double max_malloc)
{
  if (map_line) {
    typedef GoogleNGramCounts::TaskLine<GoogleNGramCounts::TaskCounts> task_type;
    typedef MapReduceLine<task_type> map_reduce_type;
    
    map_reduce_type::mapper_root(paths, path_filter, output_path, paths_counts, max_malloc, debug);
    
  } else {
    typedef GoogleNGramCounts::TaskFile<GoogleNGramCounts::TaskCounts> task_type;
    typedef MapReduceFile<task_type> map_reduce_type;
    
    map_reduce_type::mapper_root(paths, path_filter, output_path, paths_counts, max_malloc, debug);
  }
}

void accumulate_corpus_others(const path_type& path_filter,
			      const path_type& output_path,
			      path_map_type&   paths_counts,
			      const bool map_line,
			      const double max_malloc)
{
  if (map_line) {
    typedef GoogleNGramCounts::TaskLine<GoogleNGramCounts::TaskCorpus> task_type;
    typedef MapReduceLine<task_type> map_reduce_type;
    
    map_reduce_type::mapper_others(path_filter, output_path, paths_counts, max_malloc);
    
  } else {
    typedef GoogleNGramCounts::TaskFile<GoogleNGramCounts::TaskCorpus> task_type;
    typedef MapReduceFile<task_type> map_reduce_type;
    
    map_reduce_type::mapper_others(path_filter, output_path, paths_counts, max_malloc);
  }
}

void accumulate_counts_others(const path_type& path_filter,
			      const path_type& output_path,
			      path_map_type&   paths_counts,
			      const bool map_line,
			      const double max_malloc)
{
  if (map_line) {
    typedef GoogleNGramCounts::TaskLine<GoogleNGramCounts::TaskCounts> task_type;
    typedef MapReduceLine<task_type> map_reduce_type;
    
    map_reduce_type::mapper_others(path_filter, output_path, paths_counts, max_malloc);
    
  } else {
    typedef GoogleNGramCounts::TaskFile<GoogleNGramCounts::TaskCounts> task_type;
    typedef MapReduceFile<task_type> map_reduce_type;
    
    map_reduce_type::mapper_others(path_filter, output_path, paths_counts, max_malloc);
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
    
    ("output",       po::value<path_type>(&output_file), "output directory")
    
    ("filter", po::value<path_type>(&filter_file), "filtering script")

    ("prog",   po::value<path_type>(&prog_name),   "this binary")
    
    ("order",      po::value<int>(&max_order),     "ngram order")
    ("map-line",   po::bool_switch(&map_line),     "map by lines, not by files")
    ("max-malloc", po::value<double>(&max_malloc), "maximum malloc in GB")
    
    ("debug", po::value<int>(&debug)->implicit_value(1), "debug level")
    ("help", "help message");
  
  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);
  
  if (vm.count("help")) {

    if (mpi_rank == 0)
      std::cout << argv[0] << " [options]" << '\n' << desc << '\n';
    return 1;
  }
  
  return 0;
}
