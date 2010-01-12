
#include <iostream>
#include <stdexcept>

#include <vector>

#include <expgram/NGramCounts.hpp>

#include <boost/thread.hpp>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <boost/shared_ptr.hpp>

#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filter/bzip2.hpp>

#include <utils/tempfile.hpp>
#include <utils/resource.hpp>

#include <utils/mpi.hpp>
#include <utils/mpi_device.hpp>
#include <utils/mpi_device_bcast.hpp>


typedef expgram::NGramCounts ngram_type;

typedef ngram_type::size_type       size_type;
typedef ngram_type::difference_type difference_type;
typedef ngram_type::path_type       path_type;


typedef ngram_type::count_type      count_type;
typedef ngram_type::vocab_type      vocab_type;
typedef ngram_type::word_type       word_type;
typedef ngram_type::id_type         id_type;

typedef utils::mpi_intercomm intercomm_type;

path_type ngram_file;
path_type output_file;

path_type prog_name;

int debug = 0;

enum {
  modify_tag = 2000,
  sync_tag,
};

void ngram_modify_mapper(const ngram_type& ngram, intercomm_type& reducer);
void ngram_modify_reducer(ngram_type& ngram, intercomm_type& mapper);

int getoptions(int argc, char** argv);

int main(int argc, char** argv)
{
  utils::mpi_world  mpi_world(argc, argv);
  
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();  
  
  try {
    if (MPI::Comm::Get_parent() != MPI::COMM_NULL) {
      
      utils::mpi_intercomm comm_parent(MPI::Comm::Get_parent());
      
      if (getoptions(argc, argv) != 0) 
	return 1;
      
      ngram_type ngram(debug);
      ngram.open_shard(ngram_file, mpi_rank);
      
      ngram_modify_reducer(ngram, comm_parent);
      
      if (mpi_rank == 0)
	ngram.write_prepare(output_file);
      
      MPI::COMM_WORLD.Barrier();
      ngram.write_shard(output_file, mpi_rank);
      
      // perform synchronization here...?
      
    } else {
      std::vector<const char*, std::allocator<const char*> > args;
      args.reserve(argc);
      for (int i = 1; i < argc; ++ i)
	args.push_back(argv[i]);
      args.push_back(0);
      
      if (getoptions(argc, argv) != 0) 
	return 1;

      if (ngram_file.empty() || ! boost::filesystem::exists(ngram_file))
	throw std::runtime_error("no ngram file?");
      if (output_file.empty())
	throw std::runtime_error("no output file?");
      if (ngram_file == output_file)
	throw std::runtime_error("dump to the same directory?");
      if (! prog_name.empty() && ! boost::filesystem::exists(prog_name))
	throw std::runtime_error(std::string("no binary? ") + prog_name.file_string());
      
      const std::string name = (boost::filesystem::exists(prog_name) ? prog_name.file_string() : std::string(argv[0]));
      utils::mpi_intercomm comm_child(MPI::COMM_WORLD.Spawn(name.c_str(), &(*args.begin()), mpi_size, MPI::INFO_NULL, 0));
      
      ngram_type ngram(debug);
      ngram.open_shard(ngram_file, mpi_rank);
      
      if (ngram.index.size() != mpi_size)
	throw std::runtime_error("MPI universe size do not match with ngram shard size");
      
      ngram_modify_mapper(ngram, comm_child);

      // do we synchronize here...?
    }
    
  }
  catch (std::exception& err) {
    std::cerr << "error: "  << err.what() << std::endl;
    return 1;
  }
  return 0;
}



void ngram_modify_mapper(const ngram_type& ngram, intercomm_type& reducer)
{
  typedef boost::iostreams::filtering_ostream ostream_type;
  typedef utils::mpi_device_sink              odevice_type;
  
  typedef boost::shared_ptr<ostream_type> ostream_ptr_type;
  typedef boost::shared_ptr<odevice_type> odevice_ptr_type;
  
  typedef std::vector<ostream_ptr_type, std::allocator<ostream_ptr_type> > ostream_ptr_set_type;
  typedef std::vector<odevice_ptr_type, std::allocator<odevice_ptr_type> > odevice_ptr_set_type;

  typedef std::vector<count_type, std::allocator<count_type> > count_set_type;

  typedef std::vector<id_type, std::allocator<id_type> > context_type;
  
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  ostream_ptr_set_type stream(mpi_size);
  odevice_ptr_set_type device(mpi_size);
  
  for (int rank = 0; rank < mpi_size; ++ rank) {
    stream[rank].reset(new ostream_type());
    device[rank].reset(new odevice_type(reducer.comm, rank, modify_tag, 1024 * 1024, false, true));
    
    stream[rank]->push(boost::iostreams::gzip_compressor());
    stream[rank]->push(*device[rank]);

    stream[rank]->precision(20);
  }
  
  const id_type bos_id = ngram.index.vocab()[vocab_type::BOS];
  const int max_order = ngram.index.order();
  
  count_set_type unigrams(ngram.index[mpi_rank].offsets[1], count_type(0));
  context_type   context;
  
  for (int order_prev = 1; order_prev < ngram.index.order(); ++ order_prev) {
    
    if (debug)
      std::cerr << "modify counts: shard: " << mpi_rank << " order: " << (order_prev + 1) << std::endl;
    
    const size_type pos_context_first = ngram.index[mpi_rank].offsets[order_prev - 1];
    const size_type pos_context_last  = ngram.index[mpi_rank].offsets[order_prev];
        
    context.resize(order_prev + 1);
    
    size_type pos_last_prev = pos_context_last;
    for (size_type pos_context = pos_context_first; pos_context < pos_context_last; ++ pos_context) {
      const size_type pos_first = pos_last_prev;
      const size_type pos_last = ngram.index[mpi_rank].children_last(pos_context);
      pos_last_prev = pos_last;
      
      if (pos_first == pos_last) continue;
      
      context_type::iterator citer_curr = context.end() - 2;
      for (size_type pos_curr = pos_context; pos_curr != size_type(-1); pos_curr = ngram.index[mpi_rank].parent(pos_curr), -- citer_curr)
	*citer_curr = ngram.index[mpi_rank][pos_curr];
      
      // BOS handling...
      if (mpi_rank == 0 && order_prev == 1 && context.front() == bos_id)
	unigrams[context.front()] += ngram.counts[mpi_rank][pos_context];
      
      for (size_type pos = pos_first; pos != pos_last; ++ pos) {
	context.back() = ngram.index[mpi_rank][pos];
	
	if (context.size() == 2)
	  ++ unigrams[context.back()];
	else {
	  const int shard = ngram.index.shard_index(context.begin() + 1, context.end());
	  std::copy(context.begin() + 1, context.end(), std::ostream_iterator<id_type>(*stream[shard], " "));
	  *stream[shard] << 1 << '\n';
	}
	
	if (context.front() == bos_id && order_prev + 1 != max_order) {
	  const int shard = ngram.index.shard_index(context.begin(), context.end());
	  std::copy(context.begin(), context.end(), std::ostream_iterator<id_type>(*stream[shard], " "));
	  *stream[shard] << ngram.counts[mpi_rank][pos] << '\n';
	}
      }
      
      if (utils::mpi_flush_devices(stream, device))
	boost::thread::yield();
    }
  }
  
  for (id_type id = 0; id < unigrams.size(); ++ id)
    if (unigrams[id] > 0)
      *stream[0] << id << ' ' << unigrams[id] << '\n';

  
  for (int rank = 0; rank < mpi_size; ++ rank) {
    *stream[rank] << '\n';
    stream[rank].reset();
  }
  
  // loop-until terminated...
  for (;;) {
    if (std::count(device.begin(), device.end(), odevice_ptr_type()) == mpi_size) break;
    
    if (! utils::mpi_terminate_devices(stream, device))
      boost::thread::yield();
  }
}

template <typename Path, typename Data>
inline
void dump_file(const Path& file, const Data& data)
{
  std::auto_ptr<boost::iostreams::filtering_ostream> os(new boost::iostreams::filtering_ostream());
  os->push(boost::iostreams::file_sink(file.native_file_string(), std::ios_base::out | std::ios_base::trunc), 1024 * 1024);
  
  const int64_t file_size = sizeof(typename Data::value_type) * data.size();
    for (int64_t offset = 0; offset < file_size; offset += 1024 * 1024)
      os->write(((char*) &(*data.begin())) + offset, std::min(int64_t(1024 * 1024), file_size - offset));
}

template <typename Iterator>
void dump(std::ostream& os, Iterator first, Iterator last)
{
  typedef typename std::iterator_traits<Iterator>::value_type value_type;
      
  while (first != last) {
    const size_type write_size = std::min(size_type(1024 * 1024), size_type(last - first));
    os.write((char*) &(*first), write_size * sizeof(value_type));
    first += write_size;
  }
}

void ngram_modify_reducer(ngram_type& ngram, intercomm_type& mapper)
{
  typedef boost::iostreams::filtering_istream istream_type;
  typedef utils::mpi_device_source            idevice_type;
  
  typedef boost::shared_ptr<istream_type> istream_ptr_type;
  typedef boost::shared_ptr<idevice_type> idevice_ptr_type;
  
  typedef std::vector<istream_ptr_type, std::allocator<istream_ptr_type> > istream_ptr_set_type;
  typedef std::vector<idevice_ptr_type, std::allocator<idevice_ptr_type> > idevice_ptr_set_type;

  typedef std::vector<count_type, std::allocator<count_type> > count_set_type;
  
  typedef std::vector<id_type, std::allocator<id_type> > context_type;
  
  typedef std::vector<std::string, std::allocator<std::string> > tokens_type;
  typedef boost::tokenizer<utils::space_separator>               tokenizer_type;

  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  istream_ptr_set_type stream(mpi_size);
  idevice_ptr_set_type device(mpi_size);
  
  for (int rank = 0; rank < mpi_size; ++ rank) {
    stream[rank].reset(new istream_type());
    device[rank].reset(new idevice_type(mapper.comm, rank, modify_tag, 1024 * 1024));
    
    stream[rank]->push(boost::iostreams::gzip_decompressor());
    stream[rank]->push(*device[rank]);
  }
  
  
  const size_type offset = ngram.counts[mpi_rank].offset;
  count_set_type  counts_modified(ngram.index[mpi_rank].position_size() - offset, count_type(0));
  
  std::string line;
  tokens_type tokens;
  context_type context;
  
  for (;;) {
    bool found = false;
    
    for (int rank = 0; rank < mpi_size; ++ rank) 
      while (stream[rank] && device[rank] && device[rank]->test()) {
	
	if (std::getline(*stream[rank], line)) {
	  tokenizer_type tokenizer(line);
	  tokens.clear();
	  tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
	  
	  if (tokens.size() < 2) continue;
	  
	  context.clear();
	  tokens_type::const_iterator titer_end = tokens.end() - 1;
	  for (tokens_type::const_iterator titer = tokens.begin(); titer != titer_end; ++ titer)
	    context.push_back(atol(titer->c_str()));
	  
	  std::pair<context_type::const_iterator, size_type> result = ngram.index.traverse(mpi_rank, context.begin(), context.end());
	  if (result.first != context.end() || result.second == size_type(-1))
	    throw std::runtime_error("no ngram?");
	  
	  counts_modified[result.second - offset] += atoll(tokens.back().c_str());
	  
	} else {
	  stream[rank].reset();
	  device[rank].reset();
	}
	
	found = true;
      }
    
    
    if (std::count(device.begin(), device.end(), idevice_ptr_type()) == mpi_size) break;
    
    if (! found)
      boost::thread::yield();
  }
  
  const path_type path = utils::tempfile::file_name(utils::tempfile::tmp_dir() / "expgram.modified.XXXXXX");
  utils::tempfile::insert(path);
  
  boost::iostreams::filtering_ostream os;
  os.push(utils::packed_sink<count_type, std::allocator<count_type> >(path));
  dump(os, counts_modified.begin(), counts_modified.end());
  
  // dump the last order...
  for (size_type pos = ngram.index[mpi_rank].position_size(); pos < ngram.counts[mpi_rank].size(); ++ pos) {
    const count_type count = ngram.counts[mpi_rank][pos];
    os.write((char*) &count, sizeof(count_type));
  }
  os.pop();
  
  utils::tempfile::permission(path);
  
  ngram.counts[mpi_rank].counts.close();
  ngram.counts[mpi_rank].modified.open(path);
}

int getoptions(int argc, char** argv)
{
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  namespace po = boost::program_options;
  
  po::options_description desc("options");
  desc.add_options()
    ("ngram",  po::value<path_type>(&ngram_file),  "ngram language model")
    ("output", po::value<path_type>(&output_file), "output")
    
    ("prog",   po::value<path_type>(&prog_name),   "this binary")
    
    
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
