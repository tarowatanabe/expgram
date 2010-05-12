
#include <iostream>
#include <stdexcept>

#include <vector>

#include <expgram/NGram.hpp>

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


typedef expgram::NGram ngram_type;

typedef ngram_type::size_type       size_type;
typedef ngram_type::difference_type difference_type;
typedef ngram_type::path_type       path_type;

typedef ngram_type::logprob_type    logprob_type;
typedef ngram_type::quantized_type  quantized_type;
typedef ngram_type::word_type       word_type;
typedef ngram_type::id_type         id_type;

typedef utils::mpi_intercomm intercomm_type;

path_type ngram_file;
path_type output_file;

path_type prog_name;

int debug = 0;

enum {
  bound_tag = 1000,
  sync_tag,
};

void ngram_bound_mapper(const ngram_type& ngram, intercomm_type& reducer);
void ngram_bound_reducer(ngram_type& ngram, intercomm_type& mapper);

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
      
      // set up logbounds...
      ngram.logbounds.reserve(ngram.index.size());
      ngram.logbounds.resize(ngram.index.size());
      
      ngram_bound_reducer(ngram, comm_parent);
      
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
      
      ngram_bound_mapper(ngram, comm_child);

      // do we synchronize here...?
    }
    
  }
  catch (std::exception& err) {
    std::cerr << "error: "  << err.what() << std::endl;
    return 1;
  }
  return 0;
}



void ngram_bound_mapper(const ngram_type& ngram, intercomm_type& reducer)
{
  typedef boost::iostreams::filtering_ostream ostream_type;
  typedef utils::mpi_device_sink              odevice_type;
  
  typedef boost::shared_ptr<ostream_type> ostream_ptr_type;
  typedef boost::shared_ptr<odevice_type> odevice_ptr_type;
  
  typedef std::vector<ostream_ptr_type, std::allocator<ostream_ptr_type> > ostream_ptr_set_type;
  typedef std::vector<odevice_ptr_type, std::allocator<odevice_ptr_type> > odevice_ptr_set_type;

  typedef std::vector<logprob_type, std::allocator<logprob_type> > logprob_set_type;

  typedef std::vector<id_type, std::allocator<id_type> > context_type;
  
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  ostream_ptr_set_type stream(mpi_size);
  odevice_ptr_set_type device(mpi_size);
  
  for (int rank = 0; rank < mpi_size; ++ rank) {
    stream[rank].reset(new ostream_type());
    device[rank].reset(new odevice_type(reducer.comm, rank, bound_tag, 1024 * 1024, false, true));
    
    stream[rank]->push(boost::iostreams::gzip_compressor());
    stream[rank]->push(*device[rank]);

    stream[rank]->precision(20);
  }
  
  logprob_set_type unigrams(ngram.index[mpi_rank].offsets[1], ngram.logprob_min());
  context_type     context;
  
  for (int order_prev = 1; order_prev < ngram.index.order(); ++ order_prev) {
    const size_type pos_context_first = ngram.index[mpi_rank].offsets[order_prev - 1];
    const size_type pos_context_last  = ngram.index[mpi_rank].offsets[order_prev];
    
    if (debug)
      std::cerr << "rank: " << mpi_rank << " order: " << (order_prev + 1) << std::endl;
    
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
      
      for (size_type pos = pos_first; pos != pos_last; ++ pos) {
	context.back() = ngram.index[mpi_rank][pos];
	
	const logprob_type logprob = ngram.logprobs[mpi_rank](pos, order_prev + 1);
	if (logprob != ngram.logprob_min()) {
#if 1
	  context_type::const_iterator citer_end = context.end();
	  context_type::const_iterator citer_begin = context.begin() + 1;
	  if (citer_end - citer_begin == 1)
	    unigrams[*citer_begin] = std::max(unigrams[*citer_begin], logprob);
	  else {
	    const int shard = ngram.index.shard_index(citer_begin, citer_end);
	    
	    std::copy(citer_begin, citer_end, std::ostream_iterator<id_type>(*stream[shard], " "));
	    *stream[shard] << logprob << '\n';
	  }
#endif	  
#if 0
	  context_type::const_iterator citer_end = context.end();
	  for (context_type::const_iterator citer = context.begin() + 1; citer != citer_end; ++ citer) {
	    if (citer_end - citer == 1)
	      unigrams[*citer] = std::max(unigrams[*citer], logprob);
	    else {
	      const int shard = ngram.index.shard_index(citer, citer_end);
	      
	      std::copy(citer, citer_end, std::ostream_iterator<id_type>(*stream[shard], " "));
	      *stream[shard] << logprob << '\n';
	    }
	  }
#endif
	}
      }
      
      if (utils::mpi_flush_devices(stream, device))
	boost::thread::yield();
    }
  }
  
  for (id_type id = 0; id < unigrams.size(); ++ id)
    if (unigrams[id] > ngram.logprob_min())
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

void ngram_bound_reducer(ngram_type& ngram, intercomm_type& mapper)
{
  typedef boost::iostreams::filtering_istream istream_type;
  typedef utils::mpi_device_source            idevice_type;
  
  typedef boost::shared_ptr<istream_type> istream_ptr_type;
  typedef boost::shared_ptr<idevice_type> idevice_ptr_type;
  
  typedef std::vector<istream_ptr_type, std::allocator<istream_ptr_type> > istream_ptr_set_type;
  typedef std::vector<idevice_ptr_type, std::allocator<idevice_ptr_type> > idevice_ptr_set_type;

  typedef std::vector<logprob_type, std::allocator<logprob_type> > logprob_set_type;
  
  typedef std::vector<id_type, std::allocator<id_type> > context_type;
  
  typedef std::vector<std::string, std::allocator<std::string> > tokens_type;
  typedef boost::tokenizer<utils::space_separator>               tokenizer_type;

  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  istream_ptr_set_type stream(mpi_size);
  idevice_ptr_set_type device(mpi_size);
  
  for (int rank = 0; rank < mpi_size; ++ rank) {
    stream[rank].reset(new istream_type());
    device[rank].reset(new idevice_type(mapper.comm, rank, bound_tag, 1024 * 1024));
    
    stream[rank]->push(boost::iostreams::gzip_decompressor());
    stream[rank]->push(*device[rank]);
  }
  
  // set up offset...
  ngram.logbounds[mpi_rank].offset = ngram.logprobs[mpi_rank].offset;
  
  const size_type offset = ngram.logbounds[mpi_rank].offset;
  logprob_set_type logbounds(ngram.logprobs[mpi_rank].logprobs.begin(),
			     ngram.logprobs[mpi_rank].logprobs.begin() + ngram.index[mpi_rank].position_size() - offset);

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
	  
	  logprob_type& bound = logbounds[result.second - offset];
	  bound = std::max(bound, logprob_type(atof(tokens.back().c_str())));
	  
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
  
  const path_type path = utils::tempfile::file_name(utils::tempfile::tmp_dir() / "expgram.logbound.XXXXXX");
  utils::tempfile::insert(path);
  dump_file(path, logbounds);
  utils::tempfile::permission(path);
  ngram.logbounds[mpi_rank].logprobs.open(path);
}

int getoptions(int argc, char** argv)
{
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  namespace po = boost::program_options;
  
  po::options_description desc("options");
  desc.add_options()
    ("ngram",  po::value<path_type>(&ngram_file)->default_value(ngram_file),   "ngram language model in expgram format")
    ("output", po::value<path_type>(&output_file)->default_value(output_file), "output in expgram format with upper bound estimation")
    
    ("prog",   po::value<path_type>(&prog_name),   "this binary")
    
    
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
