//
//  Copyright(C) 2009-2012 Taro Watanabe <taro.watanabe@nict.go.jp>
//

#include <iostream>
#include <stdexcept>

#include <vector>

#include <expgram/NGramCounts.hpp>

#include <boost/thread.hpp>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/range.hpp>

#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filter/bzip2.hpp>

#include <utils/tempfile.hpp>
#include <utils/resource.hpp>
#include <utils/lexical_cast.hpp>
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
path_type temporary_dir = "";

path_type prog_name;
std::string host;
std::string hostfile;

int debug = 0;

void ngram_modify_mapper(const ngram_type& ngram, intercomm_type& reducer);
void ngram_modify_reducer(ngram_type& ngram, intercomm_type& mapper);

void synchronize_mapper(intercomm_type& reducer);
void synchronize_reducer(intercomm_type& mapper);

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

      if (! temporary_dir.empty())
	::setenv("TMPDIR_SPEC", temporary_dir.string().data(), 1);
      
      ngram_type ngram(debug);
      ngram.open_shard(ngram_file, mpi_rank);
      
      // prepare types structure...
      ngram.types.reserve(mpi_size);
      ngram.types.resize(mpi_size);
      
      utils::resource start;
      
      ngram_modify_reducer(ngram, comm_parent);
      
      utils::resource end;
      
      if (debug && mpi_rank == 0)
	std::cerr << "modify counts reducer"
		  << " cpu time:  " << end.cpu_time() - start.cpu_time() 
		  << " user time: " << end.user_time() - start.user_time()
		  << std::endl;
      
      if (mpi_rank == 0)
	ngram.write_prepare(output_file);
      
      MPI::COMM_WORLD.Barrier();
      ngram.write_shard(output_file, mpi_rank);
      
      synchronize_reducer(comm_parent);
      
    } else {
      std::vector<const char*, std::allocator<const char*> > args;
      args.reserve(argc);
      for (int i = 1; i < argc; ++ i)
	args.push_back(argv[i]);
      args.push_back(0);
      
      if (getoptions(argc, argv) != 0) 
	return 1;

      if (! temporary_dir.empty())
	::setenv("TMPDIR_SPEC", temporary_dir.string().data(), 1);

      if (ngram_file.empty() || ! boost::filesystem::exists(ngram_file))
	throw std::runtime_error("no ngram file?");
      if (output_file.empty())
	throw std::runtime_error("no output file?");
      if (ngram_file == output_file)
	throw std::runtime_error("dump to the same directory?");
      if (! prog_name.empty() && ! boost::filesystem::exists(prog_name))
	throw std::runtime_error(std::string("no binary? ") + prog_name.string());
      
      std::vector<int, std::allocator<int> > error_codes(mpi_size, MPI_SUCCESS);
      
      const std::string name = (boost::filesystem::exists(prog_name) ? prog_name.string() : std::string(argv[0]));
      
      MPI::Info info = MPI::Info::Create();

      if (! host.empty())
	info.Set("host", host.c_str());
      if (! hostfile.empty())
	info.Set("hostfile", hostfile.c_str());
      
      utils::mpi_intercomm comm_child(MPI::COMM_WORLD.Spawn(name.c_str(), &(*args.begin()), mpi_size, info, 0, &(*error_codes.begin())));
      
      info.Free();
      
      for (size_t i = 0; i != error_codes.size(); ++ i)
	if (error_codes[i] != MPI_SUCCESS)
	  throw std::runtime_error("one of children failed to launch!");
      
      ngram_type ngram(debug);
      ngram.open_shard(ngram_file, mpi_rank);
      
      if (static_cast<int>(ngram.index.size()) != mpi_size)
	throw std::runtime_error("MPI universe size do not match with ngram shard size");

      utils::resource start;
      
      ngram_modify_mapper(ngram, comm_child);
      
      utils::resource end;
      
      if (debug && mpi_rank == 0)
	std::cerr << "modify counts mapper"
		  << " cpu time:  " << end.cpu_time() - start.cpu_time() 
		  << " user time: " << end.user_time() - start.user_time()
		  << std::endl;

      synchronize_mapper(comm_child);
    }
    
  }
  catch (std::exception& err) {
    std::cerr << "error: "  << err.what() << std::endl;
    MPI::COMM_WORLD.Abort(1);
    return 1;
  }
  return 0;
}

enum {
  modify_tag = 2000,
  notify_tag,
};

inline
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

void synchronize_mapper(intercomm_type& reducer)
{
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();

  std::vector<MPI::Request, std::allocator<MPI::Request> > request(mpi_size);
  std::vector<bool, std::allocator<bool> > terminated(mpi_size, false);
  
  for (int rank = 0; rank != mpi_size; ++ rank)
    request[rank] = reducer.comm.Irecv(0, 0, MPI::INT, rank, notify_tag);
  
  int non_found_iter = 0;
  for (;;) {
    bool found = false;
    
    for (int rank = 0; rank != mpi_size; ++ rank)
      if (! terminated[rank] && request[rank].Test()) {
	terminated[rank] = true;
	found = true;
      }
    
    if (std::count(terminated.begin(), terminated.end(), true) == mpi_size) break;
    
    non_found_iter = loop_sleep(found, non_found_iter);
  }
}

void synchronize_reducer(intercomm_type& mapper)
{
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  std::vector<MPI::Request, std::allocator<MPI::Request> > request(mpi_size);
  std::vector<bool, std::allocator<bool> > terminated(mpi_size, false);
  
  for (int rank = 0; rank != mpi_size; ++ rank)
    request[rank] = mapper.comm.Isend(0, 0, MPI::INT, rank, notify_tag);
  
  int non_found_iter = 0;
  for (;;) {
    bool found = false;
    
    for (int rank = 0; rank != mpi_size; ++ rank)
      if (! terminated[rank] && request[rank].Test()) {
	terminated[rank] = true;
	found = true;
      }
    
    if (std::count(terminated.begin(), terminated.end(), true) == mpi_size) break;
    
    non_found_iter = loop_sleep(found, non_found_iter);
  }
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
    
    stream[rank]->push(boost::iostreams::zlib_compressor());
    stream[rank]->push(*device[rank]);

    stream[rank]->precision(20);
  }
  
  const id_type bos_id = ngram.index.vocab()[vocab_type::BOS];
  const int max_order = ngram.index.order();
  
  count_set_type unigrams(ngram.index[mpi_rank].offsets[1], count_type(0));
  context_type   context;

  namespace karma = boost::spirit::karma;
  namespace standard = boost::spirit::standard;
  
  karma::uint_generator<id_type>    id_generator;
  karma::uint_generator<count_type> count_generator;
  
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
	  
	  std::ostream_iterator<char> iter(*stream[shard]);
	  
	  if (! karma::generate(iter, +(id_generator << ' ') << "1\n", boost::make_iterator_range(context.begin() + 1, context.end())))
	    throw std::runtime_error("failed generation");
	}
	
	if (context.front() == bos_id && order_prev + 1 != max_order) {
	  const int shard = ngram.index.shard_index(context.begin(), context.end());
	  
	  std::ostream_iterator<char> iter(*stream[shard]);
	  
	  if (! karma::generate(iter, +(id_generator << ' ') << count_generator << '\n', context, ngram.counts[mpi_rank][pos]))
	    throw std::runtime_error("failed generation");
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
  int non_found_iter = 0;
  
  for (;;) {
    if (std::count(device.begin(), device.end(), odevice_ptr_type()) == mpi_size) break;
    
    non_found_iter = loop_sleep(utils::mpi_terminate_devices(stream, device), non_found_iter);
  }
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
  
  typedef std::vector<utils::piece, std::allocator<utils::piece> > tokens_type;
  typedef boost::tokenizer<utils::space_separator, utils::piece::const_iterator, utils::piece> tokenizer_type;

  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  istream_ptr_set_type stream(mpi_size);
  idevice_ptr_set_type device(mpi_size);
  
  for (int rank = 0; rank < mpi_size; ++ rank) {
    stream[rank].reset(new istream_type());
    device[rank].reset(new idevice_type(mapper.comm, rank, modify_tag, 1024 * 1024));
    
    stream[rank]->push(boost::iostreams::zlib_decompressor());
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
	  utils::piece line_piece(line);
	  tokenizer_type tokenizer(line_piece);
	  tokens.clear();
	  tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
	  
	  if (tokens.size() < 2) continue;
	  
	  context.clear();
	  tokens_type::const_iterator titer_end = tokens.end() - 1;
	  for (tokens_type::const_iterator titer = tokens.begin(); titer != titer_end; ++ titer)
	    context.push_back(utils::lexical_cast<id_type>(*titer));
	  
	  std::pair<context_type::const_iterator, size_type> result = ngram.index.traverse(mpi_rank, context.begin(), context.end());
	  if (result.first != context.end() || result.second == size_type(-1))
	    throw std::runtime_error("no ngram?");
	  
	  counts_modified[result.second - offset] += utils::lexical_cast<count_type>(tokens.back());
	  
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

  ::sync();

  while (! ngram_type::shard_data_type::count_set_type::exists(path))
    boost::thread::yield();
  
  utils::tempfile::permission(path);
  
  ngram.types[mpi_rank].offset = ngram.counts[mpi_rank].offset;
  ngram.types[mpi_rank].counts.open(path);
}

int getoptions(int argc, char** argv)
{
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  namespace po = boost::program_options;
  
  po::options_description desc("options");
  desc.add_options()
    ("ngram",     po::value<path_type>(&ngram_file)->default_value(ngram_file),   "ngram counts in expgram format")
    ("output",    po::value<path_type>(&output_file)->default_value(output_file), "output in binary format")
    ("temporary", po::value<path_type>(&temporary_dir),                           "temporary directory")
    
    ("prog",       po::value<path_type>(&prog_name),  "this binary")
    ("host",       po::value<std::string>(&host),     "host name")
    ("hostfile",   po::value<std::string>(&hostfile), "hostfile name")    
    
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
