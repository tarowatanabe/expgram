//
//  Copyright(C) 2009-2013 Taro Watanabe <taro.watanabe@nict.go.jp>
//

#include <iostream>
#include <stdexcept>

#include <vector>

#include <expgram/NGram.hpp>

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
#include <utils/base64.hpp>
#include <utils/lexical_cast.hpp>

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

struct shard_data_type
{
  typedef boost::iostreams::filtering_ostream ostream_type;
  
  ostream_type os_logprob;
  ostream_type os_logbound;
  ostream_type os_backoff;
  path_type    path_logprob;
  path_type    path_logbound;
  path_type    path_backoff;
};


path_type ngram_file;
path_type output_file;
path_type temporary_dir = "";

path_type prog_name;
std::string host;
std::string hostfile;

int debug = 0;

void ngram_backward_prepare(const ngram_type& ngram, ngram_type& ngram_backward, shard_data_type& shard);
void ngram_backward_mapper(const ngram_type& ngram, intercomm_type& reducer);
void ngram_backward_reducer(ngram_type& ngram, shard_data_type& shard, intercomm_type& mapper);

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

      if (static_cast<int>(ngram.index.size()) != mpi_size)
	throw std::runtime_error("MPI universe size do not match with ngram shard size");
      
      // currently, this is our requirement...
      if (ngram.logbounds.empty())
	throw std::runtime_error("no upper bound estiamtes...?");
      
      if (ngram.index.backward())
	throw std::runtime_error("this is already a backward structure");
      
      ngram_type      backward;
      shard_data_type shard;
      
      // prepare data structures!.
      ngram_backward_prepare(ngram, backward, shard);
      
      // reduce backward data structure
      ngram_backward_reducer(backward, shard, comm_parent);
      
      if (mpi_rank == 0)
	backward.write_prepare(output_file);
      
      MPI::COMM_WORLD.Barrier();
      backward.write_shard(output_file, mpi_rank);
      
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
      
      if (ngram.index.size() != mpi_size)
	throw std::runtime_error("MPI universe size do not match with ngram shard size");
      
      // currently, this is our requirement...
      if (ngram.logbounds.empty())
	throw std::runtime_error("no upper bound estiamtes...?");
      
      if (ngram.index.backward())
	throw std::runtime_error("this is already a backward structure");
      
      ngram_backward_mapper(ngram, comm_child);
      
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
  backward_tag = 1000,
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


struct ngram_generator_type
{

  boost::spirit::karma::uint_generator<id_type> word_id;
  
  template <typename Context, typename Logprob>
  void operator()(std::ostream& os, const Context& context, const Logprob& logprob, const Logprob& logbound, const Logprob& backoff)
  {
    namespace karma = boost::spirit::karma;
    namespace standard = boost::spirit::standard;

    typedef std::ostream_iterator<char> iter_type;

    
    karma::generate(iter_type(os), (word_id % ' '), context);
    os << ' ';
    utils::encode_base64(logprob, iter_type(os));
    os << ' ';
    utils::encode_base64(logbound, iter_type(os));
    os << ' ';
    utils::encode_base64(backoff, iter_type(os));
    os << '\n';
  }
};

void ngram_backward_mapper(const ngram_type& ngram, intercomm_type& reducer)
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
    device[rank].reset(new odevice_type(reducer.comm, rank, backward_tag, 1024 * 1024, false, true));
    
    stream[rank]->push(boost::iostreams::zlib_compressor());
    stream[rank]->push(*device[rank]);
  }
  
  context_type context;
  
  ngram_generator_type ngram_generator;

  const logprob_type logprob_min = ngram_type::logprob_min();
  
  const size_type order_max = ngram.index.order();
  for (size_type order_prev = 1; order_prev != order_max; ++ order_prev) {
    const size_type pos_context_first = ngram.index[mpi_rank].offsets[order_prev - 1];
    const size_type pos_context_last  = ngram.index[mpi_rank].offsets[order_prev];
    
    if (debug)
      std::cerr << "rank: " << mpi_rank << " order: " << (order_prev + 1) << std::endl;

    const size_type context_size = order_prev + 1;
    
    context.resize(context_size);
    
    size_type pos_last_prev = pos_context_last;
    for (size_type pos_context = pos_context_first; pos_context < pos_context_last; ++ pos_context) {
      const size_type pos_first = pos_last_prev;
      const size_type pos_last = ngram.index[mpi_rank].children_last(pos_context);
      pos_last_prev = pos_last;
      
      if (pos_first == pos_last) continue;
      
      // in the forward mode, if we traverse backward, and store context in left-to-right, it is inversed!
      context_type::iterator citer_curr = context.begin() + 1;
      for (size_type pos_curr = pos_context; pos_curr != size_type(-1); pos_curr = ngram.index[mpi_rank].parent(pos_curr), ++ citer_curr)
	*citer_curr = ngram.index[mpi_rank][pos_curr];
      
      for (size_type pos = pos_first; pos != pos_last; ++ pos) {
	context.front() = ngram.index[mpi_rank][pos];
	
	const logprob_type logprob  = ngram.logprobs[mpi_rank](pos, context_size);
	const logprob_type logbound = (context_size != order_max
				       ? ngram.logbounds[mpi_rank](pos, context_size)
				       : logprob_min);
	const logprob_type backoff  = (context_size != order_max
				       ? ngram.backoffs[mpi_rank](pos, context_size)
				       : logprob_min);
	
	// context is already reversed!
	const size_type shard_index = ngram.index.shard_index(context.begin(), context.end());
	
	ngram_generator(*stream[shard_index], context, logprob, logbound, backoff);
      }
      
      if (utils::mpi_flush_devices(stream, device))
	boost::thread::yield();
    }
  }
  
  
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


void ngram_backward_prepare(const ngram_type& ngram, ngram_type& ngram_backward, shard_data_type& shard)
{
  // prepare shard data structure..
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  const size_type shard_size = ngram.index.size();
  
  ngram_backward.index.reserve(shard_size);
  ngram_backward.index.resize(shard_size);
  ngram_backward.index.vocab() = ngram.index.vocab();
  ngram_backward.index.order() = ngram.index.order();
  ngram_backward.index.backward() = true;
  
  ngram_backward.logprobs.reserve(shard_size);
  ngram_backward.logbounds.reserve(shard_size);
  ngram_backward.backoffs.reserve(shard_size);
  
  ngram_backward.logprobs.resize(shard_size);
  ngram_backward.logbounds.resize(shard_size);
  ngram_backward.backoffs.resize(shard_size);
  
  ngram_backward.smooth = ngram.smooth;
  ngram_backward.debug  = ngram.debug;
  
  // setup paths and streams for logprob/backoff
  const path_type tmp_dir = utils::tempfile::tmp_dir();
  
  
  shard.path_logprob  = utils::tempfile::file_name(tmp_dir / "expgram.logprob.XXXXXX");
  shard.path_logbound = utils::tempfile::file_name(tmp_dir / "expgram.logbound.XXXXXX");
  shard.path_backoff  = utils::tempfile::file_name(tmp_dir / "expgram.backoff.XXXXXX");
  
  utils::tempfile::insert(shard.path_logprob);
  utils::tempfile::insert(shard.path_logbound);
  utils::tempfile::insert(shard.path_backoff);
  
  shard.os_logprob.push(boost::iostreams::file_sink(shard.path_logprob.string()),   1024 * 1024);
  shard.os_logbound.push(boost::iostreams::file_sink(shard.path_logbound.string()), 1024 * 1024);
  shard.os_backoff.push(boost::iostreams::file_sink(shard.path_backoff.string()),   1024 * 1024);
  
  const size_type unigram_size = ngram.index[mpi_rank].offsets[1];
  
  if (mpi_rank == 0)
    for (size_type pos = 0; pos != unigram_size; ++ pos) {
      const logprob_type logprob  = ngram.logprobs[0](pos, 1);
      const logprob_type logbound = ngram.logbounds[0](pos, 1);
      const logprob_type backoff  = ngram.backoffs[0](pos, 1);
      
      shard.os_logprob.write((char*) &logprob,   sizeof(logprob_type));
      shard.os_logbound.write((char*) &logbound, sizeof(logprob_type));
      shard.os_backoff.write((char*) &backoff,   sizeof(logprob_type));
    }
  
  ngram_backward.index[mpi_rank].offsets.clear();
  ngram_backward.index[mpi_rank].offsets.push_back(0);
  ngram_backward.index[mpi_rank].offsets.push_back(unigram_size);
  
  ngram_backward.logprobs[mpi_rank].offset  = utils::bithack::branch(mpi_rank == 0, size_type(0), unigram_size);
  ngram_backward.logbounds[mpi_rank].offset = utils::bithack::branch(mpi_rank == 0, size_type(0), unigram_size);
  ngram_backward.backoffs[mpi_rank].offset  = utils::bithack::branch(mpi_rank == 0, size_type(0), unigram_size);
}

struct Task
{
  typedef std::vector<path_type, std::allocator<path_type> > path_set_type;
  
  typedef std::vector<id_type, std::allocator<id_type> > context_type;
    
  struct logprob_set_type
  {
    logprob_type prob;
    logprob_type bound;
    logprob_type backoff;
    
    logprob_set_type() : prob(0.0), bound(0.0), backoff(0.0) {}
    logprob_set_type(const logprob_type& __prob,
		     const logprob_type& __bound,
		     const logprob_type& __backoff)
      : prob(__prob), bound(__bound), backoff(__backoff) {}
  };
  
  typedef std::pair<context_type, logprob_set_type> context_logprob_set_type;
  
  
  typedef std::pair<id_type, logprob_set_type>                                       word_logprob_set_type;
  typedef std::vector<word_logprob_set_type, std::allocator<word_logprob_set_type> > word_logprob_map_type;
  
  typedef utils::packed_vector<id_type, std::allocator<id_type> >  id_vector_type;
  typedef std::vector<size_type, std::allocator<size_type> >       size_vector_type;
  
  Task(ngram_type&      _ngram,
       shard_data_type& _shard_data,
       const int        _shard,
       const int        _max_order,
       const int        _debug)
    : ngram(_ngram),
      shard_data(_shard_data),
      shard(_shard),
      max_order(_max_order),
      debug(_debug) {}
  
  ngram_type&         ngram;
  shard_data_type&    shard_data;
  int                 shard;
  int                 max_order;
  int                 debug;

  // local
  id_vector_type      ids;
  size_vector_type    positions_first;
  size_vector_type    positions_last;
  
  void index_ngram()
  {
    typedef utils::succinct_vector<std::allocator<int32_t> > position_set_type;
      
    const int order_prev = ngram.index[shard].offsets.size() - 1;
    const size_type positions_size = ngram.index[shard].offsets[order_prev] - ngram.index[shard].offsets[order_prev - 1];
      
    const path_type tmp_dir       = utils::tempfile::tmp_dir();
    const path_type path_id       = utils::tempfile::directory_name(tmp_dir / "expgram.index.XXXXXX");
    const path_type path_position = utils::tempfile::directory_name(tmp_dir / "expgram.position.XXXXXX");
      
    utils::tempfile::insert(path_id);
    utils::tempfile::insert(path_position);
      
    if (debug)
      std::cerr << "perform indexing: " << (order_prev + 1) << " shard: " << shard << std::endl;
      
    position_set_type positions;
    if (ngram.index[shard].positions.is_open())
      positions = ngram.index[shard].positions;
      
    boost::iostreams::filtering_ostream os_id;
    os_id.push(utils::packed_sink<id_type, std::allocator<id_type> >(path_id));
      
    if (ngram.index[shard].ids.is_open())
      for (size_type pos = 0; pos < ngram.index[shard].ids.size(); ++ pos) {
	const id_type id = ngram.index[shard].ids[pos];
	os_id.write((char*) &id, sizeof(id_type));
      }
    
    ids.build();

    for (size_type i = 0; i < positions_size; ++ i) {
      const size_type pos_first = positions_first[i];
      const size_type pos_last  = positions_last[i];
	
      if (pos_last > pos_first)
	for (size_type pos = pos_first; pos != pos_last; ++ pos) {
	  const id_type id = ids[pos];
	  os_id.write((char*) &id, sizeof(id_type));
	  positions.set(positions.size(), true);
	}
      
      positions.set(positions.size(), false);
    }
      
    // perform indexing...
    os_id.pop();
    positions.write(path_position);

    while (! boost::filesystem::exists(path_id))
      boost::thread::yield();
    while (! boost::filesystem::exists(path_position))
      boost::thread::yield();
    
    utils::tempfile::permission(path_id);
    utils::tempfile::permission(path_position);
    
    // close and remove old index...
    if (ngram.index[shard].ids.is_open()) {
      const path_type path = ngram.index[shard].ids.path();
      ngram.index[shard].ids.close();
      utils::filesystem::remove_all(path);
      utils::tempfile::erase(path);
    }
    if (ngram.index[shard].positions.is_open()) {
      const path_type path = ngram.index[shard].positions.path();
      ngram.index[shard].positions.close();
      utils::filesystem::remove_all(path);
      utils::tempfile::erase(path);
    }
      
    // new index
    ngram.index[shard].ids.open(path_id);
    ngram.index[shard].positions.open(path_position);
    ngram.index[shard].offsets.push_back(ngram.index[shard].offsets.back() + ids.size());
    ngram.index[shard].clear_cache();
      
    if (debug)
      std::cerr << "shard: " << shard
		<< " index: " << ngram.index[shard].ids.size()
		<< " positions: " << ngram.index[shard].positions.size()
		<< " offsets: "  << ngram.index[shard].offsets.back()
		<< std::endl;

    // remove temporary index
    ids.clear();
    positions_first.clear();
    positions_last.clear();
  }
  
  void index_ngram(const context_type& prefix, word_logprob_map_type& words)
  {
    const int order_prev = ngram.index[shard].offsets.size() - 1;
    const size_type positions_size = ngram.index[shard].offsets[order_prev] - ngram.index[shard].offsets[order_prev - 1];
      
    if (positions_first.empty()) {
      positions_first.reserve(positions_size);
      positions_first.resize(positions_size, size_type(0));
    }
    if (positions_last.empty()) {
      positions_last.reserve(positions_size);
      positions_last.resize(positions_size, size_type(0));
    }

    // we still need to perform "search" since lower-order ngram may have no extension..
    
    std::pair<context_type::const_iterator, size_type> result = ngram.index.traverse(shard, prefix.begin(), prefix.end());
    if (result.first != prefix.end() || result.second == size_type(-1)) {
      std::ostringstream stream;
	
      stream << "No prefix: shard=" << shard;
      context_type::const_iterator citer_end = prefix.end();
      for (context_type::const_iterator citer = prefix.begin(); citer != citer_end; ++ citer)
	stream << ' '  << ngram.index.vocab()[*citer];
	
      throw std::runtime_error(stream.str());
    }
    
    const size_type pos = result.second - ngram.index[shard].offsets[order_prev - 1];
    positions_first[pos] = ids.size();
    positions_last[pos] = ids.size() + words.size();
    
    word_logprob_map_type::const_iterator witer_end = words.end();
    for (word_logprob_map_type::const_iterator witer = words.begin(); witer != witer_end; ++ witer) {
      ids.push_back(witer->first);
      
      shard_data.os_logprob.write((char*) &(witer->second.prob), sizeof(logprob_type));
      
      if (order_prev + 1 != max_order) {
	static const logprob_type logprob_zero(0.0);
	
	shard_data.os_logbound.write((char*) &(witer->second.bound), sizeof(logprob_type));
	shard_data.os_backoff.write(witer->second.backoff != ngram_type::logprob_min()
				    ? (char*) &(witer->second.backoff)
				    : (char*) &logprob_zero,
				    sizeof(logprob_type));
      }
    }
  }  
  
  template <typename Tp>
  struct sized_key_less
  {
    sized_key_less(size_type __size)  : size(__size) {}
    
    bool operator()(const Tp* x, const Tp* y) const
    {
      return std::lexicographical_compare(x, x + size, y, y + size);
    }
    
    size_type size;
  };

  void index_ngram(const path_type& path, const int order)
  {
    typedef utils::map_file<id_type, std::allocator<id_type> > mapped_type;
    typedef std::vector<const id_type*, std::allocator<const id_type*> > ngram_set_type;
    
    const size_type offset = sizeof(logprob_set_type) / sizeof(id_type);
    if (offset != 3 || sizeof(logprob_set_type) % sizeof(id_type) != 0)
      throw std::runtime_error("invalid offset??");
    
    while (! boost::filesystem::exists(path))
      boost::thread::yield();      
    
    // performing sorting...
    mapped_type mapped(path);
      
    // quit if no mapped file...
    if (mapped.empty()) return;
      
    // throw for invalid data....
    if (mapped.size() % (order + offset) != 0)
      throw std::runtime_error("invalid mapped size...????");
      
    const size_type ngrams_size = mapped.size() / (order + offset);

    ngram_set_type ngrams(ngrams_size);
      
    ngram_set_type::iterator niter = ngrams.begin();
    for (mapped_type::const_iterator iter = mapped.begin(); iter != mapped.end(); iter += order + offset, ++ niter)
      *niter = &(*iter);
      
    // actual sorting!
    std::sort(ngrams.begin(), ngrams.end(), sized_key_less<id_type>(order));

    // compute prefix+words...
      
    context_type prefix;
    word_logprob_map_type words;
      
    ngram_set_type::const_iterator niter_end = ngrams.end();
    for (ngram_set_type::const_iterator niter = ngrams.begin(); niter != niter_end; ++ niter) {
      const id_type* context = *niter;
      const id_type word = *(context + order - 1);
      const logprob_set_type& logprobs = *reinterpret_cast<const logprob_set_type*>(context + order);
      
      if (prefix.empty() || ! std::equal(prefix.begin(), prefix.end(), context)) {
	if (! words.empty()) {
	  index_ngram(prefix, words);
	  words.clear();
	}
	
	prefix.clear();
	prefix.insert(prefix.end(), context, context + order - 1);
      }
	
      if (words.empty() || words.back().first != word)
	words.push_back(std::make_pair(word, logprobs));
      else {
	if (words.back().second.prob == ngram_type::logprob_min())
	  words.back().second.prob = logprobs.prob;
	if (words.back().second.bound == ngram_type::logprob_min())
	  words.back().second.bound = logprobs.bound;
	if (words.back().second.backoff == ngram_type::logprob_min())
	  words.back().second.backoff = logprobs.backoff;
      }
    }
    
    // clear unused data...
    mapped.clear();
    
    ngrams.clear();
    ngram_set_type(ngrams).swap(ngrams);

    // perform final indexing
    if (! words.empty()) {
      index_ngram(prefix, words);
      words.clear();
      index_ngram();
    }
  }
  
  struct ngram_data_type
  {
    typedef boost::iostreams::filtering_ostream ostream_type;
    
    path_type    path;
    ostream_type os;
    
    ngram_data_type() {}
    ngram_data_type(const ngram_data_type& ) {}
    ngram_data_type& operator=(const ngram_data_type& x) { return *this; }
  };
  typedef std::vector<ngram_data_type, std::allocator<ngram_data_type> > ngram_data_set_type;

  ngram_data_set_type ngrams;
  
  void initialize()
  {
    const path_type tmp_dir = utils::tempfile::tmp_dir();
    
    ngrams.reserve(ngram.index.order() + 1);
    ngrams.resize(ngram.index.order() + 1);
    
    for (int order = 2; order <= ngram.index.order(); ++ order) {
      const path_type path = utils::tempfile::file_name(tmp_dir / "expgram.ngram.XXXXXX");
      
      utils::tempfile::insert(path);
      
      ngrams[order].path = path;
      ngrams[order].os.push(boost::iostreams::file_sink(path.string(), std::ios_base::out | std::ios_base::trunc), 1024 * 1024);
      ngrams[order].os.exceptions(std::ostream::eofbit | std::ostream::failbit | std::ostream::badbit);
    }
  }
  
  // call this!
  void finalize()
  {
    // final indexing...
    for (int order = 2; order <= ngram.index.order(); ++ order) {
      if (debug)
	std::cerr << "indexing: shard=" << shard << " order=" << order << std::endl;
      
      ngrams[order].os.reset();
      
      index_ngram(ngrams[order].path, order);
      
      boost::filesystem::remove(ngrams[order].path);
      utils::tempfile::erase(ngrams[order].path);
    }
    
    // finalization....
    shard_data.os_logprob.reset();
    shard_data.os_logbound.reset();
    shard_data.os_backoff.reset();
    
    while (! boost::filesystem::exists(shard_data.path_logprob))
      boost::thread::yield();      
    while (! boost::filesystem::exists(shard_data.path_logbound))
      boost::thread::yield();
    while (! boost::filesystem::exists(shard_data.path_backoff))
      boost::thread::yield();
      
    utils::tempfile::permission(shard_data.path_logprob);
    utils::tempfile::permission(shard_data.path_logbound);
    utils::tempfile::permission(shard_data.path_backoff);
      
    ngram.logprobs[shard].logprobs.open(shard_data.path_logprob);
    ngram.logbounds[shard].logprobs.open(shard_data.path_logbound);
    ngram.backoffs[shard].logprobs.open(shard_data.path_backoff);
  }
  
  void operator()(context_logprob_set_type& context_logprobs)
  {
    const context_type&     context  = context_logprobs.first;
    const logprob_set_type& logprobs = context_logprobs.second;
    
    const int order = context.size();
    
    ngrams[order].os.write((char*) &(*context.begin()), sizeof(context_type::value_type) * order);
    ngrams[order].os.write((char*) &logprobs, sizeof(logprob_set_type));
  }
};


void ngram_backward_reducer(ngram_type& ngram, shard_data_type& shard, intercomm_type& mapper)
{
  typedef boost::iostreams::filtering_istream istream_type;
  typedef utils::mpi_device_source            idevice_type;
  
  typedef boost::shared_ptr<istream_type> istream_ptr_type;
  typedef boost::shared_ptr<idevice_type> idevice_ptr_type;
  
  typedef std::vector<istream_ptr_type, std::allocator<istream_ptr_type> > istream_ptr_set_type;
  typedef std::vector<idevice_ptr_type, std::allocator<idevice_ptr_type> > idevice_ptr_set_type;
  
  typedef std::vector<utils::piece, std::allocator<utils::piece> > tokens_type;
  typedef boost::tokenizer<utils::space_separator, utils::piece::const_iterator, utils::piece> tokenizer_type;

  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  istream_ptr_set_type stream(mpi_size);
  idevice_ptr_set_type device(mpi_size);
  
  for (int rank = 0; rank < mpi_size; ++ rank) {
    stream[rank].reset(new istream_type());
    device[rank].reset(new idevice_type(mapper.comm, rank, backward_tag, 1024 * 1024));
    
    stream[rank]->push(boost::iostreams::zlib_decompressor());
    stream[rank]->push(*device[rank]);
  }

  typedef Task reducer_type;
  typedef reducer_type::context_type             context_type;
  typedef reducer_type::logprob_set_type         logprob_set_type;
  typedef reducer_type::context_logprob_set_type context_logprob_set_type;
  
  reducer_type reducer(ngram, shard, mpi_rank, ngram.index.order(), debug);

  // initialize here!
  reducer.initialize();
  
  std::string line;
  tokens_type tokens;
  context_logprob_set_type context_logprobs;
  
  for (;;) {
    bool found = false;
    
    for (int rank = 0; rank < mpi_size; ++ rank) 
      while (stream[rank] && device[rank] && device[rank]->test()) {
	
	if (std::getline(*stream[rank], line)) {
	  utils::piece line_piece(line);
	  tokenizer_type tokenizer(line_piece);
	  tokens.clear();
	  tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
	  
	  // 3 for logprob, logbound and backoff
	  if (tokens.size() < 4) continue;
	  
	  context_type&     context  = context_logprobs.first;
	  logprob_set_type& logprobs = context_logprobs.second;
	  
	  context.clear();
	  tokens_type::const_iterator titer     = tokens.begin();
	  tokens_type::const_iterator titer_end = tokens.end() - 3;
	  for (/**/; titer != titer_end; ++ titer)
	    context.push_back(utils::lexical_cast<id_type>(*titer));
	  
	  logprobs.prob    = utils::decode_base64<float>(*titer); ++ titer;
	  logprobs.bound   = utils::decode_base64<float>(*titer); ++ titer;
	  logprobs.backoff = utils::decode_base64<float>(*titer); ++ titer;
	  
	  reducer(context_logprobs);
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

  reducer.finalize();
}

int getoptions(int argc, char** argv)
{
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  namespace po = boost::program_options;
  
  po::options_description desc("options");
  desc.add_options()
    ("ngram",     po::value<path_type>(&ngram_file)->default_value(ngram_file),   "ngram language model in expgram format")
    ("output",    po::value<path_type>(&output_file)->default_value(output_file), "output in expgram format with an efficient backward trie structure")
    ("temporary", po::value<path_type>(&temporary_dir),                           "temporary directory")
    

    ("prog",   po::value<path_type>(&prog_name),   "this binary")
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
