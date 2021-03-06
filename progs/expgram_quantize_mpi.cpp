//
//  Copyright(C) 2009-2012 Taro Watanabe <taro.watanabe@nict.go.jp>
//

#include <iostream>
#include <stdexcept>
#include <map>

#include <expgram/NGram.hpp>
#include <expgram/Quantizer.hpp>

#include <boost/thread.hpp>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

#include <utils/tempfile.hpp>
#include <utils/resource.hpp>
#include <utils/mpi.hpp>
#include <utils/unordered_map.hpp>

typedef expgram::NGram ngram_type;

typedef ngram_type::size_type       size_type;
typedef ngram_type::difference_type difference_type;
typedef ngram_type::path_type       path_type;

typedef ngram_type::logprob_type    logprob_type;
typedef ngram_type::quantized_type  quantized_type;
typedef ngram_type::word_type       word_type;
typedef ngram_type::id_type         id_type;

path_type ngram_file;
path_type output_file;
path_type temporary_dir = "";

path_type prog_name;
std::string host;
std::string hostfile;

int debug = 0;

void ngram_quantize(ngram_type& ngram);
int getoptions(int argc, char** argv);

int main(int argc, char** argv)
{
  utils::mpi_world  mpi_world(argc, argv);
  
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();  
  
  try {
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
    
    ngram_type ngram(debug);
    ngram.open_shard(ngram_file, mpi_rank);

    if (static_cast<int>(ngram.index.size()) != mpi_size)
      throw std::runtime_error("MPI universe size do not match with ngram shard size");
    
    utils::resource start;

    ngram_quantize(ngram);

    utils::resource end;
    
    if (debug && mpi_rank == 0)
      std::cerr << "quantize language model"
		<< " cpu time:  " << end.cpu_time() - start.cpu_time() 
		<< " user time: " << end.user_time() - start.user_time()
		<< std::endl;
    
    if (mpi_rank == 0)
      ngram.write_prepare(output_file);
    
    MPI::COMM_WORLD.Barrier();
    ngram.write_shard(output_file, mpi_rank);
  }
  catch (std::exception& err) {
    std::cerr << "error: "  << err.what() << std::endl;
    MPI::COMM_WORLD.Abort(1);
    return 1;
  }
  return 0;
}

template <typename OStream, typename LogProbs, typename Hashed, typename Counts, typename Codemap, typename Codebook>
inline
void quantize(ngram_type& ngram, OStream& os, LogProbs& logprobs, Hashed& hashed, Counts& counts, Codemap& codemap, Codebook& codebook, int order, int shard)
{
  hashed.clear();
  counts.clear();
  codemap.clear();
  
  const size_type pos_first = ngram.index[shard].offsets[order - 1];
  const size_type pos_last  = ngram.index[shard].offsets[order];
  
  for (size_type pos = pos_first; pos < pos_last; ++ pos)
    ++ hashed[logprobs(pos, order)];

  counts.insert(hashed.begin(), hashed.end());
  hashed.clear();
  
  expgram::Quantizer::quantize(ngram, counts, codebook, codemap);
  
  for (size_type pos = pos_first; pos < pos_last; ++ pos) {
    typename Codemap::const_iterator citer = codemap.find(logprobs(pos, order));
    if (citer == codemap.end())
      throw std::runtime_error("no codemap?");
	
    os.write((char*) &(citer->second), sizeof(quantized_type));
  }
}

typedef std::map<logprob_type, size_type, std::less<logprob_type>,
		 std::allocator<std::pair<const logprob_type, size_type> > > logprob_counts_type;
//typedef std::map<logprob_type, quantized_type, std::less<logprob_type>,
//		 std::allocator<std::pair<const logprob_type, quantized_type> > > codemap_type;
typedef utils::unordered_map<logprob_type, quantized_type, boost::hash<logprob_type>, std::equal_to<logprob_type>,
			     std::allocator<std::pair<const logprob_type, quantized_type> > >::type codemap_type;
typedef utils::unordered_map<logprob_type, size_type, boost::hash<logprob_type>, std::equal_to<logprob_type>,
			     std::allocator<std::pair<const logprob_type, size_type> > >::type hashed_type;

void ngram_quantize(ngram_type& ngram)
{
  typedef ngram_type::shard_data_type shard_data_type;
  
  typedef shard_data_type::logprob_map_type logprob_map_type;

  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();

  const path_type tmp_dir = utils::tempfile::tmp_dir();
  
  hashed_type         hashed;
  logprob_counts_type counts;
  codemap_type        codemap;
  logprob_map_type    codebook;
  
  if (! ngram.logprobs[mpi_rank].quantized.is_open() && ngram.logprobs[mpi_rank].logprobs.is_open()) {

    if (debug)
      std::cerr << "shard: " << mpi_rank << " quantize logprob" << std::endl;
    
    const path_type path = utils::tempfile::directory_name(tmp_dir / "expgram.logprob.quantized.XXXXXX");
    utils::tempfile::insert(path);
    
    boost::iostreams::filtering_ostream os;
    os.push(utils::packed_sink<quantized_type, std::allocator<quantized_type> >(path));
    
    std::fill(codebook.begin(), codebook.end(), logprob_type(0.0));
    
    ngram.logprobs[mpi_rank].maps.clear();
    ngram.logprobs[mpi_rank].maps.push_back(codebook);
    
    if (mpi_rank == 0) {
      quantize(ngram, os, ngram.logprobs[mpi_rank], hashed, counts, codemap, codebook, 1, mpi_rank);
      ngram.logprobs[mpi_rank].maps.push_back(codebook);
    } else
      ngram.logprobs[mpi_rank].maps.push_back(codebook);
    
    for (int order = 2; order <= ngram.index.order(); ++ order) {
      quantize(ngram, os, ngram.logprobs[mpi_rank], hashed, counts, codemap, codebook, order, mpi_rank);
      ngram.logprobs[mpi_rank].maps.push_back(codebook);
    }
    
    os.pop();
    ::sync();
    
    while (! ngram_type::shard_data_type::quantized_set_type::exists(path))
      boost::thread::yield();
    
    utils::tempfile::permission(path);
    
    ngram.logprobs[mpi_rank].logprobs.clear();
    ngram.logprobs[mpi_rank].quantized.open(path);
  }
  
  if (! ngram.backoffs[mpi_rank].quantized.is_open() && ngram.backoffs[mpi_rank].logprobs.is_open()) {

    if (debug)
      std::cerr << "shard: " << mpi_rank << " quantize backoff" << std::endl;

    const path_type path = utils::tempfile::directory_name(tmp_dir / "expgram.backoff.quantized.XXXXXX");
    utils::tempfile::insert(path);
	
    boost::iostreams::filtering_ostream os;
    os.push(utils::packed_sink<quantized_type, std::allocator<quantized_type> >(path));
	
    std::fill(codebook.begin(), codebook.end(), logprob_type(0.0));
	
    ngram.backoffs[mpi_rank].maps.clear();
    ngram.backoffs[mpi_rank].maps.push_back(codebook);
    if (mpi_rank == 0) {
      quantize(ngram, os, ngram.backoffs[mpi_rank], hashed, counts, codemap, codebook, 1, mpi_rank);
      ngram.backoffs[mpi_rank].maps.push_back(codebook);
    } else
      ngram.backoffs[mpi_rank].maps.push_back(codebook);
	
    for (int order = 2; order < ngram.index.order(); ++ order) {
      quantize(ngram, os, ngram.backoffs[mpi_rank], hashed, counts, codemap, codebook, order, mpi_rank);
      ngram.backoffs[mpi_rank].maps.push_back(codebook);
    }
	
    os.pop();
    ::sync();
    
    while (! ngram_type::shard_data_type::quantized_set_type::exists(path))
      boost::thread::yield();

    utils::tempfile::permission(path);
	
    ngram.backoffs[mpi_rank].logprobs.clear();
    ngram.backoffs[mpi_rank].quantized.open(path);
  }
      
  if (! ngram.logbounds[mpi_rank].quantized.is_open() && ngram.logbounds[mpi_rank].logprobs.is_open()) {
    
    if (debug)
      std::cerr << "shard: " << mpi_rank << " quantize logbound" << std::endl;
    
    const path_type path = utils::tempfile::directory_name(tmp_dir / "expgram.logbound.quantized.XXXXXX");
    utils::tempfile::insert(path);
	
    boost::iostreams::filtering_ostream os;
    os.push(utils::packed_sink<quantized_type, std::allocator<quantized_type> >(path));
	
    std::fill(codebook.begin(), codebook.end(), logprob_type(0.0));
    
    ngram.logbounds[mpi_rank].maps.clear();
    ngram.logbounds[mpi_rank].maps.push_back(codebook);
    if (mpi_rank == 0) {
      quantize(ngram, os, ngram.logbounds[mpi_rank], hashed, counts, codemap, codebook, 1, mpi_rank);
      ngram.logbounds[mpi_rank].maps.push_back(codebook);
    } else
      ngram.logbounds[mpi_rank].maps.push_back(codebook);
	
    for (int order = 2; order < ngram.index.order(); ++ order) {
      quantize(ngram, os, ngram.logbounds[mpi_rank], hashed, counts, codemap, codebook, order, mpi_rank);
      ngram.logbounds[mpi_rank].maps.push_back(codebook);
    }
    
    os.pop();
    ::sync();
    
    while (! ngram_type::shard_data_type::quantized_set_type::exists(path))
      boost::thread::yield();
    
    utils::tempfile::permission(path);
    
    ngram.logbounds[mpi_rank].logprobs.clear();
    ngram.logbounds[mpi_rank].quantized.open(path);
  }
}


int getoptions(int argc, char** argv)
{
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  namespace po = boost::program_options;
  
  po::options_description desc("options");
  desc.add_options()
    ("ngram",     po::value<path_type>(&ngram_file)->default_value(ngram_file),   "ngram language model in expgram format")
    ("output",    po::value<path_type>(&output_file)->default_value(output_file), "output in expgram format")
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
