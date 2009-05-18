
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

path_type prog_name;

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
    
    if (ngram_file.empty() || ! boost::filesystem::exists(ngram_file))
      throw std::runtime_error("no ngram file?");
    if (output_file.empty())
      throw std::runtime_error("no output file?");
    if (ngram_file == output_file)
      throw std::runtime_error("dump to the same directory?");
    
    ngram_type ngram(debug);
    ngram.open_shard(ngram_file, mpi_rank);

    if (ngram.index.size() != mpi_size)
      throw std::runtime_error("MPI universe size do not match with ngram shard size");
    
    ngram_quantize(ngram);
    
    if (mpi_rank == 0)
      ngram.write_prepare(output_file);
    
    MPI::COMM_WORLD.Barrier();
    ngram.write_shard(output_file, mpi_rank);
  }
  catch (std::exception& err) {
    std::cerr << "error: "  << err.what() << std::endl;
    return 1;
  }
  return 0;
}

template <typename OStream, typename LogProbs, typename Counts, typename Codemap, typename Codebook>
inline
void quantize(ngram_type& ngram, OStream& os, LogProbs& logprobs, Counts& counts, Codemap& codemap, Codebook& codebook, int order, int shard)
{
  counts.clear();
  codemap.clear();
  
  const size_type pos_first = ngram.index[shard].offsets[order - 1];
  const size_type pos_last  = ngram.index[shard].offsets[order];
  
  for (size_type pos = pos_first; pos < pos_last; ++ pos)
    ++ counts[logprobs(pos, order)];
  
  expgram::Quantizer::quantize(counts, ngram.logprob_min(), codebook, codemap);
  
  for (size_type pos = pos_first; pos < pos_last; ++ pos) {
    typename Codemap::const_iterator citer = codemap.find(logprobs(pos, order));
    if (citer == codemap.end())
      throw std::runtime_error("no codemap?");
	
    os.write((char*) &(citer->second), sizeof(quantized_type));
  }
}

typedef std::map<logprob_type, size_type, std::less<logprob_type>,
		 std::allocator<std::pair<const logprob_type, size_type> > > logprob_counts_type;
typedef std::map<logprob_type, quantized_type, std::less<logprob_type>,
		 std::allocator<std::pair<const logprob_type, quantized_type> > > codemap_type;


void ngram_quantize(ngram_type& ngram)
{
  typedef ngram_type::shard_data_type shard_data_type;
  
  typedef shard_data_type::logprob_map_type logprob_map_type;

  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();

  const path_type tmp_dir = utils::tempfile::tmp_dir();
  

  logprob_counts_type counts;
  codemap_type        codemap;
  logprob_map_type    codebook;
  
  if (! ngram.logprobs[mpi_rank].quantized.is_open() && ngram.logprobs[mpi_rank].logprobs.is_open()) {
    
    const path_type path = utils::tempfile::directory_name(tmp_dir / "expgram.logprob.quantized.XXXXXX");
    utils::tempfile::insert(path);
    
    boost::iostreams::filtering_ostream os;
    os.push(utils::packed_sink<quantized_type, std::allocator<quantized_type> >(path));
    
    std::fill(codebook.begin(), codebook.end(), logprob_type(0.0));
    
    ngram.logprobs[mpi_rank].maps.clear();
    ngram.logprobs[mpi_rank].maps.push_back(codebook);
    
    if (mpi_rank == 0) {
      quantize(ngram, os, ngram.logprobs[mpi_rank], counts, codemap, codebook, 1, mpi_rank);
      ngram.logprobs[mpi_rank].maps.push_back(codebook);
    } else
      ngram.logprobs[mpi_rank].maps.push_back(codebook);
    
    for (int order = 2; order <= ngram.index.order(); ++ order) {
      quantize(ngram, os, ngram.logprobs[mpi_rank], counts, codemap, codebook, order, mpi_rank);
      ngram.logprobs[mpi_rank].maps.push_back(codebook);
    }
    
    os.pop();
    utils::tempfile::permission(path);
    
    ngram.logprobs[mpi_rank].logprobs.clear();
    ngram.logprobs[mpi_rank].quantized.open(path);
  }
  
  if (! ngram.backoffs[mpi_rank].quantized.is_open() && ngram.backoffs[mpi_rank].logprobs.is_open()) {
    const path_type path = utils::tempfile::directory_name(tmp_dir / "expgram.backoff.quantized.XXXXXX");
    utils::tempfile::insert(path);
	
    boost::iostreams::filtering_ostream os;
    os.push(utils::packed_sink<quantized_type, std::allocator<quantized_type> >(path));
	
    std::fill(codebook.begin(), codebook.end(), logprob_type(0.0));
	
    ngram.backoffs[mpi_rank].maps.clear();
    ngram.backoffs[mpi_rank].maps.push_back(codebook);
    if (mpi_rank == 0) {
      quantize(ngram, os, ngram.backoffs[mpi_rank], counts, codemap, codebook, 1, mpi_rank);
      ngram.backoffs[mpi_rank].maps.push_back(codebook);
    } else
      ngram.backoffs[mpi_rank].maps.push_back(codebook);
	
    for (int order = 2; order < ngram.index.order(); ++ order) {
      quantize(ngram, os, ngram.backoffs[mpi_rank], counts, codemap, codebook, order, mpi_rank);
      ngram.backoffs[mpi_rank].maps.push_back(codebook);
    }
	
    os.pop();

    utils::tempfile::permission(path);
	
    ngram.backoffs[mpi_rank].logprobs.clear();
    ngram.backoffs[mpi_rank].quantized.open(path);
  }
      
  if (! ngram.logbounds[mpi_rank].quantized.is_open() && ngram.logbounds[mpi_rank].logprobs.is_open()) {
    const path_type path = utils::tempfile::directory_name(tmp_dir / "expgram.logbound.quantized.XXXXXX");
    utils::tempfile::insert(path);
	
    boost::iostreams::filtering_ostream os;
    os.push(utils::packed_sink<quantized_type, std::allocator<quantized_type> >(path));
	
    std::fill(codebook.begin(), codebook.end(), logprob_type(0.0));
    
    ngram.logbounds[mpi_rank].maps.clear();
    ngram.logbounds[mpi_rank].maps.push_back(codebook);
    if (mpi_rank == 0) {
      quantize(ngram, os, ngram.logbounds[mpi_rank], counts, codemap, codebook, 1, mpi_rank);
      ngram.logbounds[mpi_rank].maps.push_back(codebook);
    } else
      ngram.logbounds[mpi_rank].maps.push_back(codebook);
	
    for (int order = 2; order < ngram.index.order(); ++ order) {
      quantize(ngram, os, ngram.logbounds[mpi_rank], counts, codemap, codebook, order, mpi_rank);
      ngram.logbounds[mpi_rank].maps.push_back(codebook);
    }
    
    os.pop();
    
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
