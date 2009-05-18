
#include <expgram/NGram.hpp>
#include <expgram/Quantier.hpp>

typedef std::map<logprob_type, size_type, std::less<logprob_type>,
		 std::allocator<std::pair<const logprob_type, size_type> > > logprob_counts_type;
typedef std::map<logprob_type, quantized_type, std::less<logprob_type>,
		 std::allocator<std::pair<const logprob_type, quantized_type> > > codemap_type;

int main(int argc, cha** argv)
{
  
  try {
    ngram_type ngram(debug);
    
    ngram.open_shard(ngram_file, mpi_rank);
    
    quantize_ngram(ngram);
    
    dump_ngram(ngram, output_file);
  }
  catch (std::exception& err) {
    std::cerr << "error: "  << err.what() << std::endl;
    return 1;
  }
  return 0;
}

void dump_ngram(const ngram_type& ngram, const path_type& output_file)
{
  typedef utils::repository repository_type;

  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  if (mpi_rank == 0) {
    // first, create output direcories...
    repository_type repository(output, repository_type::write);
    repository_type rep_index(repository.path("index"), repository_type::write);
    repository_type rep_count(repository.path("count"), repository_type::write);
    
    std::ostringstream stream_shard;
    std::ostringstream stream_order;
    stream_shard << mpi_size;
    stream_order << ngram.index.order();
    rep_index["shard"] = stream_shard.str();
    rep_index["order"]
    rep_count["shard"] = stream_shard.str();
  }
  
}

template <typename OStream, typename LogProbs, typename Counts, typename Codemap, typename Codebook>
inline
void quantize(OStream& os, LogProbs& logprobs, Counts& counts, Codemap& codemap, Codebook& codebook, const int order)
{
  counts.clear();
  codemap.clear();
  
  const size_type pos_first = ngram.index[shard].offsets[order - 1];
  const size_type pos_last  = ngram.index[shard].offsets[order];
  
  for (size_type pos = pos_first; pos < pos_last; ++ pos)
    ++ counts[logprobs(pos, order)];
      
  Quantizer::quantize(counts, ngram.logprob_min(), codebook, codemap);
      
  for (size_type pos = pos_first; pos < pos_last; ++ pos) {
    codemap_type::const_iterator citer = codemap.find(logprobs(pos, order));
    if (citer == codemap.end())
      throw std::runtime_error("no codemap?");
	
    os.write((char*) &(citer->second), sizeof(quantized_type));
  }
}

void quantize_ngram(ngrm_type& ngram)
{
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
      quantize(os, ngram.logprobs[mpi_rank], counts, codemap, codebook, 1);
      ngram.logprobs[mpi_rank].maps.push_back(codebook);
    } else
      ngram.logprobs[mpi_rank].maps.push_back(codebook);
    
    for (int order = 2; order <= ngram.index.order(); ++ order) {
      quantize(os, ngram.logprobs[mpi_rank], counts, codemap, codebook, order);
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
      quantize(os, ngram.backoffs[mpi_rank], counts, codemap, codebook, 1);
      ngram.backoffs[mpi_rank].maps.push_back(codebook);
    } else
      ngram.backoffs[mpi_rank].maps.push_back(codebook);
	
    for (int order = 2; order < ngram.index.order(); ++ order) {
      quantize(os, ngram.backoffs[mpi_rank], counts, codemap, codebook, order);
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
      quantize(os, ngram.logbounds[mpi_rank], counts, codemap, codebook, 1);
      ngram.logbounds[mpi_rank].maps.push_back(codebook);
    } else
      ngram.logbounds[mpi_rank].maps.push_back(codebook);
	
    for (int order = 2; order < ngram.index.order(); ++ order) {
      quantize(os, ngram.logbounds[mpi_rank], counts, codemap, codebook, order);
      ngram.logbounds[mpi_rank].maps.push_back(codebook);
    }
    
    os.pop();
    
    utils::tempfile::permission(path);
    
    ngram.logbounds[mpi_rank].logprobs.clear();
    ngram.logbounds[mpi_rank].quantized.open(path);
  }
    
}
