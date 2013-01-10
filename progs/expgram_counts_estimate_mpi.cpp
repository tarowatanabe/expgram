//
//  Copyright(C) 2009-2012 Taro Watanabe <taro.watanabe@nict.go.jp>
//

#include <iostream>
#include <sstream>
#include <stdexcept>

#include <vector>
#include <queue>
#include <deque>

#include <expgram/NGramCounts.hpp>
#include <expgram/NGram.hpp>
#include <expgram/Discount.hpp>

#include <boost/thread.hpp>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/tokenizer.hpp>

#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>

#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filter/bzip2.hpp>

#include <boost/math/special_functions/expm1.hpp>

#include <utils/tempfile.hpp>
#include <utils/resource.hpp>
#include <utils/space_separator.hpp>
#include <utils/lexical_cast.hpp>
#include <utils/base64.hpp>

#include <utils/mpi.hpp>
#include <utils/mpi_device.hpp>
#include <utils/mpi_device_bcast.hpp>
#include <utils/mpi_stream.hpp>
#include <utils/mpi_stream_simple.hpp>
#include <utils/mpi_traits.hpp>
#include <utils/mathop.hpp>

#include <utils/lockfree_list_queue.hpp>

typedef expgram::NGram       ngram_type;
typedef expgram::NGramCounts ngram_counts_type;

typedef ngram_type::size_type       size_type;
typedef ngram_type::difference_type difference_type;
typedef ngram_type::path_type       path_type;

typedef ngram_type::count_type      count_type;
typedef ngram_type::logprob_type    logprob_type;
typedef ngram_type::prob_type       prob_type;
typedef ngram_type::vocab_type      vocab_type;
typedef ngram_type::word_type       word_type;
typedef ngram_type::id_type         id_type;

typedef expgram::Discount discount_type;
typedef std::vector<discount_type, std::allocator<discount_type> > discount_set_type;

struct shard_data_type
{
  typedef std::vector<logprob_type, std::allocator<logprob_type> > logprob_set_type;
  
  
  shard_data_type() : offset(0) {}
  shard_data_type(size_type logprobs_size, size_type backoffs_size)
    : logprobs(logprobs_size, ngram_type::logprob_min()),
      logbounds(backoffs_size, ngram_type::logprob_min()),
      backoffs(backoffs_size, 0.0),
      offset(0) {}
  
  logprob_set_type logprobs;
  logprob_set_type logbounds;
  logprob_set_type backoffs;
  logprob_type     smooth;
  size_type        offset;

  discount_set_type discounts;
  discount_set_type discounts_raw;
};

void estimate_discounts(const ngram_counts_type& ngram,
			shard_data_type& shard_data,
			const bool remove_unk);

void estimate_unigram(const ngram_counts_type& ngram,
		      shard_data_type& shard_data,
		      const bool remove_unk);

void estimate_bigram(const ngram_counts_type& ngram,
		     shard_data_type& shard_data,
		     const bool remove_unk);

void estimate_ngram(const ngram_counts_type& ngram,
		    shard_data_type& shard_data,
		    const bool remove_unk);

int getoptions(int argc, char** argv);

template <typename Path, typename Data>
inline
void dump_file(const Path& file, const Data& data)
{
  boost::iostreams::filtering_ostream os;
  os.push(boost::iostreams::file_sink(file.string(), std::ios_base::out | std::ios_base::trunc), 1024 * 1024);
  
  const int64_t file_size = sizeof(typename Data::value_type) * data.size();
  for (int64_t offset = 0; offset < file_size; offset += 1024 * 1024)
    os.write(((char*) &(*data.begin())) + offset, std::min(int64_t(1024 * 1024), file_size - offset));
}  

template <typename Path, typename Iterator>
void dump(const Path& file, Iterator first, Iterator last)
{
  typedef typename std::iterator_traits<Iterator>::value_type value_type;

  boost::iostreams::filtering_ostream os;
  os.push(boost::iostreams::file_sink(file.string(), std::ios_base::out | std::ios_base::trunc), 1024 * 1024);
  
  while (first != last) {
    const size_type write_size = std::min(size_type(1024 * 1024), size_type(last - first));
    os.write((char*) &(*first), write_size * sizeof(value_type));
    first += write_size;
  }
}

path_type ngram_file;
path_type output_file;
path_type temporary_dir = "";

path_type prog_name;

bool remove_unk = false;

int debug = 0;


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
    
    ngram_counts_type ngram_counts(debug);
    ngram_counts.open_shard(ngram_file, mpi_rank);
    
    if (static_cast<int>(ngram_counts.index.size()) != mpi_size)
      throw std::runtime_error("MPI universe size do not match with ngram shard size");
    
    if (! ngram_counts.is_modified())
      throw std::runtime_error("ngram counts is nod modified...");
    
    shard_data_type shard_data(ngram_counts.index[mpi_rank].size(), ngram_counts.index[mpi_rank].position_size());
        
    estimate_discounts(ngram_counts, shard_data, remove_unk);
    
    estimate_unigram(ngram_counts, shard_data, remove_unk);
    
    estimate_bigram(ngram_counts, shard_data, remove_unk);
    
    estimate_ngram(ngram_counts, shard_data, remove_unk);
    
    // final dump...
    ngram_type ngram;
    ngram.index = ngram_counts.index;
    
    ngram.logprobs.reserve(ngram.index.size());
    ngram.logbounds.reserve(ngram.index.size());
    ngram.backoffs.reserve(ngram.index.size());
    ngram.logprobs.resize(ngram.index.size());
    ngram.logbounds.resize(ngram.index.size());
    ngram.backoffs.resize(ngram.index.size());
    
    ngram.logprobs[mpi_rank].offset  = ngram_counts.types[mpi_rank].offset;
    ngram.logbounds[mpi_rank].offset = ngram_counts.types[mpi_rank].offset;
    ngram.backoffs[mpi_rank].offset  = ngram_counts.types[mpi_rank].offset;
    
    ngram.smooth = shard_data.smooth;
    
    const path_type tmp_dir       = utils::tempfile::tmp_dir();
    const path_type path_logprob  = utils::tempfile::file_name(tmp_dir / "expgram.logprob.XXXXXX");
    const path_type path_logbound = utils::tempfile::file_name(tmp_dir / "expgram.logbound.XXXXXX");
    const path_type path_backoff  = utils::tempfile::file_name(tmp_dir / "expgram.backoff.XXXXXX");
    
    utils::tempfile::insert(path_logprob);
    utils::tempfile::insert(path_logbound);
    utils::tempfile::insert(path_backoff);
    
    dump(path_logprob,  shard_data.logprobs.begin() + ngram.logprobs[mpi_rank].offset,   shard_data.logprobs.end());
    dump(path_logbound, shard_data.logbounds.begin() + ngram.logbounds[mpi_rank].offset, shard_data.logbounds.end());
    dump(path_backoff,  shard_data.backoffs.begin() + ngram.backoffs[mpi_rank].offset,   shard_data.backoffs.end());
    
    shard_data.logprobs.clear();
    shard_data.logbounds.clear();
    shard_data.backoffs.clear();
    shard_data_type::logprob_set_type(shard_data.logprobs).swap(shard_data.logprobs);
    shard_data_type::logprob_set_type(shard_data.logbounds).swap(shard_data.logbounds);
    shard_data_type::logprob_set_type(shard_data.backoffs).swap(shard_data.backoffs);
    
    ::sync();

    while (! ngram_type::shard_data_type::logprob_set_type::exists(path_logprob))
      boost::thread::yield();
    while (! ngram_type::shard_data_type::logprob_set_type::exists(path_logbound))
      boost::thread::yield();    
    while (! ngram_type::shard_data_type::logprob_set_type::exists(path_backoff))
      boost::thread::yield();
    
    utils::tempfile::permission(path_logprob);
    utils::tempfile::permission(path_logbound);
    utils::tempfile::permission(path_backoff);
    
    ngram.logprobs[mpi_rank].logprobs.open(path_logprob);
    ngram.logbounds[mpi_rank].logprobs.open(path_logbound);
    ngram.backoffs[mpi_rank].logprobs.open(path_backoff);
    
    if (mpi_rank == 0)
      ngram.write_prepare(output_file);
    
    MPI::COMM_WORLD.Barrier();
    ngram.write_shard(output_file, mpi_rank);
  }
  catch (std::exception& err) {
    std::cerr << "error: " << err.what() << std::endl;
    MPI::COMM_WORLD.Abort(1);
    return 1;
  }
  return 0;
}

enum {
  bigram_count_tag = 2000,
  bigram_logprob_tag,
  ngram_tag,
  logprob_tag,
};


template <typename Counts>
void merge_count_of_counts(Counts& counts)
{
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  Counts mapped(counts);
  
  for (int rank = 0; rank < mpi_size; ++ rank) {
    if (rank == mpi_rank) {
      boost::iostreams::filtering_ostream stream;
      stream.push(boost::iostreams::zlib_compressor());
      stream.push(utils::mpi_device_bcast_sink(rank, 1024 * 1024));
      
      for (size_t order = 1; order < mapped.size(); ++ order) {
	typename Counts::value_type::const_iterator citer_end = mapped[order].end();
	for (typename Counts::value_type::const_iterator citer = mapped[order].begin(); citer != citer_end; ++ citer)
	  stream << order << '\t' << citer->first << ' ' << citer->second << '\n';
      }
    } else {
      typedef std::vector<utils::piece, std::allocator<utils::piece> > tokens_type;
      typedef boost::tokenizer<utils::space_separator, utils::piece::const_iterator, utils::piece> tokenizer_type;
      
      boost::iostreams::filtering_istream stream;
      stream.push(boost::iostreams::zlib_decompressor());
      stream.push(utils::mpi_device_bcast_source(rank, 1024 * 1024));
      
      std::string line;
      tokens_type tokens;
      
      while (std::getline(stream, line)) {
	utils::piece line_piece(line);
	tokenizer_type tokenizer(line_piece);
	tokens.clear();
	tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
	
	if (tokens.size() != 3) continue;
	
	const int order = utils::lexical_cast<int>(tokens[0]);
	const count_type count = utils::lexical_cast<count_type>(tokens[1]);
	const count_type count_count = utils::lexical_cast<count_type>(tokens[2]);
	
	counts[order][count] += count_count;
      }
    }
  }
}

void estimate_discounts(const ngram_counts_type& ngram,
			shard_data_type& shard_data,
			const bool remove_unk)
{
  typedef std::map<count_type, count_type, std::less<count_type>, std::allocator<std::pair<const count_type, count_type> > > count_set_type;
  typedef std::vector<count_set_type, std::allocator<count_set_type> > count_map_type;
  
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();

  const id_type bos_id = ngram.index.vocab()[vocab_type::BOS];
  const id_type unk_id = ngram.index.vocab()[vocab_type::UNK];
  
  count_map_type count_of_counts(ngram.index.order() + 1);
  count_map_type count_of_counts_raw(ngram.index.order() + 1);
  
  for (int order = (mpi_rank == 0 ? 1 : 2); order <= ngram.index.order(); ++ order) {
    const size_type pos_first = ngram.index[mpi_rank].offsets[order - 1];
    const size_type pos_last  = ngram.index[mpi_rank].offsets[order];
    
    for (size_type pos = pos_first; pos != pos_last; ++ pos) {
      const count_type count = ngram.types[mpi_rank][pos];
      if (count && (order == 1 || ! remove_unk || ngram.index[mpi_rank][pos] != unk_id))
	++ count_of_counts[order][count];
    }
    
    for (size_type pos = pos_first; pos != pos_last; ++ pos) {
      const count_type count = ngram.counts[mpi_rank][pos];
      if (count && (order == 1 || ! remove_unk || ngram.index[mpi_rank][pos] != unk_id))
	++ count_of_counts_raw[order][count];
    }
  }

  merge_count_of_counts(count_of_counts);
  merge_count_of_counts(count_of_counts_raw);
  
  shard_data.discounts.clear();
  shard_data.discounts.reserve(ngram.index.order() + 1);
  shard_data.discounts.resize(ngram.index.order() + 1);

  shard_data.discounts_raw.clear();
  shard_data.discounts_raw.reserve(ngram.index.order() + 1);
  shard_data.discounts_raw.resize(ngram.index.order() + 1);
  
  for (int order = 1; order <= ngram.index.order(); ++ order) {
    shard_data.discounts[order].estimate(count_of_counts[order].begin(), count_of_counts[order].end());
    shard_data.discounts_raw[order].estimate(count_of_counts_raw[order].begin(), count_of_counts_raw[order].end());
  }

  
  if (debug && mpi_rank == 0) {
    for (int order = 1; order <= ngram.index.order(); ++ order)
      std::cerr << "type counts: order: " << order << ' ' << shard_data.discounts[order] << std::endl;
    for (int order = 1; order <= ngram.index.order(); ++ order)
      std::cerr << "raw counts:  order: " << order << ' ' << shard_data.discounts_raw[order] << std::endl;
  }
}


void estimate_unigram(const ngram_counts_type& ngram,
		      shard_data_type& shard_data,
		      const bool remove_unk)
{
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();
  
  const id_type bos_id = ngram.index.vocab()[vocab_type::BOS];
  const id_type unk_id = ngram.index.vocab()[vocab_type::UNK];
  
  const discount_set_type& discounts     = shard_data.discounts;
  const discount_set_type& discounts_raw = shard_data.discounts_raw;

  if (mpi_rank == 0) {
    if (debug)
      std::cerr << "order: "  << 1 << std::endl;
    
    shard_data.smooth = boost::numeric::bounds<logprob_type>::lowest();
    
    count_type total = 0;
    count_type observed = 0;
    count_type min2 = 0;
    count_type min3 = 0;
    count_type zero_events = 0;
    
    for (size_type pos = 0; pos < ngram.index[0].offsets[1]; ++ pos) {
      if (id_type(pos) == bos_id) continue;
      
      const count_type count = ngram.types[0][pos];
      
      // when remove-unk is enabled, we treat it as zero_events
      if (count == 0 || (remove_unk && id_type(pos) == unk_id)) {
	++ zero_events;
	continue;
      }
      
      total += count;
      ++ observed;
      min2 += (count >= discounts[1].mincount2);
      min3 += (count >= discounts[1].mincount3);
    }
  
    double logsum = 0.0;
    const prob_type uniform_distribution = 1.0 / observed;
  
    for (/**/; logsum >= 0.0; ++ total) {
      logsum = boost::numeric::bounds<double>::lowest();
	
      for (size_type pos = 0; pos < ngram.index[0].offsets[1]; ++ pos) {
	if (id_type(pos) == bos_id) continue;
      
	const count_type count = ngram.types[0][pos];
      
	if (count == 0 || (remove_unk && id_type(pos) == unk_id)) continue;
      
	const prob_type discount = discounts[1].discount(count, total, observed);
	const prob_type prob = (discount * count / total);
	const prob_type lower_order_weight = discounts[1].lower_order_weight(total, observed, min2, min3);
	const prob_type lower_order_prob = uniform_distribution;
	const prob_type prob_combined = prob + lower_order_weight * lower_order_prob;
	const logprob_type logprob = utils::mathop::log(prob_combined);
      
	shard_data.logprobs[pos] = logprob;
	if (id_type(pos) == unk_id)
	  shard_data.smooth = logprob;
	  
	logsum = utils::mathop::logsum(logsum, double(logprob));
      }
    }
    
    using namespace boost::math::policies;
    typedef policy<domain_error<errno_on_error>,
      pole_error<errno_on_error>,
      overflow_error<errno_on_error>,
      rounding_error<errno_on_error>,
      evaluation_error<errno_on_error>
      > policy_type;

    //const double discounted_mass =  1.0 - utils::mathop::exp(logsum);
    const double discounted_mass =  - boost::math::expm1(logsum, policy_type());
    if (discounted_mass > 0.0) {
      if (zero_events > 0) {
	// distribute probability mass to zero events...
	
	// if we set remove_unk, then zero events will be incremented when we actually observed UNK
	const double logdistribute = utils::mathop::log(discounted_mass) - utils::mathop::log(zero_events);
	for (size_type pos = 0; pos < ngram.index[0].offsets[1]; ++ pos)
	  if (id_type(pos) != bos_id && (ngram.types[0][pos] == 0 || (remove_unk && id_type(pos) == unk_id)))
	    shard_data.logprobs[pos] = logdistribute;
	if (shard_data.smooth == boost::numeric::bounds<logprob_type>::lowest())
	  shard_data.smooth = logdistribute;
      } else {
	for (size_type pos = 0; pos < ngram.index[0].offsets[1]; ++ pos)
	  if (id_type(pos) != bos_id) {
	    shard_data.logprobs[pos] -= logsum;
	    if (id_type(pos) == unk_id)
	      shard_data.smooth = shard_data.logprobs[pos];
	  }
      }
    }
    
    // bos is assigned -99 (base 10) from srilm
    shard_data.logprobs[bos_id] = expgram::NGram::logprob_bos();
    
    // fallback to uniform distribution...
    if (shard_data.smooth == boost::numeric::bounds<logprob_type>::lowest())
      shard_data.smooth = utils::mathop::log(uniform_distribution);
    
    if (debug)
      std::cerr << "\tsmooth: " << shard_data.smooth << std::endl;
    
    if (ngram.index.order() > 1) {
      count_type total = 0;
      count_type observed = 0;
      count_type min2 = 0;
      count_type min3 = 0;
      
      for (size_type pos = 0; pos < ngram.index[0].offsets[1]; ++ pos) {
	if (id_type(pos) == bos_id) continue;
	
	const count_type count = ngram.counts[0][pos];
	
	// when remove-unk is enabled, we treat it as zero_events
	if (count == 0 || (remove_unk && id_type(pos) == unk_id)) continue;
	
	total += count;
	++ observed;
	min2 += (count >= discounts_raw[1].mincount2);
	min3 += (count >= discounts_raw[1].mincount3);
      }
      
      if (total > 0)
	for (size_type pos = 0; pos < ngram.index[0].offsets[1]; ++ pos) {
	  if (id_type(pos) == bos_id) continue;
	  
	  const count_type count = ngram.counts[0][pos];
	  
	  if (count == 0 || (remove_unk && id_type(pos) == unk_id)) continue;
	  
	  const prob_type discount = discounts_raw[1].discount(count, total, observed);
	  const prob_type prob = (discount * count / total);
	  const prob_type lower_order_weight = discounts_raw[1].lower_order_weight(total, observed, min2, min3);
	  const prob_type lower_order_prob = uniform_distribution;
	  const prob_type prob_combined = prob + lower_order_weight * lower_order_prob;
	  const logprob_type logprob = utils::mathop::log(prob_combined);
	  
	  shard_data.logbounds[pos] = logprob;
	}
    }
  }
  
  // we will distribute unigram data across shards...
  MPI::COMM_WORLD.Bcast(&shard_data.smooth, 1, utils::mpi_traits<logprob_type>::data_type(), 0);
  MPI::COMM_WORLD.Bcast(&(*shard_data.logprobs.begin()), ngram.index[mpi_rank].offsets[1], utils::mpi_traits<logprob_type>::data_type(), 0);
  
  shard_data.offset = ngram.index[mpi_rank].offsets[1];
}

// bigram estimator..
// we can simply adapt the map-reduce framework...
// do we...?

template <typename Tp>
struct greater_pfirst
{
  bool operator()(const Tp* x, const Tp* y) const
  {
    return x->first > y->first;
  }
      
  bool operator()(const boost::shared_ptr<Tp>& x, const boost::shared_ptr<Tp>& y) const
  {
    return x->first > y->first;
  }
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

struct EstimateBigramMapReduce
{
  typedef std::vector<id_type, std::allocator<id_type> > context_type;
  
  struct count_pair_type
  {
    count_type count;
    count_type raw;
    
    count_pair_type(): count(0), raw(0) {}
    count_pair_type(const count_type& __count, const count_type& __raw)
      : count(__count), raw(__raw) {}
  };
  
  struct logprob_pair_type
  {
    logprob_type logprob;
    logprob_type logbound;
    
    logprob_pair_type(): logprob(0), logbound(0) {}
    logprob_pair_type(const logprob_type& __logprob)
      : logprob(__logprob), logbound(0) {}
    
    logprob_pair_type(const logprob_type& __logprob, const logprob_type& __logbound)
      : logprob(__logprob), logbound(__logbound) {}
  };
  
  typedef std::pair<id_type, count_pair_type>   word_count_type;
  typedef std::pair<id_type, logprob_pair_type> word_logprob_type;

  typedef std::pair<id_type, word_count_type>   context_count_type;
  typedef std::pair<id_type, word_logprob_type> context_logprob_type;

  typedef utils::lockfree_list_queue<context_count_type>   queue_count_type;
  typedef utils::lockfree_list_queue<context_logprob_type> queue_logprob_type;

  typedef boost::shared_ptr<queue_count_type>   queue_count_ptr_type;
  typedef boost::shared_ptr<queue_logprob_type> queue_logprob_ptr_type;

  typedef std::vector<queue_count_ptr_type, std::allocator<queue_count_ptr_type> >     queue_count_ptr_set_type;
  typedef std::vector<queue_logprob_ptr_type, std::allocator<queue_logprob_ptr_type> > queue_logprob_ptr_set_type;
  
  typedef boost::thread                  thread_type;
  typedef boost::shared_ptr<thread_type> thread_ptr_type;
};


struct EstimateBigramMapper
{
  // map bigram counts...

  typedef EstimateBigramMapReduce map_reduce_type;

  typedef map_reduce_type::count_pair_type   count_pair_type;
  typedef map_reduce_type::logprob_pair_type logprob_pair_type;
  
  typedef map_reduce_type::context_type         context_type;
  typedef map_reduce_type::context_count_type   context_count_type;
  typedef map_reduce_type::context_logprob_type context_logprob_type;
  
  typedef map_reduce_type::queue_count_type   queue_count_type;
  
  typedef map_reduce_type::queue_count_ptr_type   queue_count_ptr_type;
  
  typedef map_reduce_type::queue_count_ptr_set_type   queue_count_ptr_set_type;
  
  
  const ngram_counts_type&    ngram;
  queue_count_ptr_set_type&   queues;
  
  bool remove_unk;
  
  int mpi_rank;
  int mpi_size;

  EstimateBigramMapper(const ngram_counts_type&    _ngram,
		       queue_count_ptr_set_type&   _queues,
		       const bool _remove_unk,
		       const int _mpi_rank,
		       const int _mpi_size)
    : ngram(_ngram),
      queues(_queues),
      remove_unk(_remove_unk),
      mpi_rank(_mpi_rank), mpi_size(_mpi_size) {}
  
  void operator()()
  {
    // simple mapping...
    
    const int order = 2;
    const int order_prev = 1;
    
    const size_type pos_context_first = ngram.index[mpi_rank].offsets[order_prev - 1];
    const size_type pos_context_last  = ngram.index[mpi_rank].offsets[order_prev];

    const size_type pos_estimate_first = ngram.index[mpi_rank].offsets[order - 1];
    const size_type pos_estimate_last  = ngram.index[mpi_rank].offsets[order];

    double percent_next = 10.0;
      
    const id_type bos_id = ngram.index.vocab()[vocab_type::BOS];
    const id_type unk_id = ngram.index.vocab()[vocab_type::UNK];

    if (debug)
      std::cerr << "order: " << 2 << " shard: " << mpi_rank << std::endl;
    
    for (size_type pos_context = pos_context_first; pos_context < pos_context_last; ++ pos_context) {
      const size_type pos_first = ngram.index[mpi_rank].children_first(pos_context);
      const size_type pos_last  = ngram.index[mpi_rank].children_last(pos_context);
      
      const int shard_index = pos_context % mpi_size;
      
      for (size_type pos = pos_first; pos != pos_last; ++ pos) {
	const id_type id = ngram.index[mpi_rank][pos];
	
	if (remove_unk && id == unk_id) continue;
	
	queues[shard_index]->push(std::make_pair(pos_context, std::make_pair(id, count_pair_type(ngram.types[mpi_rank][pos],
												 ngram.counts[mpi_rank][pos]))));
      }
      
      const double percent_first(100.0 * (pos_first - pos_estimate_first) / (pos_estimate_last - pos_estimate_first));
      const double percent_last(100.0 *  (pos_last - pos_estimate_first) / (pos_estimate_last - pos_estimate_first));
      
      if (percent_first < percent_next && percent_next <= percent_last) {
	if (debug >= 2)
	  std::cerr << "rank: " << mpi_rank << " " << percent_next << "%" << std::endl;
	percent_next += 10;
      }
    }

    if (debug)
      std::cerr << "finished order: " << 2 << " shard: " << mpi_rank << std::endl;
    
    for (int shard_index = 0; shard_index < mpi_size; ++ shard_index)
      queues[shard_index]->push(std::make_pair(id_type(-1), std::make_pair(id_type(-1), count_pair_type())));
  }
};

struct EstimateBigramEstimator
{
  // collect bigram counts, estimate and emit

  typedef EstimateBigramMapReduce map_reduce_type;
  
  typedef map_reduce_type::count_pair_type   count_pair_type;
  typedef map_reduce_type::logprob_pair_type logprob_pair_type;
  
  typedef map_reduce_type::context_type         context_type;
  typedef map_reduce_type::context_count_type   context_count_type;
  typedef map_reduce_type::context_logprob_type context_logprob_type;

  typedef map_reduce_type::word_count_type   word_count_type;
  typedef map_reduce_type::word_logprob_type word_logprob_type;
  
  typedef map_reduce_type::queue_count_type   queue_count_type;
  typedef map_reduce_type::queue_logprob_type queue_logprob_type;

  typedef map_reduce_type::queue_count_ptr_type   queue_count_ptr_type;
  typedef map_reduce_type::queue_logprob_ptr_type queue_logprob_ptr_type;
  
  typedef map_reduce_type::queue_count_ptr_set_type   queue_count_ptr_set_type;
  typedef map_reduce_type::queue_logprob_ptr_set_type queue_logprob_ptr_set_type;
  
  const ngram_counts_type&    ngram;
  shard_data_type&            shard_data;
  queue_count_ptr_set_type&   queue_counts;
  queue_logprob_ptr_set_type& queue_logprobs;
  
  int mpi_rank;
  int mpi_size;

  EstimateBigramEstimator(const ngram_counts_type&    _ngram,
			  shard_data_type&            _shard_data,
			  queue_count_ptr_set_type&   _queue_counts,
			  queue_logprob_ptr_set_type& _queue_logprobs,
			  const int _mpi_rank,
			  const int _mpi_size)
    : ngram(_ngram), shard_data(_shard_data),
      queue_counts(_queue_counts), queue_logprobs(_queue_logprobs),
      mpi_rank(_mpi_rank), mpi_size(_mpi_size) {}
  
  typedef std::pair<id_type, logprob_type> id_logprob_type;
  typedef std::vector<id_logprob_type, std::allocator<id_logprob_type> > id_logprob_set_type;

  id_logprob_set_type logprobs;
  id_logprob_set_type logbounds;
  
  template <typename WordCountSet, typename WordLogProbSet>
  void estimate(const id_type& context, const WordCountSet& counts, WordLogProbSet& estimated)
  {
    const int order = 2;

    const bool estimate_bound = (ngram.index.order() > 2);
    
    const discount_set_type& discounts     = shard_data.discounts;
    const discount_set_type& discounts_raw = shard_data.discounts_raw;
    
    logprobs.clear();
    logbounds.clear();
    estimated.clear();
    
    if (ngram.index.order() > 2) {
      count_type total = 0;
      count_type observed = 0;
      count_type min2 = 0;
      count_type min3 = 0;
      
      typename WordCountSet::const_iterator citer_end = counts.end();
      for (typename WordCountSet::const_iterator citer = counts.begin(); citer != citer_end; ++ citer) {
	const id_type&    id = citer->first;
	const count_type& count = citer->second.raw;
	
	if (count == 0) continue;
	
	total += count;
	++ observed;
	min2 += (count >= discounts_raw[order].mincount2);
	min3 += (count >= discounts_raw[order].mincount3);
      }
      
      if (total > 0)
	for (typename WordCountSet::const_iterator citer = counts.begin(); citer != citer_end; ++ citer) {
	  const id_type&    id = citer->first;
	  const count_type& count = citer->second.raw;
	  
	  if (count == 0) continue;
	  
	  const prob_type discount = discounts_raw[order].discount(count, total, observed);
	  const prob_type prob = (discount * count / total);
	  
	  const prob_type lower_order_weight = discounts_raw[order].lower_order_weight(total, observed, min2, min3);
	  const logprob_type lower_order_logprob = shard_data.logprobs[id];
	  const prob_type lower_order_prob = utils::mathop::exp(double(lower_order_logprob));
	  const prob_type prob_combined = prob + lower_order_weight * lower_order_prob;
	  const logprob_type logprob = utils::mathop::log(prob_combined);
	  
	  logbounds.push_back(std::make_pair(id, logprob));
	}
    }
    
    count_type total = 0;
    count_type observed = 0;
    count_type min2 = 0;
    count_type min3 = 0;
    
    double logsum_lower_order = boost::numeric::bounds<double>::lowest();
    
    typename WordCountSet::const_iterator citer_end = counts.end();
    for (typename WordCountSet::const_iterator citer = counts.begin(); citer != citer_end; ++ citer) {
      const id_type&    id = citer->first;
      const count_type& count = citer->second.count;
      
      if (count == 0) continue;
      
      total += count;
      ++ observed;
      min2 += (count >= discounts[order].mincount2);
      min3 += (count >= discounts[order].mincount3);
      
      logsum_lower_order = utils::mathop::logsum(logsum_lower_order, double(shard_data.logprobs[id]));
    }
    
    if (total > 0) {
      double logsum = 0.0;
      for (/**/; logsum >= 0.0; ++ total) {
	logsum = boost::numeric::bounds<double>::lowest();
	
	logprobs.clear();
	
	typename WordCountSet::const_iterator citer_end = counts.end();
	for (typename WordCountSet::const_iterator citer = counts.begin(); citer != citer_end; ++ citer) {
	  const id_type&    id = citer->first;
	  const count_type& count = citer->second.count;
	  
	  if (count == 0) continue;
	  
	  const prob_type discount = discounts[order].discount(count, total, observed);
	  const prob_type prob = (discount * count / total);
	  
	  const prob_type lower_order_weight = discounts[order].lower_order_weight(total, observed, min2, min3);
	  const logprob_type lower_order_logprob = shard_data.logprobs[id];
	  const prob_type lower_order_prob = utils::mathop::exp(double(lower_order_logprob));
	  const prob_type prob_combined = prob + lower_order_weight * lower_order_prob;
	  const logprob_type logprob = utils::mathop::log(prob_combined);
	  
	  logsum = utils::mathop::logsum(logsum, double(logprob));
	  
	  logprobs.push_back(std::make_pair(id, logprob));
	}
      }
      
      using namespace boost::math::policies;
      typedef policy<domain_error<errno_on_error>,
		     pole_error<errno_on_error>,
		     overflow_error<errno_on_error>,
		     rounding_error<errno_on_error>,
		     evaluation_error<errno_on_error>
		     > policy_type;
      
      //const double numerator = 1.0 - utils::mathop::exp(logsum);
      //const double denominator = 1.0 - utils::mathop::exp(logsum_lower_order);
      
      const double numerator = - boost::math::expm1(logsum, policy_type());
      const double denominator =  - boost::math::expm1(logsum_lower_order, policy_type());
      
      if (numerator > 0.0) {
	if (denominator > 0.0)
	  shard_data.backoffs[context] = utils::mathop::log(numerator) - utils::mathop::log(denominator);
	else {
	  id_logprob_set_type::iterator liter_end = logprobs.end();
	  for (id_logprob_set_type::iterator liter = logprobs.begin(); liter != liter_end; ++ liter)
	    liter->second -= logsum;
	}
      }
    }
    
    if (! logprobs.empty() || ! logbounds.empty()) {
      // try merge two streams...
      
      id_logprob_set_type::const_iterator iter1 = logprobs.begin();
      id_logprob_set_type::const_iterator iter1_end = logprobs.end();
      id_logprob_set_type::const_iterator iter2 = logbounds.begin();
      id_logprob_set_type::const_iterator iter2_end = logbounds.end();
      
      while (iter1 != iter1_end && iter2 != iter2_end) {
	if (iter1->first < iter2->first) {
	  estimated.push_back(std::make_pair(iter1->first, logprob_pair_type(iter1->second, ngram_type::logprob_min())));
	  ++ iter1;
	} else if (iter2->first < iter1->first) {
	  estimated.push_back(std::make_pair(iter2->first, logprob_pair_type(ngram_type::logprob_min(), iter2->second)));
	  ++ iter2;
	} else {
	  estimated.push_back(std::make_pair(iter1->first, logprob_pair_type(iter1->second, iter2->second)));
	  ++ iter1;
	  ++ iter2;
	}
      }
      
      for (/**/; iter1 != iter1_end; ++ iter1)
	estimated.push_back(std::make_pair(iter1->first, logprob_pair_type(iter1->second, ngram_type::logprob_min())));
      
      for (/**/; iter2 != iter2_end; ++ iter2)
	estimated.push_back(std::make_pair(iter2->first, logprob_pair_type(ngram_type::logprob_min(), iter2->second)));
    }
  }
  
  void operator()()
  {
    // we will try to merge bigram counts wrt marginals (unigrams)...
    
    typedef std::pair<word_count_type, queue_count_ptr_type> word_count_queue_type;
    typedef std::pair<id_type, word_count_queue_type>        context_count_queue_type;
    typedef boost::shared_ptr<context_count_queue_type>      context_count_queue_ptr_type;
    typedef std::vector<context_count_queue_ptr_type, std::allocator<context_count_queue_ptr_type> > pqueue_base_type;
    typedef std::priority_queue<context_count_queue_ptr_type, pqueue_base_type, greater_pfirst<context_count_queue_type> > pqueue_type;

    typedef std::vector<word_count_type, std::allocator<word_count_type> > word_count_set_type;
    typedef std::vector<word_logprob_type, std::allocator<word_logprob_type> > word_logprob_set_type;
    
    pqueue_type pqueue;
    
    context_count_type context_count;
    for (int rank = 0; rank < mpi_size; ++ rank) {
      queue_counts[rank]->pop(context_count);
      if (context_count.first != id_type(-1))
	pqueue.push(context_count_queue_ptr_type(new context_count_queue_type(context_count.first,
									      std::make_pair(context_count.second, queue_counts[rank]))));
    }
    
    id_type               context(id_type(-1));
    word_count_set_type   counts;
    word_logprob_set_type logprobs;

    context_type bigram(2);
    while (! pqueue.empty()) {
      context_count_queue_ptr_type context_count_queue(pqueue.top());
      pqueue.pop();
      
      if (context != context_count_queue->first) {
	if (! counts.empty()) {
	  estimate(context, counts, logprobs);
	  
	  bigram[0] = context;
	  
	  word_logprob_set_type::const_iterator liter_end = logprobs.end();
	  for (word_logprob_set_type::const_iterator liter = logprobs.begin(); liter != liter_end; ++ liter) {
	    bigram[1] = liter->first;
	    queue_logprobs[ngram.index.shard_index(bigram.begin(), bigram.end())]->push(std::make_pair(context, *liter));
	  }
	}
	
	context = context_count_queue->first;
	counts.clear();
      }
      
      counts.push_back(context_count_queue->second.first);
      
      context_count_queue->second.second->pop(context_count);
      if (context_count.first != id_type(-1)) {
	context_count_queue->first        = context_count.first;
	context_count_queue->second.first = context_count.second;
	pqueue.push(context_count_queue);
      }
    }
    
    if (! counts.empty()) {
      estimate(context, counts, logprobs);
      
      bigram[0] = context;
      
      word_logprob_set_type::const_iterator liter_end = logprobs.end();
      for (word_logprob_set_type::const_iterator liter = logprobs.begin(); liter != liter_end; ++ liter) {
	bigram[1] = liter->first;
	queue_logprobs[ngram.index.shard_index(bigram.begin(), bigram.end())]->push(std::make_pair(context, *liter));
      }
    }
    
    // termination...
    for (int rank = 0; rank < mpi_size; ++ rank)
      queue_logprobs[rank]->push(std::make_pair(id_type(-1), std::make_pair(id_type(-1), ngram_type::logprob_min())));
  }
};

struct EstimateBigramReducer
{
  typedef EstimateBigramMapReduce map_reduce_type;
  
  typedef map_reduce_type::context_type         context_type;
  typedef map_reduce_type::context_logprob_type context_logprob_type;

  typedef map_reduce_type::queue_logprob_type queue_logprob_type;

  const ngram_counts_type&  ngram;
  shard_data_type&          shard_data;
  queue_logprob_type&       queue;
  
  int mpi_rank;
  int mpi_size;
  
  EstimateBigramReducer(const ngram_counts_type&  _ngram,
			shard_data_type&          _shard_data,
			queue_logprob_type&       _queue,
			const int _mpi_rank,
			const int _mpi_size)
    : ngram(_ngram), shard_data(_shard_data), queue(_queue),
      mpi_rank(_mpi_rank), mpi_size(_mpi_size) {}
  
  // reduce logprobs..
  void operator()()
  {
    int terminated = 0;
    
    context_type bigram(2);
    context_logprob_type context_logprob;

    const bool estimate_bound = (ngram.index.order() > 2);
    
    while (1) {
      queue.pop(context_logprob);
      
      if (context_logprob.first == id_type(-1)) {
	++ terminated;
	if (terminated >= mpi_size)
	  break;
	else
	  continue;
      }
      
      bigram[0] = context_logprob.first;
      bigram[1] = context_logprob.second.first;
      
      std::pair<context_type::const_iterator, size_type> result = ngram.index.traverse(mpi_rank, bigram.begin(), bigram.end());
      if (result.first != bigram.end() || result.second == size_type(-1))
	throw std::runtime_error("no ngram?");
      
      if (context_logprob.second.second.logprob != ngram_type::logprob_min()) 
	shard_data.logprobs[result.second]  = context_logprob.second.second.logprob;
      
      if (estimate_bound && context_logprob.second.second.logbound != ngram_type::logprob_min())
	shard_data.logbounds[result.second] = context_logprob.second.second.logbound;
    }
  }
};

void estimate_bigram(const ngram_counts_type& ngram,
		     shard_data_type& shard_data,
		     const bool remove_unk)
{
  typedef EstimateBigramMapReduce map_reduce_type;

  typedef EstimateBigramMapper    mapper_type;
  typedef EstimateBigramReducer   reducer_type;
  typedef EstimateBigramEstimator estimator_type;

  typedef map_reduce_type::count_pair_type   count_pair_type;
  typedef map_reduce_type::logprob_pair_type logprob_pair_type;
  
  typedef map_reduce_type::context_type         context_type;
  typedef map_reduce_type::context_count_type   context_count_type;
  typedef map_reduce_type::context_logprob_type context_logprob_type;
  
  typedef map_reduce_type::queue_count_type   queue_count_type;
  typedef map_reduce_type::queue_logprob_type queue_logprob_type;

  typedef map_reduce_type::queue_count_ptr_type   queue_count_ptr_type;
  typedef map_reduce_type::queue_logprob_ptr_type queue_logprob_ptr_type;
  
  typedef map_reduce_type::queue_count_ptr_set_type   queue_count_ptr_set_type;
  typedef map_reduce_type::queue_logprob_ptr_set_type queue_logprob_ptr_set_type;

  typedef map_reduce_type::thread_type     thread_type;
  typedef map_reduce_type::thread_ptr_type thread_ptr_type;

  //typedef utils::mpi_istream_simple istream_type;
  //typedef utils::mpi_ostream_simple ostream_type;

  typedef utils::mpi_istream istream_type;
  typedef utils::mpi_ostream ostream_type;
  
  typedef boost::shared_ptr<istream_type> istream_ptr_type;
  typedef boost::shared_ptr<ostream_type> ostream_ptr_type;
  
  typedef std::vector<istream_ptr_type, std::allocator<istream_ptr_type> > istream_ptr_set_type;
  typedef std::vector<ostream_ptr_type, std::allocator<ostream_ptr_type> > ostream_ptr_set_type;
  
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();

  istream_ptr_set_type istream_counts(mpi_size);
  ostream_ptr_set_type ostream_counts(mpi_size);
  
  istream_ptr_set_type istream_logprobs(mpi_size);
  ostream_ptr_set_type ostream_logprobs(mpi_size);
  
  queue_count_ptr_set_type   queue_counts_mapper(mpi_size);
  queue_count_ptr_set_type   queue_counts_reducer(mpi_size);
  queue_logprob_ptr_set_type queue_logprobs(mpi_size);
  
  for (int rank = 0; rank < mpi_size; ++ rank) {
    
    if (rank != mpi_rank) {
      istream_counts[rank].reset(new istream_type(rank, bigram_count_tag, 512));
      ostream_counts[rank].reset(new ostream_type(rank, bigram_count_tag, 512));
      
      istream_logprobs[rank].reset(new istream_type(rank, bigram_logprob_tag, 512));
      ostream_logprobs[rank].reset(new ostream_type(rank, bigram_logprob_tag, 512));
    }
    
    queue_counts_mapper[rank].reset(new queue_count_type());
    queue_counts_reducer[rank].reset(new queue_count_type());
    queue_logprobs[rank].reset(new queue_logprob_type());
  }
  
  queue_counts_mapper[mpi_rank] = queue_counts_reducer[mpi_rank];
  
  thread_ptr_type mapper(new thread_type(mapper_type(ngram, queue_counts_mapper, remove_unk, mpi_rank, mpi_size)));
  thread_ptr_type estimator(new thread_type(estimator_type(ngram, shard_data, queue_counts_reducer, queue_logprobs, mpi_rank, mpi_size)));
  thread_ptr_type reducer(new thread_type(reducer_type(ngram, shard_data, *queue_logprobs[mpi_rank], mpi_rank, mpi_size)));

  context_count_type   context_count;
  context_logprob_type context_logprob;
  std::string buffer;

  std::vector<bool, std::allocator<bool> > finished_counts(mpi_size, false);
  std::vector<bool, std::allocator<bool> > finished_logprobs(mpi_size, false);
  finished_counts[mpi_rank] = true;
  finished_logprobs[mpi_rank] = true;
  
  int non_found_iter = 0;
  while (1) {
    bool found = false;
    
    // send counts...
    for (int rank = 0; rank < mpi_size; ++ rank)
      if (rank != mpi_rank && ostream_counts[rank] && ostream_counts[rank]->test()) {
	buffer.clear();
	boost::iostreams::filtering_ostream stream;
	stream.push(boost::iostreams::back_inserter(buffer));
	
	context_count.first = 0;
	while (queue_counts_mapper[rank]->pop(context_count, true) && context_count.first != id_type(-1)) {
	  stream.write((char*) &context_count, sizeof(context_count_type));
	  context_count.first = 0;
	}
	stream.pop();
	
	if (! buffer.empty()) {
	  ostream_counts[rank]->write(buffer);
	  found = true;
	}
	if (context_count.first == id_type(-1)) {
	  finished_counts[rank] = true;
	  found = true;
	}
      }
    
    // send logprobs...
    for (int rank = 0; rank < mpi_size; ++ rank)
      if (rank != mpi_rank && ostream_logprobs[rank] && ostream_logprobs[rank]->test()) {
	buffer.clear();
	boost::iostreams::filtering_ostream stream;
	stream.push(boost::iostreams::back_inserter(buffer));
	
	context_logprob.first = 0;
	while (queue_logprobs[rank]->pop(context_logprob, true) && context_logprob.first != id_type(-1)) {
	  stream.write((char*) &context_logprob, sizeof(context_logprob_type));
	  context_logprob.first = 0;
	}
	stream.pop();
	
	if (! buffer.empty()) {
	  ostream_logprobs[rank]->write(buffer);
	  found = true;
	}
	
	if (context_logprob.first == id_type(-1)) {
	  finished_logprobs[rank] = true;
	  found = true;
	}
      }
    
    // terminate counts...
    for (int rank = 0; rank < mpi_size; ++ rank)
      if (rank != mpi_rank && ostream_counts[rank] && ostream_counts[rank]->test() && finished_counts[rank]) {
	if (! ostream_counts[rank]->terminated())
	  ostream_counts[rank]->terminate();
	else
	  ostream_counts[rank].reset();
	
	found = true;
      }
    
    // terminate logprobs...
    for (int rank = 0; rank < mpi_size; ++ rank)
      if (rank != mpi_rank && ostream_logprobs[rank] && ostream_logprobs[rank]->test() && finished_logprobs[rank]) {
	if (! ostream_logprobs[rank]->terminated())
	  ostream_logprobs[rank]->terminate();
	else
	  ostream_logprobs[rank].reset();
	
	found = true;
      }

    
    // receive counts...
    for (int rank = 0; rank < mpi_size; ++ rank)
      if (rank != mpi_rank && istream_counts[rank] && istream_counts[rank]->test()) {
	if (istream_counts[rank]->read(buffer)) {
	  boost::iostreams::filtering_istream stream;
	  stream.push(boost::iostreams::array_source(buffer.c_str(), buffer.size()));
	  
	  while (stream.read((char*) &context_count, sizeof(context_count_type)))
	    queue_counts_reducer[rank]->push(context_count);
	} else {
	  istream_counts[rank].reset();
	  queue_counts_reducer[rank]->push(context_count_type(id_type(-1), std::make_pair(id_type(-1), count_pair_type())));
	}
	
	found = true;
      }
    
    // receive logprobs...
    for (int rank = 0; rank < mpi_size; ++ rank)
      if (rank != mpi_rank && istream_logprobs[rank] && istream_logprobs[rank]->test()) {
	if (istream_logprobs[rank]->read(buffer)) {
	  boost::iostreams::filtering_istream stream;
	  stream.push(boost::iostreams::array_source(buffer.c_str(), buffer.size()));
	  
	  while (stream.read((char*) &context_logprob, sizeof(context_logprob_type)))
	    queue_logprobs[mpi_rank]->push(context_logprob);
	} else {
	  istream_logprobs[rank].reset();
	  queue_logprobs[mpi_rank]->push(context_logprob_type(id_type(-1), std::make_pair(id_type(-1), ngram_type::logprob_min())));
	}
	
	found = true;
      }
    
    if (std::count(ostream_counts.begin(), ostream_counts.end(), ostream_ptr_type()) == mpi_size
	&& std::count(ostream_logprobs.begin(), ostream_logprobs.end(), ostream_ptr_type()) == mpi_size
	&& std::count(istream_counts.begin(), istream_counts.end(), istream_ptr_type()) == mpi_size
	&& std::count(istream_logprobs.begin(), istream_logprobs.end(), istream_ptr_type()) == mpi_size) break;
    
    non_found_iter = loop_sleep(found, non_found_iter);
  }
  
  mapper->join();
  estimator->join();
  reducer->join();
  
  // now, bcast backoff parameters....
  const size_type unigram_size = ngram.index[mpi_rank].offsets[1];
  for (int rank = 0; rank < mpi_size; ++ rank) {
    if (rank == mpi_rank) {
      boost::iostreams::filtering_ostream stream;
      stream.push(boost::iostreams::zlib_compressor());
      stream.push(utils::mpi_device_bcast_sink(rank, 1024 * 1024));
      
      for (size_type pos = 0; pos < unigram_size; ++ pos) 
	if (static_cast<int>(pos % mpi_size) == rank)
	  stream.write((char*) &shard_data.backoffs[pos], sizeof(logprob_type));
    } else {
      boost::iostreams::filtering_istream stream;
      stream.push(boost::iostreams::zlib_decompressor());
      stream.push(utils::mpi_device_bcast_source(rank, 1024 * 1024));
      
      for (size_type pos = 0; pos < unigram_size; ++ pos) 
	if (static_cast<int>(pos % mpi_size) == rank)
	  stream.read((char*) &shard_data.backoffs[pos], sizeof(logprob_type));
    }
  }
}

template <typename Tp>
class Event
{
public:
  typedef Tp& reference;
  typedef const Tp& const_reference;
  
private:
  struct impl;
  
public:
  Event() : pimpl(new impl()) {}
  Event(const Tp& x) : pimpl(new impl(x)) {}
  
  operator reference() { return pimpl->value; }
  operator const_reference() const { return pimpl->value; }
  
  Event& operator=(const Tp& x) { pimpl->value = x; return *this; }
  Event& operator+=(const Tp& x) { pimpl->value += x; return *this; }
  Event& operator-=(const Tp& x) { pimpl->value -= x; return *this; }
  Event& operator*=(const Tp& x) { pimpl->value *= x; return *this; }
  Event& operator/=(const Tp& x) { pimpl->value /= x; return *this; }

  void swap(Event& x) { pimpl.swap(x.pimpl); }
  
  bool ready()
  {
    return utils::atomicop::fetch_and_add(pimpl->completed, int(0));
  }
  
  void wait()
  {
    for (;;) {
      for (int i = 0; i < 50; ++ i) {
	if (utils::atomicop::fetch_and_add(pimpl->completed, int(0)))
	  return;
	else
	  boost::thread::yield();
      }
      
      struct timespec tm;
      tm.tv_sec = 0;
      tm.tv_nsec = 2000001;
      nanosleep(&tm, NULL);
    }
  }
  
  void notify()
  {
    pimpl->completed = 1;
  }
  
private:
  struct impl
  {
    typedef boost::mutex              mutex_type;
    typedef boost::condition          condition_type;
    typedef boost::mutex::scoped_lock lock_type;
    
    Tp value;
    int completed;
    
    impl() : value(), completed(false) {}
    impl(const Tp& x) : value(x), completed(false) {}
  };
  
private:
  boost::shared_ptr<impl> pimpl;
};

namespace std
{
  template <typename Tp>
  inline
  void swap(Event<Tp>& x, Event<Tp>& y)
  {
    x.swap(y);
  }
};

struct EstimateNGramMapReduce
{
  
  typedef std::vector<id_type, std::allocator<id_type> > context_type;
  typedef Event<logprob_type>                            logprob_event_type;
  
  typedef std::pair<context_type, logprob_event_type>                          context_logprob_type;
  typedef std::vector<logprob_event_type, std::allocator<logprob_event_type> > logprob_event_set_type;

  typedef utils::lockfree_list_queue<context_logprob_type, std::allocator<context_logprob_type> >     queue_context_type;
  typedef utils::lockfree_list_queue<logprob_event_set_type, std::allocator<logprob_event_set_type> > queue_logprob_type;

  typedef boost::shared_ptr<queue_context_type> queue_context_ptr_type;
  typedef std::vector<queue_context_ptr_type, std::allocator<queue_context_ptr_type> > queue_context_ptr_set_type;
  
  typedef boost::thread                  thread_type;
  typedef boost::shared_ptr<thread_type> thread_ptr_type;
};

namespace std
{
  inline
  void swap(EstimateNGramMapReduce::context_logprob_type& x,
	    EstimateNGramMapReduce::context_logprob_type& y)
  {
    x.first.swap(y.first);
    x.second.swap(y.second);
  }
};

struct EstimateNGramMapper
{
  // send lower-order ngram scoring....

  typedef EstimateNGramMapReduce map_reduce_type;
  
  typedef map_reduce_type::logprob_event_type     logprob_event_type;
  typedef map_reduce_type::logprob_event_set_type logprob_event_set_type;
  typedef map_reduce_type::queue_logprob_type     queue_logprob_type;

  typedef map_reduce_type::context_type               context_type;
  typedef map_reduce_type::context_logprob_type       context_logprob_type;
  typedef map_reduce_type::queue_context_ptr_set_type queue_context_ptr_set_type;
  
  const ngram_counts_type&    ngram;
  queue_logprob_type&         queue;
  queue_context_ptr_set_type& queue_ngram;
  
  bool remove_unk;
  
  int mpi_rank;
  int mpi_size;

  EstimateNGramMapper(const ngram_counts_type&    _ngram,
		      queue_logprob_type&         _queue,
		      queue_context_ptr_set_type& _queue_ngram,
		      const bool _remove_unk,
		      const int _mpi_rank,
		      const int _mpi_size)
    : ngram(_ngram),
      queue(_queue), queue_ngram(_queue_ngram),
      remove_unk(_remove_unk),
      mpi_rank(_mpi_rank), mpi_size(_mpi_size) {}
  
  void operator()()
  {
    const int shard = mpi_rank;
    
    const id_type bos_id = ngram.index.vocab()[vocab_type::BOS];
    const id_type unk_id = ngram.index.vocab()[vocab_type::UNK];
    
    context_type           context;
    logprob_event_set_type lowers;
    
    for (int order_prev = 2; order_prev < ngram.index.order(); ++ order_prev) {
      const int order = order_prev + 1;
            
      const size_type pos_context_first = ngram.index[shard].offsets[order_prev - 1];
      const size_type pos_context_last  = ngram.index[shard].offsets[order_prev];
      
      context.resize(order);
      
      size_type pos_last_prev = pos_context_last;
      for (size_type pos_context = pos_context_first; pos_context < pos_context_last; ++ pos_context) {
	const size_type pos_first = pos_last_prev;
	const size_type pos_last = ngram.index[shard].children_last(pos_context);
	pos_last_prev = pos_last;
	
	if (pos_first == pos_last) continue;
	
	context_type::iterator citer_curr = context.end() - 2;
	for (size_type pos_curr = pos_context; pos_curr != size_type(-1); pos_curr = ngram.index[shard].parent(pos_curr), -- citer_curr)
	  *citer_curr = ngram.index[shard][pos_curr];
	
	lowers.resize(pos_last - pos_first);
	
	logprob_event_set_type::iterator liter = lowers.begin();
	for (size_type pos = pos_first; pos != pos_last; ++ pos, ++ liter) {
	  const id_type id = ngram.index[shard][pos];
	  
	  if (remove_unk && id == unk_id) continue;
	  
	  context.back() = id;
	  
	  const int shard_index = ngram.index.shard_index(context.begin() + 1, context.end());
	  
	  logprob_event_type logprob_lower(0.0);
	  
	  queue_ngram[shard_index]->push(context_logprob_type(context_type(context.begin() + 1, context.end()), logprob_lower));
	  
	  *liter = logprob_lower;
	}
	
	queue.push_swap(lowers);
	lowers.clear();
      }
    }
  }
};

struct EstimateNGramReducer
{
  // actually reduced, and estimate ngram probabilities...
  
  typedef EstimateNGramMapReduce map_reduce_type;
  
  typedef map_reduce_type::logprob_event_type     logprob_event_type;
  typedef map_reduce_type::logprob_event_set_type logprob_event_set_type;
  typedef map_reduce_type::queue_logprob_type     queue_logprob_type;
  
  typedef map_reduce_type::context_type               context_type;
  typedef map_reduce_type::context_logprob_type       context_logprob_type;
  typedef map_reduce_type::queue_context_ptr_set_type queue_context_ptr_set_type;
  
  const ngram_counts_type&    ngram;
  shard_data_type&            shard_data;
  queue_logprob_type&         queue;
  queue_context_ptr_set_type& queue_ngram;

  bool remove_unk;
  
  int mpi_rank;
  int mpi_size;

  EstimateNGramReducer(const ngram_counts_type&    _ngram,
		       shard_data_type&            _shard_data,
		       queue_logprob_type&         _queue,
		       queue_context_ptr_set_type& _queue_ngram,
		       const bool _remove_unk,
		       const int _mpi_rank,
		       const int _mpi_size)
    : ngram(_ngram), shard_data(_shard_data), 
      queue(_queue), queue_ngram(_queue_ngram),
      remove_unk(_remove_unk),
      mpi_rank(_mpi_rank), mpi_size(_mpi_size) {}

  void operator()()
  {
    const discount_set_type& discounts     = shard_data.discounts;
    const discount_set_type& discounts_raw = shard_data.discounts_raw;

    const int shard = mpi_rank;
    
    const id_type bos_id = ngram.index.vocab()[vocab_type::BOS];
    const id_type unk_id = ngram.index.vocab()[vocab_type::UNK];
    
    context_type           context;
    logprob_event_set_type lowers;
    
    for (int order_prev = 2; order_prev < ngram.index.order(); ++ order_prev) {
      const int order = order_prev + 1;
      
      if (debug)
	std::cerr << "order: " << order << " shard: " << shard << std::endl;
	
      const size_type pos_context_first = ngram.index[shard].offsets[order_prev - 1];
      const size_type pos_context_last  = ngram.index[shard].offsets[order_prev];

      const size_type pos_estimate_first = ngram.index[shard].offsets[order - 1];
      const size_type pos_estimate_last  = ngram.index[shard].offsets[order];
      
      double percent_next = 10.0;
      
      context.resize(order);
      
      size_type pos_last_prev = pos_context_last;
      for (size_type pos_context = pos_context_first; pos_context < pos_context_last; ++ pos_context) {
	const size_type pos_first = pos_last_prev;
	const size_type pos_last = ngram.index[shard].children_last(pos_context);
	pos_last_prev = pos_last;
	
	shard_data.offset = pos_first;
	
	if (pos_first == pos_last) continue;
	
	context_type::iterator citer_curr = context.end() - 2;
	for (size_type pos_curr = pos_context; pos_curr != size_type(-1); pos_curr = ngram.index[shard].parent(pos_curr), -- citer_curr)
	  *citer_curr = ngram.index[shard][pos_curr];
	
	lowers.clear();
	queue.pop_swap(lowers);
	
	// wait...
	{
	  logprob_event_set_type::iterator liter = lowers.begin();
	  
	  for (size_type pos = pos_first; pos != pos_last; ++ pos, ++ liter) {
	    if (remove_unk && ngram.index[shard][pos] == unk_id) continue;
	    
	    liter->wait();
	  }
	}
	
	// logbounds!
	if (ngram.index.order() > order) {
	  count_type total = 0;
	  count_type observed = 0;
	  count_type min2 = 0;
	  count_type min3 = 0;
	  
	  for (size_type pos = pos_first; pos != pos_last; ++ pos) {
	    if (remove_unk && ngram.index[shard][pos] == unk_id) continue;
	    
	    const count_type count = ngram.counts[shard][pos];
	    
	    if (count == 0) continue;
	    
	    total += count;
	    ++ observed;
	    min2 += (count >= discounts_raw[order].mincount2);
	    min3 += (count >= discounts_raw[order].mincount3);
	  }
	  
	  if (total > 0) {
	    logprob_event_set_type::const_iterator liter = lowers.begin();
	    
	    for (size_type pos = pos_first; pos != pos_last; ++ pos, ++ liter) {
	      if (remove_unk && ngram.index[shard][pos] == unk_id) continue;
	      
	      const count_type count = ngram.counts[shard][pos];
	      
	      if (count == 0) continue;
	      
	      const prob_type discount = discounts_raw[order].discount(count, total, observed);
	      const prob_type prob = (discount * count / total);
	      
	      const prob_type lower_order_weight = discounts_raw[order].lower_order_weight(total, observed, min2, min3);
	      const logprob_type lower_order_logprob = *liter;
	      const prob_type lower_order_prob = utils::mathop::exp(double(lower_order_logprob));
	      const prob_type prob_combined = prob + lower_order_weight * lower_order_prob;
	      const logprob_type logprob = utils::mathop::log(prob_combined);
	      
	      shard_data.logbounds[pos] = logprob;
	    }
	  }
	}
	
	count_type total = 0;
	count_type observed = 0;
	count_type min2 = 0;
	count_type min3 = 0;
	
	double logsum_lower_order = boost::numeric::bounds<double>::lowest();
	  
	logprob_event_set_type::const_iterator liter = lowers.begin();
	for (size_type pos = pos_first; pos != pos_last; ++ pos, ++ liter) {
	  if (remove_unk && ngram.index[shard][pos] == unk_id) continue;
	  
	  const count_type count = ngram.types[shard][pos];
	  
	  if (count == 0) continue;
	  
	  total += count;
	  ++ observed;
	  min2 += (count >= discounts[order].mincount2);
	  min3 += (count >= discounts[order].mincount3);
	  
	  const logprob_type logprob_lower_order = *liter;
	  logsum_lower_order = utils::mathop::logsum(logsum_lower_order, double(logprob_lower_order));
	}
	
	if (total == 0) continue;
	
	double logsum = 0.0;
	for (/**/; logsum >= 0.0; ++ total) {
	  logsum = boost::numeric::bounds<double>::lowest();
	  
	  logprob_event_set_type::const_iterator liter = lowers.begin();
	  for (size_type pos = pos_first; pos != pos_last; ++ pos, ++ liter) {
	    const count_type count = ngram.types[shard][pos];
	      
	    if (remove_unk && ngram.index[shard][pos] == unk_id) continue;
	    if (count == 0) continue;
	    
	    const prob_type discount = discounts[order].discount(count, total, observed);
	    const prob_type prob = (discount * count / total);
	      
	    const prob_type lower_order_weight = discounts[order].lower_order_weight(total, observed, min2, min3);
	    const logprob_type lower_order_logprob = *liter;
	    const prob_type lower_order_prob = utils::mathop::exp(double(lower_order_logprob));
	    const prob_type prob_combined = prob + lower_order_weight * lower_order_prob;
	    const logprob_type logprob = utils::mathop::log(prob_combined);
	    
	    logsum = utils::mathop::logsum(logsum, double(logprob));
	    shard_data.logprobs[pos] = logprob;
	  }
	}
	
	lowers.clear();
	
	using namespace boost::math::policies;
	typedef policy<domain_error<errno_on_error>,
	  pole_error<errno_on_error>,
	  overflow_error<errno_on_error>,
	  rounding_error<errno_on_error>,
	  evaluation_error<errno_on_error>
	  > policy_type;
	
	//const double numerator = 1.0 - utils::mathop::exp(logsum);
	//const double denominator = 1.0 - utils::mathop::exp(logsum_lower_order);
	
	const double numerator = - boost::math::expm1(logsum, policy_type());
	const double denominator =  - boost::math::expm1(logsum_lower_order, policy_type());
	
	if (numerator > 0.0) {
	  if (denominator > 0.0)
	    shard_data.backoffs[pos_context] = utils::mathop::log(numerator) - utils::mathop::log(denominator);
	  else {
	    for (size_type pos = pos_first; pos != pos_last; ++ pos) 
	      if (ngram.types[shard][pos] && ((! remove_unk) || (ngram.index[shard][pos] != unk_id)))
		shard_data.logprobs[pos] -= logsum;
	  }
	}
	
	// we have computed until pos_last!
	shard_data.offset = pos_last;
	
	const double percent_first(100.0 * (pos_first - pos_estimate_first) / (pos_estimate_last - pos_estimate_first));
	const double percent_last(100.0 *  (pos_last - pos_estimate_first) / (pos_estimate_last - pos_estimate_first));
	
	if (percent_first < percent_next && percent_next <= percent_last) {
	  if (debug >= 2)
	    std::cerr << "rank: " << mpi_rank << " " << percent_next << "%" << std::endl;
	  percent_next += 10;
	}
      }
      
      if (debug)
	std::cerr << "finished order: " << order << " shard: " << shard << std::endl;
    }
    
    // termination...
    for (int rank = 0; rank < mpi_size; ++ rank)
      queue_ngram[rank]->push(context_logprob_type(context_type(), logprob_event_type()));
  }
};

struct EstimateNGramServer
{
  // actual scoring for lower-order ngrams...
  // we will receive scoring query from mapper or shards...

  
  typedef EstimateNGramMapReduce map_reduce_type;
  
  typedef map_reduce_type::logprob_event_type     logprob_event_type;
  typedef map_reduce_type::logprob_event_set_type logprob_event_set_type;
  typedef map_reduce_type::queue_logprob_type     queue_logprob_type;

  typedef map_reduce_type::context_type               context_type;
  typedef map_reduce_type::context_logprob_type       context_logprob_type;
  typedef map_reduce_type::queue_context_ptr_set_type queue_context_ptr_set_type;
  
  const ngram_counts_type&    ngram;
  shard_data_type&            shard_data;
  
  queue_context_ptr_set_type& queue_logprob;
  queue_context_ptr_set_type& queue_ngram;
  
  int mpi_rank;
  int mpi_size;

  EstimateNGramServer(const ngram_counts_type&    _ngram,
		      shard_data_type&            _shard_data,
		      queue_context_ptr_set_type& _queue_logprob,
		      queue_context_ptr_set_type& _queue_ngram,
		      const int _mpi_rank,
		      const int _mpi_size)
    : ngram(_ngram), shard_data(_shard_data),
      queue_logprob(_queue_logprob), queue_ngram(_queue_ngram),
      mpi_rank(_mpi_rank), mpi_size(_mpi_size) {}

  template <typename PendingSet>
  void logprob(context_logprob_type& context_logprob, PendingSet& pendings)
  {
    context_type::const_iterator iter_begin = context_logprob.first.begin();
    context_type::const_iterator iter_end   = context_logprob.first.end();
    
    for (context_type::const_iterator iter = iter_begin; iter != iter_end; ++ iter) {
      const int shard_index = ngram.index.shard_index(iter, iter_end);
      
      if (shard_index == mpi_rank) {
	std::pair<context_type::const_iterator, size_type> result = ngram.index.traverse(shard_index, iter, iter_end);
	if (result.first == iter_end) {
	  
	  if (result.second >= utils::atomicop::fetch_and_add(shard_data.offset, size_type(0))) {
	    pendings.insert(std::make_pair(result.second, context_logprob_type(context_type(iter, iter_end), context_logprob.second)));
	    break;
	  }
	  
	  if (shard_data.logprobs[result.second] != ngram_type::logprob_min()) {
	    context_logprob.second += shard_data.logprobs[result.second];
	    context_logprob.second.notify();
	    break;
	  } else {
	    const size_type parent = ngram.index[shard_index].parent(result.second);
	    if (parent != size_type(-1))
	      context_logprob.second += shard_data.backoffs[parent];
	  }
	} else if (result.first == iter_end - 1)
	  context_logprob.second += shard_data.backoffs[result.second];
	
      } else {
	queue_ngram[shard_index]->push(context_logprob_type(context_type(iter, iter_end), context_logprob.second));
	break;
      }
    }
  }
  
  void operator()()
  {
    typedef std::multimap<size_type, context_logprob_type, std::less<size_type>, std::allocator<std::pair<const size_type, context_logprob_type> > > pending_set_type;
    
    context_logprob_type context_logprob;
    pending_set_type pendings;
    
    int finished = 0;
    
    int non_found_iter = 0;
    while (1) {
      bool found = false;
      
      for (int rank = 0; rank < mpi_size; ++ rank) 
	while (queue_logprob[rank]->pop_swap(context_logprob, true)) {
	  if (context_logprob.first.empty())
	    ++ finished;
	  else
	    logprob(context_logprob, pendings);

	  while (! pendings.empty() && pendings.begin()->first < utils::atomicop::fetch_and_add(shard_data.offset, size_type(0))) {
	    std::swap(context_logprob, pendings.begin()->second);
	    pendings.erase(pendings.begin());
	    logprob(context_logprob, pendings);
	  }
	  
	  found = true;
	}
      
      while (! pendings.empty() && pendings.begin()->first < utils::atomicop::fetch_and_add(shard_data.offset, size_type(0))) {
	std::swap(context_logprob, pendings.begin()->second);
	pendings.erase(pendings.begin());
	logprob(context_logprob, pendings);
	
	found = true;
      }
      
      if (pendings.empty() && finished >= mpi_size) break;
      
      non_found_iter = loop_sleep(found, non_found_iter);
    }
  }
};



void estimate_ngram(const ngram_counts_type& ngram,
		    shard_data_type& shard_data,
		    const bool remove_unk)
{
  //
  // lower-order logprobs are transfered between mapper/reducer
  // server will handle lower-order logprob-request, by sending/receiving logprob requests
  //
  
  typedef EstimateNGramMapReduce map_reduce_type;

  typedef EstimateNGramMapper  mapper_type;
  typedef EstimateNGramReducer reducer_type;
  typedef EstimateNGramServer  server_type;
  
  typedef map_reduce_type::logprob_event_type   logprob_event_type;
  typedef map_reduce_type::context_type         context_type;
  typedef map_reduce_type::context_logprob_type context_logprob_type;
  
  typedef map_reduce_type::queue_logprob_type         queue_logprob_type;
  typedef map_reduce_type::queue_context_type         queue_context_type;
  typedef map_reduce_type::queue_context_ptr_set_type queue_context_ptr_set_type;

  typedef map_reduce_type::thread_type     thread_type;
  typedef map_reduce_type::thread_ptr_type thread_ptr_type;
  
  typedef std::deque<logprob_event_type, std::allocator<logprob_event_type> > pending_set_type;
  typedef std::vector<pending_set_type, std::allocator<pending_set_type> >    pending_map_type;
  
  //typedef utils::mpi_istream_simple istream_type;
  //typedef utils::mpi_ostream_simple ostream_type;

  typedef utils::mpi_istream istream_type;
  typedef utils::mpi_ostream ostream_type;
  
  typedef boost::shared_ptr<istream_type> istream_ptr_type;
  typedef boost::shared_ptr<ostream_type> ostream_ptr_type;
  
  typedef std::vector<istream_ptr_type, std::allocator<istream_ptr_type> > istream_ptr_set_type;
  typedef std::vector<ostream_ptr_type, std::allocator<ostream_ptr_type> > ostream_ptr_set_type;
  
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();

  queue_logprob_type queue(256);
  
  istream_ptr_set_type istream_ngram(mpi_size);
  ostream_ptr_set_type ostream_ngram(mpi_size);
  
  istream_ptr_set_type istream_logprob(mpi_size);
  ostream_ptr_set_type ostream_logprob(mpi_size);
  
  queue_context_ptr_set_type queue_logprob(mpi_size);
  queue_context_ptr_set_type queue_ngram(mpi_size);
  
  pending_map_type pending_logprob(mpi_size);
  pending_map_type pending_ngram(mpi_size);

  for (int rank = 0; rank < mpi_size; ++ rank) {
    if (rank != mpi_rank) {
      istream_ngram[rank].reset(new istream_type(rank, ngram_tag, 512));
      ostream_ngram[rank].reset(new ostream_type(rank, ngram_tag, 512));
      
      istream_logprob[rank].reset(new istream_type(rank, logprob_tag, 512));
      ostream_logprob[rank].reset(new ostream_type(rank, logprob_tag, 512));
    }
    
    queue_logprob[rank].reset(new queue_context_type());
    queue_ngram[rank].reset(new queue_context_type());
  }
  
  // use the same queue!
  queue_logprob[mpi_rank] = queue_ngram[mpi_rank];
  
  shard_data.offset = ngram.index[mpi_rank].offsets[2];

  thread_ptr_type mapper(new thread_type(mapper_type(ngram, queue, queue_ngram, remove_unk, mpi_rank, mpi_size)));
  thread_ptr_type reducer(new thread_type(reducer_type(ngram, shard_data, queue, queue_ngram, remove_unk, mpi_rank, mpi_size)));
  thread_ptr_type server(new thread_type(server_type(ngram, shard_data, queue_logprob, queue_ngram, mpi_rank, mpi_size)));
  
  context_logprob_type context_logprob;
  std::string buffer;

  int terminated_recv = 0;
  int terminated_send = 0;

  namespace qi    = boost::spirit::qi;
  namespace karma = boost::spirit::karma;
  namespace standard = boost::spirit::standard;
  
  qi::uint_parser<id_type>       id_parser;
  karma::uint_generator<id_type> id_generator;
  
  int non_found_iter = 0;
  while (1) {
    bool found = false;

    // receive ngram request... mpi_rank will receive requests from mapper, not istream_ngram...
    for (int rank = 0; rank < mpi_size; ++ rank)
      if (rank != mpi_rank && istream_ngram[rank] && istream_ngram[rank]->test()) {
	buffer.clear();
	if (istream_ngram[rank]->read(buffer)) {
	  std::string::const_iterator iter = buffer.begin();
	  std::string::const_iterator iter_end = buffer.end();
	  
	  while (iter != iter_end) {
	    context_logprob.first.clear();
	    
	    if (! qi::phrase_parse(iter, iter_end, *id_parser >> qi::eol, standard::blank, context_logprob.first))
	      throw std::runtime_error("failed parsing id-ngram");
	    
	    context_logprob.second = logprob_event_type(0.0);
	    
	    if (! context_logprob.first.empty())
	      pending_logprob[rank].push_back(context_logprob.second);
	    else
	      ++ terminated_recv;
	    
	    queue_logprob[rank]->push_swap(context_logprob);
	  }
	} else
	  istream_ngram[rank].reset();
	
	found = true;
      }
    
    // send back the ngram results... (send logprob...)
    for (int rank = 0; rank < mpi_size; ++ rank)
      if (rank != mpi_rank && ostream_logprob[rank] && ostream_logprob[rank]->test()) {
	buffer.clear();
	boost::iostreams::filtering_ostream stream;
	stream.push(boost::iostreams::back_inserter(buffer));
	
	while (! pending_logprob[rank].empty() && pending_logprob[rank].front().ready()) {
	  stream.write((char*) &static_cast<const logprob_type&>(pending_logprob[rank].front()), sizeof(logprob_type));
	  pending_logprob[rank].pop_front();
	}
	
	stream.pop();
	if (! buffer.empty()) {
          ostream_logprob[rank]->write(buffer);
          found = true;
        }
      }
    
    // send ngram request... we will write ngram...
    for (int rank = 0; rank < mpi_size; ++ rank)
      if (rank != mpi_rank && ostream_ngram[rank] && ostream_ngram[rank]->test()) {
	buffer.clear();
	std::back_insert_iterator<std::string> iter(buffer);
	
	while (queue_ngram[rank]->pop_swap(context_logprob, true)) {
	  if (! karma::generate(iter, -(id_generator % ' ') << '\n', context_logprob.first))
	    throw std::runtime_error("failed generating id-ngram");
	  
	  if (! context_logprob.first.empty())
	    pending_ngram[rank].push_back(context_logprob.second);
	  else
	    ++ terminated_send;
	}
	
	if (! buffer.empty()) {
          ostream_ngram[rank]->write(buffer);
          found = true;
        }
      }
    
    // receive ngram results..
    for (int rank = 0; rank < mpi_size; ++ rank)
      if (rank != mpi_rank && istream_logprob[rank] && istream_logprob[rank]->test()) {
	
	if (istream_logprob[rank]->read(buffer)) {
	  boost::iostreams::filtering_istream stream;
	  stream.push(boost::iostreams::array_source(buffer.c_str(), buffer.size()));
	  
	  logprob_type logprob;
	  while (stream.read((char*) &logprob, sizeof(logprob_type))) {
	    pending_ngram[rank].front() += logprob;
	    pending_ngram[rank].front().notify();
	    pending_ngram[rank].pop_front();
	  }
	} else
	  istream_logprob[rank].reset();
	
	found = true;
      }

    // terminated_send >= mpi_size - 1 implies that the shard mpi_rank finished estimation
    // terminated_recv >= mpi_size - 1 implies that other shards finished estimation
    if (terminated_recv >= mpi_size - 1 && terminated_send >= mpi_size - 1) {
      
      // terminate ostreams...
      for (int rank = 0; rank < mpi_size; ++ rank)
	if (ostream_ngram[rank] && ostream_ngram[rank]->test()) {
	  if (! ostream_ngram[rank]->terminated())
	    ostream_ngram[rank]->terminate();
	  else
	    ostream_ngram[rank].reset();
	  
	  found = true;
	}
      
      for (int rank = 0; rank < mpi_size; ++ rank)
	if (ostream_logprob[rank] && ostream_logprob[rank]->test()) {
	  if (! ostream_logprob[rank]->terminated())
	    ostream_logprob[rank]->terminate();
	  else
	    ostream_logprob[rank].reset();
	  
	  found = true;
	}
      
      // termination...
      if (std::count(istream_ngram.begin(), istream_ngram.end(), istream_ptr_type()) == mpi_size
	  && std::count(ostream_ngram.begin(), ostream_ngram.end(), ostream_ptr_type()) == mpi_size
	  && std::count(istream_logprob.begin(), istream_logprob.end(), istream_ptr_type()) == mpi_size
	  && std::count(ostream_logprob.begin(), ostream_logprob.end(), ostream_ptr_type()) == mpi_size) break;
    }
    
    non_found_iter = loop_sleep(found, non_found_iter);
  }
  
  mapper->join();
  reducer->join();
  server->join();
}


int getoptions(int argc, char** argv)
{
  const int mpi_rank = MPI::COMM_WORLD.Get_rank();
  const int mpi_size = MPI::COMM_WORLD.Get_size();

  namespace po = boost::program_options;
  
  po::options_description desc("options");
  desc.add_options()
    ("ngram",     po::value<path_type>(&ngram_file)->default_value(ngram_file),   "ngram counts in binary format")
    ("output",    po::value<path_type>(&output_file)->default_value(output_file), "output in binary format")
    ("temporary", po::value<path_type>(&temporary_dir),                           "temporary directory")

    ("prog",   po::value<path_type>(&prog_name),   "this binary")
    
    ("remove-unk", po::bool_switch(&remove_unk),   "remove UNK when estimating language model")
    
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
