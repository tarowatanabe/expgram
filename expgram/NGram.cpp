
#include <fstream>
#include <queue>

#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/tokenizer.hpp>

#include <utils/lockfree_queue.hpp>
#include <utils/lockfree_list_queue.hpp>
#include <utils/compress_stream.hpp>
#include <utils/space_separator.hpp>
#include <utils/tempfile.hpp>
#include <utils/packed_device.hpp>

#include "NGram.hpp"
#include "Quantizer.hpp"

namespace expgram
{
 
  inline bool true_false(const std::string& optarg)
  {
    if (strcasecmp(optarg.c_str(), "true") == 0)
      return true;
    if (strcasecmp(optarg.c_str(), "yes") == 0)
      return true;
    if (atoi(optarg.c_str()) > 0)
      return true;
    return false;
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

  void NGram::ShardData::open(const path_type& path)
  {
    typedef utils::repository repository_type;
    
    clear();
    
    repository_type rep(path, repository_type::read);
    
    repository_type::const_iterator oiter = rep.find("offset");
    if (oiter == rep.end())
      throw std::runtime_error("no offset?");
    offset = atoll(oiter->second.c_str());
    
    if (boost::filesystem::exists(rep.path("quantized"))) {
      quantized.open(rep.path("quantized"));
      
      logprob_map_type logprob_map;
      
      maps.clear();
      maps.push_back(logprob_map);
      for (int n = 1; /**/; ++ n) {
	std::ostringstream stream_map_file;
	stream_map_file << n << "-logprob-map";
	
	if (! boost::filesystem::exists(rep.path(stream_map_file.str()))) break;
	
	std::ifstream is(rep.path(stream_map_file.str()).file_string().c_str());
	is.read((char*) &(*logprob_map.begin()), sizeof(logprob_type) * logprob_map.size());
	maps.push_back(logprob_map);
      }
    } else
      logprobs.open(rep.path("logprob"));
  }
  
  void NGram::ShardData::write(const path_type& file) const
  {
    typedef utils::repository repository_type;
    
    if (path() == file) return;
    
    repository_type rep(file, repository_type::write);
    
    if (quantized.is_open()) {
      quantized.write(rep.path("quantized"));
      
      for (int n = 1; n < maps.size(); ++ n) {
	std::ostringstream stream_map_file;
	stream_map_file << n << "-logprob-map";
	
	std::ofstream os(rep.path(stream_map_file.str()).file_string().c_str());
	os.write((char*) &(*maps[n].begin()), sizeof(logprob_type) * maps[n].size());
      }
    }

    if (logprobs.is_open())
      logprobs.write(rep.path("logprob"));
    
    std::ostringstream stream_offset;
    stream_offset << offset;
    rep["offset"] = stream_offset.str();
  }
  
  template <typename Path, typename Shards>
  inline
  void open_shards(const Path& path, Shards& shards, int shard)
  {
    typedef utils::repository repository_type;
    
    repository_type rep(path, repository_type::read);
    
    repository_type::const_iterator siter = rep.find("shard");
    if (siter == rep.end())
      throw std::runtime_error("no shard size...");
    
    shards.clear();
    shards.reserve(atoi(siter->second.c_str()));
    shards.resize(atoi(siter->second.c_str()));
    
    if (shard >= shards.size())
      throw std::runtime_error("shard is out of range");

    std::ostringstream stream_shard;
    stream_shard << "ngram-" << std::setfill('0') << std::setw(6) << shard;
    
    shards[shard].open(rep.path(stream_shard.str()));
  }

  template <typename Path, typename Shards>
  inline
  void open_shards(const Path& path, Shards& shards)
  {
    typedef utils::repository repository_type;
    
    repository_type rep(path, repository_type::read);
    
    repository_type::const_iterator siter = rep.find("shard");
    if (siter == rep.end())
      throw std::runtime_error("no shard size...");

    shards.clear();
    shards.reserve(atoi(siter->second.c_str()));
    shards.resize(atoi(siter->second.c_str()));
    
    for (int shard = 0; shard < shards.size(); ++ shard) {
      std::ostringstream stream_shard;
      stream_shard << "ngram-" << std::setfill('0') << std::setw(6) << shard;
      
      shards[shard].open(rep.path(stream_shard.str()));
    }
  }

  template <typename Path, typename Shard>
  struct TaskWriter
  {
    typedef boost::thread                                                  thread_type;
    typedef boost::shared_ptr<thread_type>                                 thread_ptr_type;
    typedef std::vector<thread_ptr_type, std::allocator<thread_ptr_type> > thread_ptr_set_type;

    Path path;
    const Shard& shard;
    
    TaskWriter(const Path& _path,
	       const Shard& _shard)
      : path(_path),
	shard(_shard) {}
    
    void operator()()
    {
      shard.write(path);
    }
  };

  
  template <typename Path, typename Shards>
  inline
  void write_shards(const Path& path, const Shards& shards)
  {
    typedef utils::repository repository_type;
    
    typedef TaskWriter<Path, typename Shards::value_type>    task_type;
    typedef typename task_type::thread_type         thread_type;
    typedef typename task_type::thread_ptr_set_type thread_ptr_set_type;
    
    {
      repository_type rep(path, repository_type::write);
      
      std::ostringstream stream_shard;
      stream_shard << shards.size();
      rep["shard"] = stream_shard.str();
    }
    
    thread_ptr_set_type threads(shards.size());
    
    {
      for (int shard = 1; shard < shards.size(); ++ shard) {
	std::ostringstream stream_shard;
	stream_shard << "ngram-" << std::setfill('0') << std::setw(6) << shard;
	
	threads[shard].reset(new thread_type(task_type(path / stream_shard.str(), shards[shard])));
      }
      
      // dump for root...
      std::ostringstream stream_shard;
      stream_shard << "ngram-" << std::setfill('0') << std::setw(6) << 0;
      task_type(path / stream_shard.str(), shards[0])();
      
      // terminate...
      for (int shard = 1; shard < shards.size(); ++ shard)
	threads[shard]->join();
    }
    
    threads.clear();
  }
  
  template <typename Path, typename Shards>
  inline
  void write_shards(const Path& path, const Shards& shards, int shard)
  {
    typedef utils::repository repository_type;
    
    while (! boost::filesystem::exists(path))
      boost::thread::yield();
    
    repository_type rep(path, repository_type::read);
    
    std::ostringstream stream_shard;
    stream_shard << "ngram-" << std::setfill('0') << std::setw(6) << shard;
    
    shards[shard].write(rep.path(stream_shard.str()));
  }
  
  template <typename Path, typename Shards>
  inline
  void write_shards_prepare(const Path& path, const Shards& shards)
  {
    typedef utils::repository repository_type;
    
    repository_type rep(path, repository_type::write);
    
    std::ostringstream stream_shard;
    stream_shard << shards.size();
    rep["shard"] = stream_shard.str();
  }


  void NGram::write_prepare(const path_type& file) const
  {
    typedef utils::repository repository_type;
    
    if (path() == file) return;
    
    repository_type rep(file, repository_type::write);
    
    index.write_prepare(rep.path("index"));
    
    if (! logprobs.empty())
      write_shards_prepare(rep.path("logprob"), logprobs);
    if (! backoffs.empty())
      write_shards_prepare(rep.path("backoff"), backoffs);
    if (! logbounds.empty())
      write_shards_prepare(rep.path("logbound"), logbounds);
    
    std::ostringstream stream_smooth;
    stream_smooth.precision(20);
    stream_smooth << smooth;
    rep["smooth"] = stream_smooth.str();
  }

  void NGram::write_shard(const path_type& file, int shard) const
  {
    typedef utils::repository repository_type;
    
    if (path() == file) return;
    
    while (! boost::filesystem::exists(file))
      boost::thread::yield();
    
    repository_type rep(file, repository_type::read);
    
    index.write_shard(rep.path("index"), shard);
    if (! logprobs.empty())
      write_shards(rep.path("logprob"), logprobs, shard);
    if (! backoffs.empty())
      write_shards(rep.path("backoff"), backoffs, shard);
    if (! logbounds.empty())
      write_shards(rep.path("logbound"), logbounds, shard);
  }
  
  // write in binary format
  void NGram::write(const path_type& file) const
  {
    typedef utils::repository repository_type;
    
    if (path() == file) return;
    
    repository_type rep(file, repository_type::write);
    
    index.write(rep.path("index"));
    if (! logprobs.empty())
      write_shards(rep.path("logprob"), logprobs);
    if (! backoffs.empty())
      write_shards(rep.path("backoff"), backoffs);
    if (! logbounds.empty())
      write_shards(rep.path("logbound"), logbounds);
    
    std::ostringstream stream_smooth;
    stream_smooth.precision(20);
    stream_smooth << smooth;
    rep["smooth"] = stream_smooth.str();
  }
  
  void NGram::open(const path_type& path, const size_type shard_size)
  {
    clear();

    if (boost::filesystem::is_directory(path))
      open_binary(path);
    else
      open_arpa(path, shard_size);
  }
  
  
  void NGram::open_shard(const path_type& path, int shard)
  {
    typedef utils::repository repository_type;
    
    clear();
    
    repository_type rep(path, repository_type::read);
    
    index.open_shard(rep.path("index"), shard);
    if (boost::filesystem::exists(rep.path("logprob")))
      open_shards(rep.path("logprob"), logprobs, shard);
    if (boost::filesystem::exists(rep.path("backoff")))
      open_shards(rep.path("backoff"), backoffs, shard);
    if (boost::filesystem::exists(rep.path("logbound")))
      open_shards(rep.path("logbound"), logbounds, shard);
    
    repository_type::const_iterator siter = rep.find("smooth");
    if (siter == rep.end())
      throw std::runtime_error("no smoothing parameter...?");
    smooth = atof(siter->second.c_str());
  }
  
  
  void NGram::open_binary(const path_type& path)
  {
    typedef utils::repository repository_type;
    
    clear();
    
    repository_type rep(path, repository_type::read);
    
    index.open(rep.path("index"));
    if (boost::filesystem::exists(rep.path("logprob")))
      open_shards(rep.path("logprob"), logprobs);
    if (boost::filesystem::exists(rep.path("backoff")))
      open_shards(rep.path("backoff"), backoffs);
    if (boost::filesystem::exists(rep.path("logbound")))
      open_shards(rep.path("logbound"), logbounds);
    
    repository_type::const_iterator siter = rep.find("smooth");
    if (siter == rep.end())
      throw std::runtime_error("no smoothing parameter...?");
    smooth = atof(siter->second.c_str());
    
    if (debug)
      std::cerr << "# of shards: " << index.size()
		<< " smooth: " << smooth
		<< std::endl;
  }
  
  // quantization...
  
  struct NGramQuantizeMapReduce
  {
    typedef boost::thread                                                  thread_type;
    typedef boost::shared_ptr<thread_type>                                 thread_ptr_type;
    typedef std::vector<thread_ptr_type, std::allocator<thread_ptr_type> > thread_ptr_set_type;

    typedef expgram::NGram ngram_type;
    
    typedef ngram_type::logprob_type    logprob_type;
    typedef ngram_type::quantized_type  quantized_type;
    typedef ngram_type::size_type       size_type;
    typedef ngram_type::difference_type difference_type;
    typedef ngram_type::shard_data_type shard_data_type;
    
    typedef ngram_type::path_type       path_type;
    
    typedef shard_data_type::logprob_map_type logprob_map_type;
  };
  
  
  struct NGramQuantizeReducer
  {
    typedef NGramQuantizeMapReduce map_reduce_type;
    
    typedef map_reduce_type::ngram_type       ngram_type;
    
    typedef map_reduce_type::size_type        size_type;
    typedef map_reduce_type::logprob_type     logprob_type;
    typedef map_reduce_type::quantized_type   quantized_type;
    typedef map_reduce_type::path_type        path_type;
    
    typedef map_reduce_type::logprob_map_type logprob_map_type;
    
    typedef std::map<logprob_type, size_type, std::less<logprob_type>,
		     std::allocator<std::pair<const logprob_type, size_type> > > logprob_counts_type;
    typedef std::map<logprob_type, quantized_type, std::less<logprob_type>,
		     std::allocator<std::pair<const logprob_type, quantized_type> > > codemap_type;
    
    ngram_type& ngram;
    int         shard;
    int         debug;
    
    NGramQuantizeReducer(ngram_type& _ngram,
			 const int   _shard,
			 const int   _debug)
      : ngram(_ngram),
	shard(_shard),
	debug(_debug)
    {}

    template <typename OStream, typename LogProbs, typename Counts, typename Codemap, typename Codebook>
    void quantize(OStream& os, LogProbs& logprobs, Counts& counts, Codemap& codemap, Codebook& codebook, const int order)
    {
      counts.clear();
      codemap.clear();
      
      const size_type pos_first = ngram.index[shard].offsets[order - 1];
      const size_type pos_last  = ngram.index[shard].offsets[order];
      
      for (size_type pos = pos_first; pos < pos_last; ++ pos)
	++ counts[logprobs(pos, order)];
      
      Quantizer::quantize(counts, ngram.logprob_min(), codebook, codemap, debug >= 2);
      
      for (size_type pos = pos_first; pos < pos_last; ++ pos) {
	codemap_type::const_iterator citer = codemap.find(logprobs(pos, order));
	if (citer == codemap.end())
	  throw std::runtime_error("no codemap?");
	
	os.write((char*) &(citer->second), sizeof(quantized_type));
      }
    }
    
    void operator()()
    {
      const path_type tmp_dir = utils::tempfile::tmp_dir();
      
      logprob_counts_type counts;
      codemap_type        codemap;
      logprob_map_type    codebook;

      if (! ngram.logprobs[shard].quantized.is_open() && ngram.logprobs[shard].logprobs.is_open()) {
	
	if (debug)
	  std::cerr << "shard: " << shard << " quantize logprob" << std::endl;

	const path_type path = utils::tempfile::directory_name(tmp_dir / "expgram.logprob.quantized.XXXXXX");
	utils::tempfile::insert(path);
	
	boost::iostreams::filtering_ostream os;
	os.push(utils::packed_sink<quantized_type, std::allocator<quantized_type> >(path));
	
	std::fill(codebook.begin(), codebook.end(), logprob_type(0.0));
	
	ngram.logprobs[shard].maps.clear();
	ngram.logprobs[shard].maps.push_back(codebook);
	if (shard == 0) {
	  quantize(os, ngram.logprobs[shard], counts, codemap, codebook, 1);
	  ngram.logprobs[shard].maps.push_back(codebook);
	} else
	  ngram.logprobs[shard].maps.push_back(codebook);
	
	for (int order = 2; order <= ngram.index.order(); ++ order) {
	  quantize(os, ngram.logprobs[shard], counts, codemap, codebook, order);
	  ngram.logprobs[shard].maps.push_back(codebook);
	}
	
	os.pop();
	
	utils::tempfile::permission(path);

	ngram.logprobs[shard].logprobs.clear();
	ngram.logprobs[shard].quantized.open(path);
      }
      
      if (! ngram.backoffs[shard].quantized.is_open() && ngram.backoffs[shard].logprobs.is_open()) {
	
	if (debug)
	  std::cerr << "shard: " << shard << " quantize backoff" << std::endl;

	const path_type path = utils::tempfile::directory_name(tmp_dir / "expgram.backoff.quantized.XXXXXX");
	utils::tempfile::insert(path);
	
	boost::iostreams::filtering_ostream os;
	os.push(utils::packed_sink<quantized_type, std::allocator<quantized_type> >(path));
	
	std::fill(codebook.begin(), codebook.end(), logprob_type(0.0));
	
	ngram.backoffs[shard].maps.clear();
	ngram.backoffs[shard].maps.push_back(codebook);
	if (shard == 0) {
	  quantize(os, ngram.backoffs[shard], counts, codemap, codebook, 1);
	  ngram.backoffs[shard].maps.push_back(codebook);
	} else
	  ngram.backoffs[shard].maps.push_back(codebook);
	
	for (int order = 2; order < ngram.index.order(); ++ order) {
	  quantize(os, ngram.backoffs[shard], counts, codemap, codebook, order);
	  ngram.backoffs[shard].maps.push_back(codebook);
	}
	
	os.pop();

	utils::tempfile::permission(path);
	
	ngram.backoffs[shard].logprobs.clear();
	ngram.backoffs[shard].quantized.open(path);
      }
      
      if (! ngram.logbounds[shard].quantized.is_open() && ngram.logbounds[shard].logprobs.is_open()) {
	
	if (debug)
	  std::cerr << "shard: " << shard << " quantize logbound" << std::endl;
	
	const path_type path = utils::tempfile::directory_name(tmp_dir / "expgram.logbound.quantized.XXXXXX");
	utils::tempfile::insert(path);
	
	boost::iostreams::filtering_ostream os;
	os.push(utils::packed_sink<quantized_type, std::allocator<quantized_type> >(path));
	
	std::fill(codebook.begin(), codebook.end(), logprob_type(0.0));
	
	ngram.logbounds[shard].maps.clear();
	ngram.logbounds[shard].maps.push_back(codebook);
	if (shard == 0) {
	  quantize(os, ngram.logbounds[shard], counts, codemap, codebook, 1);
	  ngram.logbounds[shard].maps.push_back(codebook);
	} else
	  ngram.logbounds[shard].maps.push_back(codebook);
	
	for (int order = 2; order < ngram.index.order(); ++ order) {
	  quantize(os, ngram.logbounds[shard], counts, codemap, codebook, order);
	  ngram.logbounds[shard].maps.push_back(codebook);
	}
	
	os.pop();

	utils::tempfile::permission(path);
	
	ngram.logbounds[shard].logprobs.clear();
	ngram.logbounds[shard].quantized.open(path);
      }
    }
  };
  
  // perform quantization...
  void NGram::quantize()
  {
    typedef NGramQuantizeMapReduce map_reduce_type;
    typedef NGramQuantizeReducer   reducer_type;
    
    typedef map_reduce_type::thread_type         thread_type;
    typedef map_reduce_type::thread_ptr_set_type thread_ptr_set_type;
    
    thread_ptr_set_type threads(index.size());
    for (int shard = 0; shard < index.size(); ++ shard)
      threads[shard].reset(new thread_type(reducer_type(*this, shard, debug)));
    for (int shard = 0; shard < index.size(); ++ shard)
      threads[shard]->join();
    threads.clear();
  }
  
  // dump in arpa format...  
  // we use threads to  access the data structure.
  // the only-one reducer simply merge and dump in (word-id) sorted order
  //
  
  struct NGramDumpMapReduce
  {
    typedef boost::thread                                                  thread_type;
    typedef boost::shared_ptr<thread_type>                                 thread_ptr_type;
    typedef std::vector<thread_ptr_type, std::allocator<thread_ptr_type> > thread_ptr_set_type;
    
    typedef expgram::NGram              ngram_type;
    
    typedef ngram_type::word_type       word_type;
    typedef ngram_type::vocab_type      vocab_type;
    typedef ngram_type::id_type         id_type;
    typedef ngram_type::logprob_type    logprob_type;
    
    typedef ngram_type::size_type       size_type;
    typedef ngram_type::difference_type difference_type;
    typedef ngram_type::path_type       path_type;
    
    typedef std::vector<id_type, std::allocator<id_type> >           context_type;
    typedef std::pair<logprob_type, logprob_type>                    logprob_pair_type;
    typedef std::pair<id_type, logprob_pair_type>                    word_logprob_pair_type;
    
    typedef std::vector<word_logprob_pair_type, std::allocator<word_logprob_pair_type> > word_set_type;
    typedef std::pair<context_type, word_set_type>                                       context_logprob_type;
    
    typedef utils::lockfree_list_queue<context_logprob_type, std::allocator<context_logprob_type> > queue_type;
    typedef boost::shared_ptr<queue_type>                                                      queue_ptr_type;
    typedef std::vector<queue_ptr_type, std::allocator<queue_ptr_type> >                       queue_ptr_set_type;
  };
  
  inline
  void swap(NGramDumpMapReduce::context_logprob_type& x,
	    NGramDumpMapReduce::context_logprob_type& y)
  {
    x.first.swap(y.first);
    x.second.swap(y.second);
  }
  
  struct NGramDumpMapper
  {
    typedef NGramDumpMapReduce map_reduce_type;

    typedef map_reduce_type::ngram_type   ngram_type;
    
    typedef map_reduce_type::id_type      id_type;
    typedef map_reduce_type::size_type    size_type;
    typedef map_reduce_type::logprob_type logprob_type;
    
    typedef map_reduce_type::context_type         context_type;
    typedef map_reduce_type::word_set_type        word_set_type;
    typedef map_reduce_type::context_logprob_type context_logprob_type;
    
    typedef map_reduce_type::queue_type queue_type;

    const ngram_type& ngram;
    queue_type&       queue;
    int               shard;
    
    NGramDumpMapper(const ngram_type& _ngram,
		    queue_type&       _queue,
		    const int         _shard)
      : ngram(_ngram),
	queue(_queue),
	shard(_shard) {}
    
    void operator()()
    {
      context_type         context;
      context_logprob_type context_logprob;

      for (int order_prev = 1; order_prev < ngram.index.order(); ++ order_prev) {
	const size_type pos_context_first = ngram.index[shard].offsets[order_prev - 1];
	const size_type pos_context_last  = ngram.index[shard].offsets[order_prev];
	
	context.resize(order_prev);
	
	size_type pos_last_prev = pos_context_last;
	for (size_type pos_context = pos_context_first; pos_context < pos_context_last; ++ pos_context) {
	  const size_type pos_first = pos_last_prev;
	  const size_type pos_last = ngram.index[shard].children_last(pos_context);
	  pos_last_prev = pos_last;
	  
	  if (pos_first == pos_last) continue;
	  
	  context_type::iterator citer_curr = context.end() - 1;
	  for (size_type pos_curr = pos_context; pos_curr != size_type(-1); pos_curr = ngram.index[shard].parent(pos_curr), -- citer_curr)
	    *citer_curr = ngram.index[shard][pos_curr];
	  
	  context_logprob.first = context;
	  context_logprob.second.clear();
	  
	  word_set_type& words = context_logprob.second;
	  for (size_type pos = pos_first; pos != pos_last; ++ pos) {
	    const logprob_type logprob = ngram.logprobs[shard](pos, order_prev + 1);
	    if (logprob != ngram.logprob_min()) {
	      const logprob_type backoff = (pos < ngram.backoffs[shard].size() ? ngram.backoffs[shard](pos, order_prev + 1) : logprob_type(0.0));
	      words.push_back(std::make_pair(ngram.index[shard][pos], std::make_pair(logprob, backoff)));
	    }
	  }
	  
	  queue.push_swap(context_logprob);
	}
      }
      
      queue.push(context_logprob_type(context_type(), word_set_type()));
    }
  };

  template <typename Tp>
  struct greater_pfirst_size_value
  {
    bool operator()(const boost::shared_ptr<Tp>& x, const boost::shared_ptr<Tp>& y) const
    {
      return x->first.size() > y->first.size() || (x->first.size() == y->first.size() && x->first > y->first);
    }
    
    bool operator()(const Tp* x, const Tp* y) const
    {
      return x->first.size() > y->first.size() || (x->first.size() == y->first.size() && x->first > y->first);
    }
  };
  
  void NGram::dump(const path_type& path) const
  {
    typedef NGramDumpMapReduce map_reduce_type;
    typedef NGramDumpMapper    mapper_type;
    
    typedef map_reduce_type::thread_type         thread_type;
    typedef map_reduce_type::thread_ptr_set_type thread_ptr_set_type;
    typedef map_reduce_type::queue_type          queue_type;
    typedef map_reduce_type::queue_ptr_set_type  queue_ptr_set_type;
    
    typedef map_reduce_type::context_type         context_type;
    typedef map_reduce_type::word_set_type        word_set_type;
    typedef map_reduce_type::context_logprob_type context_logprob_type;

    typedef std::vector<const char*, std::allocator<const char*> > vocab_map_type;
    
    typedef std::pair<word_set_type, queue_type*>       words_queue_type;
    typedef std::pair<context_type, words_queue_type>   context_words_queue_type;
    typedef boost::shared_ptr<context_words_queue_type> context_words_queue_ptr_type;
    
    typedef std::vector<context_words_queue_ptr_type, std::allocator<context_words_queue_ptr_type> > pqueue_base_type;
    typedef std::priority_queue<context_words_queue_ptr_type, pqueue_base_type, greater_pfirst_size_value<context_words_queue_type> > pqueue_type;
    
    if (index.empty()) return;
    
    thread_ptr_set_type threads(index.size());
    queue_ptr_set_type  queues(index.size());
    
    for (int shard = 0; shard < index.size(); ++ shard) {
      queues[shard].reset(new queue_type(1024 * 64));
      threads[shard].reset(new thread_type(mapper_type(*this, *queues[shard], shard)));
    }
    
    const id_type bos_id = index.vocab()[vocab_type::BOS];
    
    vocab_map_type vocab_map;
    vocab_map.reserve(index[0].offsets[1]);
    
    utils::compress_ostream os(path, 1024 * 1024);
    os.precision(7);
    
    // dump headers...
    os << "\\data\\" << '\n';
    {
      os << "ngram " << 1 << '=' << index[0].offsets[1] << '\n';
      for (int order = 2; order <= index.order(); ++ order) {
	size_type size = 0;
	for (int shard = 0; shard < index.size(); ++ shard)
	  size += index[shard].offsets[order] - index[shard].offsets[order - 1];
	os << "ngram " << order << '=' << size << '\n';
      }
    }
    os << '\n';
    
    const double log_10 = utils::mathop::log(10.0);

    // unigrams...
    {
      static const logprob_type logprob_bos = double(-99) * utils::mathop::log(10);

      os << "\\1-grams:" << '\n';
      for (size_type pos = 0; pos < index[0].offsets[1]; ++ pos) {
	logprob_type logprob = logprobs[0](pos, 1);
	
	// escape for BOS
	if (logprob == logprob_min() && pos == bos_id)
	  logprob = logprob_bos;
	
	if (logprob != logprob_min()) {
	  const id_type id(pos);
	  
	  if (id >= vocab_map.size())
	    vocab_map.resize(id + 1, 0);
	  if (! vocab_map[id])
	    vocab_map[id] = static_cast<const std::string&>(index.vocab()[id]).c_str();
	  
	  const logprob_type backoff = (pos < backoffs[0].size() ? backoffs[0](pos, 1) : logprob_type(0.0));	  
	  os << (logprob / log_10) << '\t' << vocab_map[id];
	  if (backoff != 0.0)
	    os << '\t' << (backoff / log_10);
	  os << '\n';
	}
      }
    }
    
    // ngrams...
    pqueue_type pqueue;
    for (int shard = 0; shard < index.size(); ++ shard) {
      context_logprob_type context_logprob;
      queues[shard]->pop_swap(context_logprob);
      
      if (! context_logprob.first.empty()) {
	context_words_queue_ptr_type context_queue(new context_words_queue_type());
	context_queue->first.swap(context_logprob.first);
	context_queue->second.first.swap(context_logprob.second);
	context_queue->second.second = &(*queues[shard]);
	
	pqueue.push(context_queue);
      }
    }
    
    typedef std::vector<const char*, std::allocator<const char*> > phrase_type;

    phrase_type phrase;
    int order = 1;
    while (! pqueue.empty()) {
      context_words_queue_ptr_type context_queue(pqueue.top());
      pqueue.pop();
      
      if (context_queue->first.size() + 1 != order) {
	order = context_queue->first.size() + 1;
	os << '\n';
	os << "\\" << order << "-grams:" << '\n';
      }
      
      phrase.clear();
      context_type::const_iterator citer_end = context_queue->first.end();
      for (context_type::const_iterator citer = context_queue->first.begin(); citer != citer_end; ++ citer) {
	if (*citer >= vocab_map.size())
	  vocab_map.resize(*citer + 1, 0);
	if (! vocab_map[*citer])
	  vocab_map[*citer] = static_cast<const std::string&>(index.vocab()[*citer]).c_str();
	
	phrase.push_back(vocab_map[*citer]);
      }
      
      word_set_type::const_iterator witer_end = context_queue->second.first.end();
      for (word_set_type::const_iterator witer = context_queue->second.first.begin(); witer != witer_end; ++ witer) {
	const id_type id = witer->first;
	const logprob_type& logprob = witer->second.first;
	const logprob_type& backoff = witer->second.second;
	
	if (id >= vocab_map.size())
	  vocab_map.resize(id + 1, 0);
	if (! vocab_map[id])
	  vocab_map[id] = static_cast<const std::string&>(index.vocab()[id]).c_str();
	
	os << (logprob / log_10) << '\t';
	std::copy(phrase.begin(), phrase.end(), std::ostream_iterator<const char*>(os, " "));
	os << vocab_map[id];
	
	if (backoff != 0.0)
	  os << '\t' << (backoff / log_10);
	os << '\n';
      }
      
      context_logprob_type context_logprob;
      context_queue->second.second->pop_swap(context_logprob);
      if (! context_logprob.first.empty()) {
	context_queue->first.swap(context_logprob.first);
	context_queue->second.first.swap(context_logprob.second);
	
	pqueue.push(context_queue);
      }
    }
    
    os << '\n';
    os << "\\end\\" << '\n';
  }

  struct NGramBoundMapReduce
  {
    typedef boost::thread                                                  thread_type;
    typedef boost::shared_ptr<thread_type>                                 thread_ptr_type;
    typedef std::vector<thread_ptr_type, std::allocator<thread_ptr_type> > thread_ptr_set_type;

    typedef expgram::NGram ngram_type;
    
    typedef ngram_type::word_type       word_type;
    typedef ngram_type::vocab_type      vocab_type;
    typedef ngram_type::id_type         id_type;
    
    typedef ngram_type::logprob_type    logprob_type;
    typedef ngram_type::size_type       size_type;
    typedef ngram_type::difference_type difference_type;
    
    typedef boost::filesystem::path         path_type;
    
    typedef std::vector<id_type, std::allocator<id_type> >           context_type;
    typedef std::pair<context_type, logprob_type>                    context_logprob_type;
    
    typedef utils::lockfree_list_queue<context_logprob_type, std::allocator<context_logprob_type> > queue_type;
    typedef boost::shared_ptr<queue_type>                                                      queue_ptr_type;
    typedef std::vector<queue_ptr_type, std::allocator<queue_ptr_type> >                       queue_ptr_set_type;
  };
  
  struct NGramBoundMapper
  {
    typedef NGramBoundMapReduce map_reduce_type;
    
    typedef map_reduce_type::ngram_type        ngram_type;
    
    typedef map_reduce_type::size_type          size_type;
    typedef map_reduce_type::id_type            id_type;
    typedef map_reduce_type::logprob_type       logprob_type;
    typedef map_reduce_type::context_type       context_type;
    typedef map_reduce_type::queue_ptr_set_type queue_ptr_set_type;
    
    const ngram_type&   ngram;
    queue_ptr_set_type& queues;
    int                 shard;
    int                 debug;
    
    NGramBoundMapper(const ngram_type&   _ngram,
		     queue_ptr_set_type& _queues,
		     const int           _shard,
		     const int           _debug)
      : ngram(_ngram),
	queues(_queues),
	shard(_shard),
	debug(_debug) {}
    
    void operator()()
    {
      typedef std::vector<logprob_type, std::allocator<logprob_type> > logprob_set_type;

      logprob_set_type unigrams(ngram.index[shard].offsets[1], ngram.logprob_min());
      context_type context;
      
      for (int order_prev = 1; order_prev < ngram.index.order(); ++ order_prev) {
	const size_type pos_context_first = ngram.index[shard].offsets[order_prev - 1];
	const size_type pos_context_last  = ngram.index[shard].offsets[order_prev];

	if (debug)
	  std::cerr << "shard: " << shard << " order: " << (order_prev + 1) << std::endl;
	
	context.resize(order_prev + 1);
	
	size_type pos_last_prev = pos_context_last;
	for (size_type pos_context = pos_context_first; pos_context < pos_context_last; ++ pos_context) {
	  const size_type pos_first = pos_last_prev;
	  const size_type pos_last = ngram.index[shard].children_last(pos_context);
	  pos_last_prev = pos_last;
	  
	  if (pos_first == pos_last) continue;
	  
	  context_type::iterator citer_curr = context.end() - 2;
	  for (size_type pos_curr = pos_context; pos_curr != size_type(-1); pos_curr = ngram.index[shard].parent(pos_curr), -- citer_curr)
	    *citer_curr = ngram.index[shard][pos_curr];
	  
	  for (size_type pos = pos_first; pos != pos_last; ++ pos) {
	    context.back() = ngram.index[shard][pos];
	    
	    const logprob_type logprob = ngram.logprobs[shard](pos, order_prev + 1);
	    if (logprob != ngram.logprob_min()) {
	      
#if 1
	      context_type::const_iterator citer_end = context.end();
	      context_type::const_iterator citer_begin = context.begin() + 1;
	      if (citer_end - citer_begin == 1)
		unigrams[*citer_begin] = std::max(unigrams[*citer_begin], logprob);
	      else
		queues[ngram.index.shard_index(citer_begin, citer_end)]->push(std::make_pair(context_type(citer_begin, citer_end), logprob));
#endif
#if 0      
	      context_type::const_iterator citer_end = context.end();
	      for (context_type::const_iterator citer = context.begin() + 1; citer != citer_end; ++ citer) {
		if (citer_end - citer == 1)
		  unigrams[*citer] = std::max(unigrams[*citer], logprob);
		else
		  queues[ngram.index.shard_index(citer, citer_end)]->push(std::make_pair(context_type(citer, citer_end), logprob));
	      }
#endif
	    }
	  }
	}
      }

      // reduce unigram's bounds...
      for (id_type id = 0; id < unigrams.size(); ++ id)
	if (unigrams[id] > ngram.logprob_min())
	  queues[0]->push(std::make_pair(context_type(1, id), unigrams[id]));
      
      for (int shard = 0; shard < queues.size(); ++ shard)
	queues[shard]->push(std::make_pair(context_type(), 0.0));
    }
  };
  
  struct NGramBoundReducer
  {
    typedef NGramBoundMapReduce map_reduce_type;

    typedef map_reduce_type::ngram_type           ngram_type;
    
    typedef map_reduce_type::size_type            size_type;
    typedef map_reduce_type::id_type              id_type;
    typedef map_reduce_type::logprob_type         logprob_type;
    typedef map_reduce_type::context_type         context_type;
    typedef map_reduce_type::context_logprob_type context_logprob_type;
    
    typedef map_reduce_type::queue_type           queue_type;
    typedef map_reduce_type::path_type            path_type;
    
    ngram_type&       ngram;
    queue_type&       queue;
    int               shard;
    int               debug;
    
    NGramBoundReducer(ngram_type&      _ngram,
		      queue_type&      _queue,
		      const int        _shard,
		      const int        _debug)
      : ngram(_ngram),
	queue(_queue),
	shard(_shard),
	debug(_debug)
    {}
    
    void operator()()
    {
      typedef std::vector<logprob_type, std::allocator<logprob_type> > logprob_set_type;
      
      const size_type offset = ngram.logbounds[shard].offset;
      
      logprob_set_type logbounds(ngram.logprobs[shard].logprobs.begin(),
				 ngram.logprobs[shard].logprobs.begin() + ngram.index[shard].position_size() - offset);
      context_logprob_type context_logprob;
      size_type num_empty = 0;
      
      while (num_empty < ngram.index.size()) {
	queue.pop(context_logprob);
	
	if (context_logprob.first.empty()) {
	  ++ num_empty;
	  continue;
	}
	
	const context_type& context = context_logprob.first;
	
	std::pair<context_type::const_iterator, size_type> result = ngram.index.traverse(shard, context.begin(), context.end());
	if (result.first != context.end() || result.second == size_type(-1)) {
	  if (debug >= 2) {
	    std::cerr << "WARNING: no ngram for:";
	    context_type::const_iterator citer_end = context.end();
	    for (context_type::const_iterator citer = context.begin(); citer != citer_end; ++ citer)
	      std::cerr << ' ' << ngram.index.vocab()[*citer];
	    std::cerr << std::endl;
	  }
	  continue;
	}
	
	logprob_type& bound = logbounds[result.second - offset];
	bound = std::max(bound, context_logprob.second);
      }
      
      // dump...
      const path_type path = utils::tempfile::file_name(utils::tempfile::tmp_dir() / "expgram.logbound.XXXXXX");
      utils::tempfile::insert(path);
      dump_file(path, logbounds);
      utils::tempfile::permission(path);
      ngram.logbounds[shard].logprobs.open(path);
      
      
      if (debug)
	std::cerr << "shard: " << shard << " logbound: " << ngram.logbounds[shard].size() << std::endl;
    }
  };
  
  void NGram::bounds()
  {
    // we use threads to compute upper-bounds
    
    typedef NGramBoundMapReduce map_reduce_type;
    typedef NGramBoundMapper    mapper_type;
    typedef NGramBoundReducer   reducer_type;
    
    typedef map_reduce_type::thread_type        thread_type;
    typedef map_reduce_type::queue_type         queue_type;
    typedef map_reduce_type::queue_ptr_set_type queue_ptr_set_type;
    
    typedef boost::shared_ptr<thread_type>                                 thread_ptr_type;
    typedef std::vector<thread_ptr_type, std::allocator<thread_ptr_type> > thread_ptr_set_type;
    
    // check if we already computed upper-bounds...
    if (logbounds.size() == logprobs.size())
      return;

    std::cerr << "upper-bounds" << std::endl;

    queue_ptr_set_type  queues(index.size());
    thread_ptr_set_type threads_mapper(index.size());
    thread_ptr_set_type threads_reducer(index.size());
    
    // first, run reducer...
    logbounds.clear();
    logbounds.resize(index.size());
    for (int shard = 0; shard < logbounds.size(); ++ shard) {
      logbounds[shard].offset = logprobs[shard].offset;
      
      queues[shard].reset(new queue_type(1024 * 64));
      threads_reducer[shard].reset(new thread_type(reducer_type(*this, *queues[shard], shard, debug)));
    }
    
    // second, mapper...
    for (int shard = 0; shard < logbounds.size(); ++ shard)
      threads_mapper[shard].reset(new thread_type(mapper_type(*this, queues, shard, debug)));
    
    // termination...
    for (int shard = 0; shard < logbounds.size(); ++ shard)
      threads_mapper[shard]->join();
    for (int shard = 0; shard < logbounds.size(); ++ shard)
      threads_reducer[shard]->join();
  }

  static const std::string& __BOS = static_cast<const std::string&>(Vocab::BOS);
  static const std::string& __EOS = static_cast<const std::string&>(Vocab::EOS);
  static const std::string& __UNK = static_cast<const std::string&>(Vocab::UNK);
  
  inline
  NGram::word_type escape_word(const std::string& word)
  {
    if (strcasecmp(word.c_str(), __BOS.c_str()) == 0)
      return Vocab::BOS;
    else if (strcasecmp(word.c_str(), __EOS.c_str()) == 0)
      return Vocab::EOS;
    else if (strcasecmp(word.c_str(), __UNK.c_str()) == 0)
      return Vocab::UNK;
    else
      return word;
  }
  
  // we define map-reduce object, but no mapper.... since mapper will be managed by main thread...
  struct NGramIndexMapReduce
  {
    typedef expgram::NGram              ngram_type;
    
    typedef ngram_type::word_type       word_type;
    typedef ngram_type::vocab_type      vocab_type;
    typedef ngram_type::id_type         id_type;
    typedef ngram_type::logprob_type    logprob_type;
    
    typedef ngram_type::size_type       size_type;
    typedef ngram_type::difference_type difference_type;
    
    typedef ngram_type::path_type                              path_type;
    typedef std::vector<path_type, std::allocator<path_type> > path_set_type;
    
    typedef std::vector<id_type, std::allocator<id_type> > context_type;
    typedef std::pair<logprob_type, logprob_type>          logprob_pair_type;
    typedef std::pair<context_type, logprob_pair_type>     context_logprob_pair_type;
    
    typedef boost::thread                                                  thread_type;
    typedef boost::shared_ptr<thread_type>                                 thread_ptr_type;
    typedef std::vector<thread_ptr_type, std::allocator<thread_ptr_type> > thread_ptr_set_type;
    
    typedef utils::lockfree_list_queue<context_logprob_pair_type, std::allocator<context_logprob_pair_type> > queue_type;
    typedef boost::shared_ptr<queue_type>                                                                queue_ptr_type;
    typedef std::vector<queue_ptr_type, std::allocator<queue_ptr_type> >                                 queue_ptr_set_type;
    
    typedef boost::iostreams::filtering_ostream                              ostream_type;
    typedef boost::shared_ptr<ostream_type>                                  ostream_ptr_type;
    typedef std::vector<ostream_ptr_type, std::allocator<ostream_ptr_type> > ostream_ptr_set_type;
    
  };
  
  struct NGramIndexReducer
  {
    typedef NGramIndexMapReduce map_reduce_type;

    typedef map_reduce_type::ngram_type      ngram_type;
    
    typedef map_reduce_type::thread_type     thread_type;
    typedef map_reduce_type::size_type       size_type;
    typedef map_reduce_type::difference_type difference_type;
    
    typedef map_reduce_type::word_type       word_type;
    typedef map_reduce_type::vocab_type      vocab_type;
    typedef map_reduce_type::id_type         id_type;
    typedef map_reduce_type::path_type       path_type;
    
    typedef map_reduce_type::context_type              context_type;
    typedef map_reduce_type::logprob_type              logprob_type;
    typedef map_reduce_type::logprob_pair_type         logprob_pair_type;
    typedef map_reduce_type::context_logprob_pair_type context_logprob_pair_type;
    
    typedef map_reduce_type::queue_type                queue_type;
    typedef map_reduce_type::ostream_type              ostream_type;
    
    typedef std::pair<id_type, logprob_pair_type>                                        word_logprob_pair_type;
    typedef std::vector<word_logprob_pair_type, std::allocator<word_logprob_pair_type> > word_logprob_pair_set_type;
    
    typedef utils::packed_vector<id_type, std::allocator<id_type> >  id_set_type;
    typedef std::vector<logprob_type, std::allocator<logprob_type> > logprob_set_type;
    typedef std::vector<size_type, std::allocator<size_type> >       size_set_type;
    
    ngram_type&   ngram;
    queue_type&   queue;
    ostream_type& os_logprob;
    ostream_type& os_backoff;
    int shard;
    int max_order;
    int debug;

    // thread local...
    id_set_type ids;
    logprob_set_type logprobs;
    logprob_set_type backoffs;
    size_set_type    positions_first;
    size_set_type    positions_last;
    
    
    NGramIndexReducer(ngram_type&   _ngram,
		      queue_type&   _queue,
		      ostream_type& _os_logprob,
		      ostream_type& _os_backoff,
		      const int     _shard,
		      const int     _max_order,
		      const int     _debug)
      : ngram(_ngram),
	queue(_queue),
	os_logprob(_os_logprob),
	os_backoff(_os_backoff),
	shard(_shard),
	max_order(_max_order),
	debug(_debug) {}
    
    template <typename Tp>
    struct less_first
    {
      bool operator()(const Tp& x, const Tp& y) const
      {
	return x.first < y.first;
      }
    };
    
    
    template <typename Iterator>
    void dump(std::ostream& os, Iterator first, Iterator last)
    {
      typedef typename std::iterator_traits<Iterator>::value_type value_type;
      os.write((char*) &(*first), (last - first) * sizeof(value_type));
    }
    
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
      
      if (ngram.index[shard].ids.is_open()) {
	for (size_type pos = 0; pos < ngram.index[shard].ids.size(); ++ pos) {
	  const id_type id = ngram.index[shard].ids[pos];
	  os_id.write((char*) &id, sizeof(id_type));
	}
      }
      
      ids.build();

      for (size_type i = 0; i < positions_size; ++ i) {
	const size_type pos_first = positions_first[i];
	const size_type pos_last  = positions_last[i];
	
	if (pos_last > pos_first) {
	  dump(os_logprob, logprobs.begin() + pos_first, logprobs.begin() + pos_last);
	  
	  if (order_prev + 1 != max_order)
	    dump(os_backoff, backoffs.begin() + pos_first, backoffs.begin() + pos_last);
	  
	  for (size_type pos = pos_first; pos != pos_last; ++ pos) {
	    const id_type id = ids[pos];
	    os_id.write((char*) &id, sizeof(id_type));
	    positions.set(positions.size(), true);
	  }
	}
	positions.set(positions.size(), false);
      }
      
      // perform indexing...
      os_id.pop();
      positions.write(path_position);
      
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
      
      if (debug)
	std::cerr << "shard: " << shard
		  << " index: " << ngram.index[shard].ids.size()
		  << " positions: " << ngram.index[shard].positions.size()
		  << " offsets: "  << ngram.index[shard].offsets.back()
		  << std::endl;

      // remove temporary index
      ids.clear();
      logprobs.clear();
      backoffs.clear();
      positions_first.clear();
      positions_last.clear();
    }
    
    void index_ngram(const context_type& prefix, word_logprob_pair_set_type& words)
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

      std::pair<context_type::const_iterator, size_type> result = ngram.index.traverse(shard, prefix.begin(), prefix.end());
      if (result.first != prefix.end() || result.second == size_type(-1)) {
	std::cerr << "context: ";
	std::copy(prefix.begin(), prefix.end(), std::ostream_iterator<id_type>(std::cerr, " "));
	std::cerr << "result: " << result.second
		  << std::endl;
	
	throw std::runtime_error("no prefix?");
      }
      
      const size_type pos = result.second - ngram.index[shard].offsets[order_prev - 1];
      positions_first[pos] = ids.size();
      positions_last[pos] = ids.size() + words.size();
      
      std::sort(words.begin(), words.end(), less_first<word_logprob_pair_type>());
      word_logprob_pair_set_type::const_iterator witer_end = words.end();
      for (word_logprob_pair_set_type::const_iterator witer = words.begin(); witer != witer_end; ++ witer) {
	ids.push_back(witer->first);
	logprobs.push_back(witer->second.first);
	
	if (order_prev + 1 != max_order)
	  backoffs.push_back(witer->second.second);
      }
    }
    
    void operator()()
    {
      context_logprob_pair_type context_logprob_pair;
      context_type prefix;
      word_logprob_pair_set_type words;
      
      // we will start from bigram... unigrams...
      int order = 2;
      
      while (1) {
	
	queue.pop(context_logprob_pair);
	if (context_logprob_pair.first.empty()) break;
	
	const context_type&      context  = context_logprob_pair.first;
	const logprob_pair_type& logprobs = context_logprob_pair.second;
	
	if (context.size() != prefix.size() + 1 || ! std::equal(prefix.begin(), prefix.end(), context.begin())) {
	  if (! words.empty()) {
	    index_ngram(prefix, words);
	    words.clear();
	    
	    if (context.size() != order)
	      index_ngram();
	  }
	  
	  prefix.clear();
	  prefix.insert(prefix.end(), context.begin(), context.end() - 1);
	  order = context.size();
	}
	
	words.push_back(std::make_pair(context.back(), logprobs));
      }
      
      // perform final indexing...
      if (! words.empty()) {
	index_ngram(prefix, words);
	index_ngram();
      }
    }
  };
  
  template <typename Tp>
  struct greater_second_first
  {
    bool operator()(const Tp& x, const Tp& y) const
    {
      return x.second.first > y.second.first;
    }
  };
  
  void NGram::open_arpa(const path_type& path,
			const size_type shard_size)
  {
    typedef NGramIndexMapReduce map_reduce_type;
    typedef NGramIndexReducer   reducer_type;
    
    typedef map_reduce_type::thread_type          thread_type;
    typedef map_reduce_type::thread_ptr_set_type  thread_ptr_set_type;
    typedef map_reduce_type::queue_type           queue_type;
    typedef map_reduce_type::queue_ptr_set_type   queue_ptr_set_type;
    typedef map_reduce_type::ostream_type         ostream_type;
    typedef map_reduce_type::ostream_ptr_set_type ostream_ptr_set_type;
    typedef map_reduce_type::path_set_type        path_set_type;
    typedef map_reduce_type::context_type         context_type;
    
    
    typedef boost::tokenizer<utils::space_separator>               tokenizer_type;
    typedef std::vector<std::string, std::allocator<std::string> > tokens_type;
    
    typedef std::pair<logprob_type, logprob_type>                                        logprob_pair_type;
    typedef std::pair<word_type, logprob_pair_type>                                      word_logprob_pair_type;
    typedef std::vector<word_logprob_pair_type, std::allocator<word_logprob_pair_type> > word_logprob_pair_set_type;
    
    typedef std::vector<id_type, std::allocator<id_type> > vocab_map_type;
    
    // setup structures...
    clear();
    
    index.reserve(shard_size);
    logprobs.reserve(shard_size);
    backoffs.reserve(shard_size);
    
    index.resize(shard_size);
    logprobs.resize(shard_size);
    backoffs.resize(shard_size);
    
    // setup paths and streams for logprob/backoff
    const path_type tmp_dir = utils::tempfile::tmp_dir();
    
    ostream_ptr_set_type os_logprobs(shard_size);
    ostream_ptr_set_type os_backoffs(shard_size);
    path_set_type        path_logprobs(shard_size);
    path_set_type        path_backoffs(shard_size);
    for (int shard = 0; shard < shard_size; ++ shard) {
      path_logprobs[shard] = utils::tempfile::file_name(tmp_dir / "expgram.logprob.XXXXXX");
      path_backoffs[shard] = utils::tempfile::file_name(tmp_dir / "expgram.backoff.XXXXXX");
      
      utils::tempfile::insert(path_logprobs[shard]);
      utils::tempfile::insert(path_backoffs[shard]);
      
      os_logprobs[shard].reset(new ostream_type());
      os_backoffs[shard].reset(new ostream_type());
      os_logprobs[shard]->push(boost::iostreams::file_sink(path_logprobs[shard].file_string().c_str()), 1024 * 1024);
      os_backoffs[shard]->push(boost::iostreams::file_sink(path_backoffs[shard].file_string().c_str()), 1024 * 1024);
    }
    
    typedef enum {
      NONE,
      END,
      DATA,
      NGRAMS,
    } mode_type;
    
    const double log_10 = utils::mathop::log(10.0);
    
    utils::compress_istream is(path, 1024 * 1024);
    
    mode_type mode(NONE);
    bool start_data = false;
    int order = 0;
    int max_order = 0;
    
    logprob_type logprob_lowest = boost::numeric::bounds<logprob_type>::highest();
    smooth = boost::numeric::bounds<logprob_type>::lowest();
    
    std::string line;
    tokens_type tokens;

    word_logprob_pair_set_type unigrams;
    
    while (std::getline(is, line)) {
      tokenizer_type tokenized(line);
      
      tokens.clear();
      tokens.insert(tokens.end(), tokenized.begin(), tokenized.end());
      
      if (tokens.empty()) continue;
      
      if (tokens.size() == 1 && tokens[0][0] == '\\') {
	if (tokens[0] == "\\end\\")
	  mode = END;
	else if (tokens[0] == "\\data\\") {
	  mode = DATA;
	  start_data = true;
	} else if (start_data && strcmp(&(tokens[0].c_str()[tokens[0].size() - 7]), "-grams:") == 0) {
	  sscanf(tokens[0].c_str(), "\\%d-grams:", &order);
	  index.order() = order;
	  
	  if (debug)
	    std::cerr << "order: " << order << std::endl;
	  
	  mode = NGRAMS;
	  // we will quit!
	  if (order > 1) break;
	} else
	  mode = NONE;
      }
      
      if (mode == DATA && tokens.size() == 2 && tokens[0] == "ngram") {
	std::string::size_type pos = tokens[1].find('=');
	if (pos != std::string::npos)
	  max_order = std::max(max_order, atoi(tokens[1].substr(0, pos).c_str()));
      }
      
      if (order == 0 || mode != NGRAMS) continue;

      if (tokens.size() < 2) continue;
      
      const logprob_type logprob = atof(tokens.front().c_str()) * log_10;
      const logprob_type logbackoff = (tokens.size() == order + 2 ? (atof(tokens.back().c_str()) * log_10) : 0.0);
      
      
      unigrams.push_back(std::make_pair(escape_word(tokens[1]), std::make_pair(logprob, logbackoff)));
      
      if (unigrams.back().first != vocab_type::BOS)
	logprob_lowest = std::min(logprob_lowest, logprob);
    }
    
    // index unigrams!
    std::sort(unigrams.begin(), unigrams.end(), greater_second_first<word_logprob_pair_type>());
    
    vocab_map_type vocab_map;
    vocab_map.reserve(word_type::allocated());
    {
      const path_type path_vocab = utils::tempfile::directory_name(utils::tempfile::tmp_dir() / "expgram.vocab.XXXXXX");
      utils::tempfile::insert(path_vocab);
      
      vocab_type& vocab = index.vocab();
      vocab.open(path_vocab, word_type::allocated() >> 1);
      
      id_type id = 0;
      word_logprob_pair_set_type::const_iterator witer_end = unigrams.end();
      for (word_logprob_pair_set_type::const_iterator witer = unigrams.begin(); witer != witer_end; ++ witer, ++ id) {
	vocab.insert(witer->first);
	
	// use UNK for smoothing parameter
	if (witer->first == vocab_type::UNK)
	  smooth = witer->second.first;
	
	os_logprobs[0]->write((char*) &witer->second.first, sizeof(logprob_type));
	os_backoffs[0]->write((char*) &witer->second.second, sizeof(logprob_type));
	
	if (witer->first.id() >= vocab_map.size())
	  vocab_map.resize(witer->first.id() + 1, id_type(-1));
	vocab_map[witer->first.id()] = id;
      }
      
      vocab.close();
      
      utils::tempfile::permission(path_vocab);

      vocab.open(path_vocab);
      
      
      const size_type unigram_size = unigrams.size();
      
      if (debug)
	std::cerr << "\t1-gram size: " << unigram_size << std::endl;

      for (int shard = 0; shard < index.size(); ++ shard) {
	index[shard].offsets.clear();
	index[shard].offsets.push_back(0);
	index[shard].offsets.push_back(unigram_size);
	
	logprobs[shard].offset = unigram_size;
	backoffs[shard].offset = unigram_size;
      }
      
      logprobs[0].offset = 0;
      backoffs[0].offset = 0;
      
      // setup smooth... is this correct?
      // do we have to estimate again...?
      if (smooth == boost::numeric::bounds<logprob_type>::lowest()) {
	if (logprob_lowest == boost::numeric::bounds<logprob_type>::highest())
	  smooth = utils::mathop::log(1.0 / unigram_size);
	else
	  smooth = logprob_lowest;
      }
      
      unigrams.clear();
      word_logprob_pair_set_type(unigrams).swap(unigrams);
    }
    
    
    
    // prepare queues, run threads!
    queue_ptr_set_type   queues(shard_size);
    thread_ptr_set_type  threads(shard_size);
    for (int shard = 0; shard < shard_size; ++ shard) {
      queues[shard].reset(new queue_type(1024 * 64));
      threads[shard].reset(new thread_type(reducer_type(*this, *queues[shard], *os_logprobs[shard], *os_backoffs[shard], shard, max_order, debug)));
    }
    
    context_type context;
    
    while (std::getline(is, line)) {
      tokenizer_type tokenized(line);
      
      tokens.clear();
      tokens.insert(tokens.end(), tokenized.begin(), tokenized.end());
      
      if (tokens.empty()) continue;
      
      if (tokens.size() == 1 && tokens[0][0] == '\\') {
	if (tokens[0] == "\\end\\")
	  mode = END;
	else if (tokens[0] == "\\data\\") {
	  mode = DATA;
	  start_data = true;
	} else if (start_data && strcmp(&(tokens[0].c_str()[tokens[0].size() - 7]), "-grams:") == 0) {
	  sscanf(tokens[0].c_str(), "\\%d-grams:", &order);
	  index.order() = order;
	  
	  if (debug)
	    std::cerr << "order: " << order << std::endl;

	  mode = NGRAMS;
	} else
	  mode = NONE;
      }
      
      if (order == 0 || mode != NGRAMS) continue;

      if (tokens.size() < order + 1) continue;
      
      const logprob_type logprob = atof(tokens.front().c_str()) * log_10;
      const logprob_type logbackoff = (tokens.size() == order + 2 ? (atof(tokens.back().c_str()) * log_10) : 0.0);
      
      context.clear();
      
      tokens_type::const_iterator titer_begin = tokens.begin() + 1;
      tokens_type::const_iterator titer_end = titer_begin + order;
      for (tokens_type::const_iterator titer = titer_begin; titer != titer_end; ++ titer) {
	const id_type id = escape_word(*titer).id();
	
	if (id >= vocab_map.size() || vocab_map[id] == id_type(-1))
	  throw std::runtime_error("invalid vocbulary");
	
	context.push_back(vocab_map[id]);
      }
      
      const size_type shard = index.shard_index(context.begin(), context.end());
      queues[shard]->push(std::make_pair(context, std::make_pair(logprob, logbackoff)));
    }
    
    
    // termination...
    for (int shard = 0; shard < index.size(); ++ shard)
      queues[shard]->push(std::make_pair(context_type(), std::make_pair(0.0, 0.0)));
    
    for (int shard = 0; shard < index.size(); ++ shard) {
      threads[shard]->join();
      
      os_logprobs[shard].reset();
      os_backoffs[shard].reset();

      utils::tempfile::permission(path_logprobs[shard]);
      utils::tempfile::permission(path_backoffs[shard]);
      
      logprobs[shard].logprobs.open(path_logprobs[shard]);
      backoffs[shard].logprobs.open(path_backoffs[shard]);
      
      if (debug)
	std::cerr << "shard: " << shard
		  << " logprob: " << logprobs[shard].size()
		  << " backoff: " << backoffs[shard].size()
		  << std::endl;
    }
    threads.clear();
    
    // compute upper bounds...
    bounds();
  }
};
