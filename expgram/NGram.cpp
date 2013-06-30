//
//  Copyright(C) 2009-2013 Taro Watanabe <taro.watanabe@nict.go.jp>
//

#include <fstream>
#include <queue>
#include <sstream>

#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/tokenizer.hpp>

#include <utils/lockfree_queue.hpp>
#include <utils/lockfree_list_queue.hpp>
#include <utils/compress_stream.hpp>
#include <utils/space_separator.hpp>
#include <utils/tempfile.hpp>
#include <utils/packed_device.hpp>
#include <utils/lexical_cast.hpp>
#include <utils/unordered_map.hpp>
#include <utils/trie_compact.hpp>
#include <utils/vector2.hpp>

#include "NGram.hpp"
#include "Quantizer.hpp"

namespace expgram
{
 
  template <typename Path, typename Data>
  inline
  void dump_file(const Path& file, const Data& data)
  {
    std::auto_ptr<boost::iostreams::filtering_ostream> os(new boost::iostreams::filtering_ostream());
    os->push(boost::iostreams::file_sink(file.string(), std::ios_base::out | std::ios_base::trunc), 1024 * 1024);
    os->exceptions(std::ostream::eofbit | std::ostream::failbit | std::ostream::badbit);
    
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
    offset = utils::lexical_cast<size_type>(oiter->second);
    
    if (boost::filesystem::exists(rep.path("quantized"))) {
      quantized.open(rep.path("quantized"));
      
      logprob_map_type logprob_map;
      
      maps.clear();
      maps.push_back(logprob_map);
      for (int n = 1; /**/; ++ n) {
	std::ostringstream stream_map_file;
	stream_map_file << n << "-logprob-map";
	
	if (! boost::filesystem::exists(rep.path(stream_map_file.str()))) break;
	
	std::ifstream is(rep.path(stream_map_file.str()).string().c_str());
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
      
      for (size_type n = 1; n < maps.size(); ++ n) {
	std::ostringstream stream_map_file;
	stream_map_file << n << "-logprob-map";
	
	std::ofstream os(rep.path(stream_map_file.str()).string().c_str());
	os.exceptions(std::ostream::eofbit | std::ostream::failbit | std::ostream::badbit);
	os.write((char*) &(*maps[n].begin()), sizeof(logprob_type) * maps[n].size());
      }
    }

    if (logprobs.is_open())
      logprobs.write(rep.path("logprob"));
    
    rep["offset"] = utils::lexical_cast<std::string>(offset);
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
    shards.reserve(utils::lexical_cast<size_t>(siter->second));
    shards.resize(utils::lexical_cast<size_t>(siter->second));
    
    if (shard >= static_cast<int>(shards.size()))
      throw std::runtime_error("shard is out of range");

    std::ostringstream stream_shard;
    stream_shard << "ngram-" << std::setfill('0') << std::setw(6) << shard;
    
    shards[shard].open(rep.path(stream_shard.str()));
  }

  template <typename Path, typename Shards>
  inline
  void open_shards(const Path& path, Shards& shards)
  {
    typedef size_t    size_type;
    typedef ptrdiff_t difference_type;
    
    typedef utils::repository repository_type;
    
    repository_type rep(path, repository_type::read);
    
    repository_type::const_iterator siter = rep.find("shard");
    if (siter == rep.end())
      throw std::runtime_error("no shard size...");

    shards.clear();
    shards.reserve(utils::lexical_cast<size_t>(siter->second));
    shards.resize(utils::lexical_cast<size_t>(siter->second));
    
    for (size_type shard = 0; shard != shards.size(); ++ shard) {
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
    typedef size_t    size_type;
    typedef ptrdiff_t difference_type;

    typedef utils::repository repository_type;
    
    typedef TaskWriter<Path, typename Shards::value_type>    task_type;
    typedef typename task_type::thread_type         thread_type;
    typedef typename task_type::thread_ptr_set_type thread_ptr_set_type;
    
    {
      repository_type rep(path, repository_type::write);
      
      rep["shard"] = utils::lexical_cast<std::string>(shards.size());
    }
    
    ::sync();
    
    thread_ptr_set_type threads(shards.size());
    
    {
      for (size_type shard = 1; shard != shards.size(); ++ shard) {
	std::ostringstream stream_shard;
	stream_shard << "ngram-" << std::setfill('0') << std::setw(6) << shard;
	
	threads[shard].reset(new thread_type(task_type(path / stream_shard.str(), shards[shard])));
      }
      
      // dump for root...
      std::ostringstream stream_shard;
      stream_shard << "ngram-" << std::setfill('0') << std::setw(6) << 0;
      task_type(path / stream_shard.str(), shards[0])();
      
      // terminate...
      for (size_type shard = 1; shard != shards.size(); ++ shard)
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
    
    rep["shard"] = utils::lexical_cast<std::string>(shards.size());
  }


  void NGram::write_prepare(const path_type& file) const
  {
    typedef utils::repository repository_type;
    
    if (path() == file) return;
    
    {
      repository_type rep(file, repository_type::write);
      
      index.write_prepare(rep.path("index"));
      
      if (! logprobs.empty())
	write_shards_prepare(rep.path("logprob"), logprobs);
      if (! backoffs.empty())
	write_shards_prepare(rep.path("backoff"), backoffs);
      if (! logbounds.empty())
	write_shards_prepare(rep.path("logbound"), logbounds);
      
      rep["smooth"] = utils::lexical_cast<std::string>(smooth);
    }
    
    ::sync();
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
    
    rep["smooth"] = utils::lexical_cast<std::string>(smooth);
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
    
    if (! boost::filesystem::exists(path))
      throw std::runtime_error("no file? " + path.string());

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
    smooth = utils::lexical_cast<double>(siter->second);
  }
  
  
  void NGram::open_binary(const path_type& path)
  {
    typedef utils::repository repository_type;
    
    clear();

    if (path.empty())
      throw std::runtime_error("no ngram?");
    else if (! boost::filesystem::exists(path))
      throw std::runtime_error("no ngram? " + path.string());
    
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
    smooth = utils::lexical_cast<double>(siter->second);
    
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
    
    typedef utils::unordered_map<logprob_type, size_type, boost::hash<logprob_type>, std::equal_to<logprob_type>,
				 std::allocator<std::pair<const logprob_type, size_type> > >::type hashed_type;

    typedef std::map<logprob_type, size_type, std::less<logprob_type>,
		     std::allocator<std::pair<const logprob_type, size_type> > > logprob_counts_type;
    //typedef std::map<logprob_type, quantized_type, std::less<logprob_type>,
    //                 std::allocator<std::pair<const logprob_type, quantized_type> > > codemap_type;
    
    typedef utils::unordered_map<logprob_type, quantized_type, boost::hash<logprob_type>, std::equal_to<logprob_type>,
				 std::allocator<std::pair<const logprob_type, quantized_type> > >::type codemap_type;

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

    template <typename OStream, typename LogProbs, typename Hashed, typename Counts, typename Codemap, typename Codebook>
    void quantize(OStream& os, LogProbs& logprobs, Hashed& hashed, Counts& counts, Codemap& codemap, Codebook& codebook, const int order)
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
      
      Quantizer::quantize(ngram, counts, codebook, codemap, debug >= 2);
      
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
      
      hashed_type         hashed;
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
	  quantize(os, ngram.logprobs[shard], hashed, counts, codemap, codebook, 1);
	  ngram.logprobs[shard].maps.push_back(codebook);
	} else
	  ngram.logprobs[shard].maps.push_back(codebook);
	
	for (int order = 2; order <= ngram.index.order(); ++ order) {
	  quantize(os, ngram.logprobs[shard], hashed, counts, codemap, codebook, order);
	  ngram.logprobs[shard].maps.push_back(codebook);
	}
	
	os.pop();

	while (! boost::filesystem::exists(path))
	  boost::thread::yield();
	
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
	  quantize(os, ngram.backoffs[shard], hashed, counts, codemap, codebook, 1);
	  ngram.backoffs[shard].maps.push_back(codebook);
	} else
	  ngram.backoffs[shard].maps.push_back(codebook);
	
	for (int order = 2; order < ngram.index.order(); ++ order) {
	  quantize(os, ngram.backoffs[shard], hashed, counts, codemap, codebook, order);
	  ngram.backoffs[shard].maps.push_back(codebook);
	}
	
	os.pop();
	
	while (! boost::filesystem::exists(path))
	  boost::thread::yield();

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
	  quantize(os, ngram.logbounds[shard], hashed, counts, codemap, codebook, 1);
	  ngram.logbounds[shard].maps.push_back(codebook);
	} else
	  ngram.logbounds[shard].maps.push_back(codebook);
	
	for (int order = 2; order < ngram.index.order(); ++ order) {
	  quantize(os, ngram.logbounds[shard], hashed, counts, codemap, codebook, order);
	  ngram.logbounds[shard].maps.push_back(codebook);
	}
	
	os.pop();
	
	while (! boost::filesystem::exists(path))
	  boost::thread::yield();

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
    for (size_type shard = 0; shard != index.size(); ++ shard)
      threads[shard].reset(new thread_type(reducer_type(*this, shard, debug)));
    for (size_type shard = 0; shard != index.size(); ++ shard)
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
    
    struct logprob_pair_type
    {
      logprob_type logprob;
      logprob_type logbound;
      logprob_type backoff;
      
      logprob_pair_type() : logprob(), logbound(), backoff() {}
      logprob_pair_type(const logprob_type& __logprob,
			const logprob_type& __logbound,
			const logprob_type& __backoff) : logprob(__logprob), logbound(__logbound), backoff(__backoff) {}
    };
    
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

    typedef map_reduce_type::logprob_pair_type logprob_pair_type;

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

	  if (! ngram.logbounds.empty()) {
	    word_set_type& words = context_logprob.second;
	    for (size_type pos = pos_first; pos != pos_last; ++ pos) {
	      const logprob_type logprob = ngram.logprobs[shard](pos, order_prev + 1);
	      if (logprob != ngram.logprob_min()) {
		const logprob_type logbound = (pos < ngram.logbounds[shard].size()
					       ? ngram.logbounds[shard](pos, order_prev + 1)
					       : logprob);
		const logprob_type backoff = (pos < ngram.backoffs[shard].size()
					      ? ngram.backoffs[shard](pos, order_prev + 1)
					      : logprob_type(0.0));
		words.push_back(std::make_pair(ngram.index[shard][pos], logprob_pair_type(logprob, logbound, backoff)));
	      } else {
#if 0
		// debug messages..
		if (shard == 0) {
		  std::cerr << ngram.index.vocab()[ngram.index[shard][pos]];
		  for (size_t i = context.size(); i != 0; -- i)
		    std::cerr << ' ' << ngram.index.vocab()[context[i - 1]];
		  std::cerr << std::endl;
		}
#endif
	      }
	    }
	  } else {
	    word_set_type& words = context_logprob.second;
	    for (size_type pos = pos_first; pos != pos_last; ++ pos) {
	      const logprob_type logprob = ngram.logprobs[shard](pos, order_prev + 1);
	      if (logprob != ngram.logprob_min()) {
		const logprob_type backoff = (pos < ngram.backoffs[shard].size()
					      ? ngram.backoffs[shard](pos, order_prev + 1)
					      : logprob_type(0.0));
		words.push_back(std::make_pair(ngram.index[shard][pos], logprob_pair_type(logprob, logprob, backoff)));
	      }
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
  
  void NGram::dump(const path_type& path, const bool lower) const
  {
    typedef NGramDumpMapReduce map_reduce_type;
    typedef NGramDumpMapper    mapper_type;
    
    typedef map_reduce_type::thread_type         thread_type;
    typedef map_reduce_type::thread_ptr_set_type thread_ptr_set_type;
    typedef map_reduce_type::queue_type          queue_type;
    typedef map_reduce_type::queue_ptr_set_type  queue_ptr_set_type;
    
    typedef map_reduce_type::logprob_pair_type    logprob_pair_type;
    typedef map_reduce_type::context_type         context_type;
    typedef map_reduce_type::word_set_type        word_set_type;
    typedef map_reduce_type::context_logprob_type context_logprob_type;

    typedef std::vector<utils::piece, std::allocator<utils::piece> > vocab_map_type;
    
    typedef std::pair<word_set_type, queue_type*>       words_queue_type;
    typedef std::pair<context_type, words_queue_type>   context_words_queue_type;
    typedef boost::shared_ptr<context_words_queue_type> context_words_queue_ptr_type;
    
    typedef std::vector<context_words_queue_ptr_type, std::allocator<context_words_queue_ptr_type> > pqueue_base_type;
    typedef std::priority_queue<context_words_queue_ptr_type, pqueue_base_type, greater_pfirst_size_value<context_words_queue_type> > pqueue_type;
    
    if (index.empty()) return;
    
    thread_ptr_set_type threads(index.size());
    queue_ptr_set_type  queues(index.size());
    
    for (size_type shard = 0; shard != index.size(); ++ shard) {
      queues[shard].reset(new queue_type(1024 * 64));
      threads[shard].reset(new thread_type(mapper_type(*this, *queues[shard], shard)));
    }
    const id_type bos_id = index.vocab()[vocab_type::BOS];
    const id_type unk_id = index.vocab()[vocab_type::UNK];
    const bool has_unk = (unk_id < index[0].offsets[1]);
    
    vocab_map_type vocab_map;
    vocab_map.reserve(index[0].offsets[1]);
    
    utils::compress_ostream os(path, 1024 * 1024);
    os.exceptions(std::ostream::eofbit | std::ostream::failbit | std::ostream::badbit);
    os.precision(7);
    
    // dump headers...
    os << "\\data\\" << '\n';
    {
      os << "ngram " << 1 << '=' << (index[0].offsets[1] + !has_unk) << '\n';
      for (int order = 2; order <= index.order(); ++ order) {
	size_type size = 0;
	for (size_type shard = 0; shard != index.size(); ++ shard)
	  size += index[shard].offsets[order] - index[shard].offsets[order - 1];
	os << "ngram " << order << '=' << size << '\n';
      }
    }
    os << '\n';
    
    const double factor_log_10 = 1.0 / M_LN10;

    static const logprob_type logprob_srilm_min = double(-99) * M_LN10;

    // unigrams...
    if (lower && ! logbounds.empty() && index.order() > 1) {
      os << "\\1-grams:" << '\n';
      
      if (! has_unk)
	os << (smooth * factor_log_10) << ' ' << (smooth * factor_log_10) << '\t' << vocab_type::UNK << '\n';
      
      for (size_type pos = 0; pos < index[0].offsets[1]; ++ pos) {
	logprob_type logprob = logprobs[0](pos, 1);
	logprob_type logbound = (pos < logbounds[0].size() ? logbounds[0](pos, 1) : logprob);
	
	// escape logprob-min...
	if (logprob == logprob_min() && pos == bos_id)
	  logprob = logprob_srilm_min;
		
	if (logprob != logprob_min()) {
	  
	  // escape logprob-min...
	  if (logbound == logprob_min() && pos == bos_id)
	    logbound = logprob_srilm_min;
	  
	  const id_type id(pos);
	  
	  if (id >= vocab_map.size())
	    vocab_map.resize(id + 1, utils::piece());
	  if (vocab_map[id].empty())
	    vocab_map[id] = index.vocab()[id];
	  
	  const logprob_type backoff = (pos < backoffs[0].size() ? backoffs[0](pos, 1) : logprob_type(0.0));	  
	  os << (logprob * factor_log_10) << ' ' << (logbound * factor_log_10) << '\t' << vocab_map[id];
	  if (backoff != 0.0)
	    os << '\t' << (backoff * factor_log_10);
	  os << '\n';
	}
      }
      
    } else {
      os << "\\1-grams:" << '\n';
      
      if (! has_unk)
	os << (smooth * factor_log_10) << '\t' << vocab_type::UNK << '\n';

      for (size_type pos = 0; pos < index[0].offsets[1]; ++ pos) {
	logprob_type logprob = logprobs[0](pos, 1);
	
	// escape logprob-min...
	if (logprob == logprob_min() && pos == bos_id)
	  logprob = logprob_srilm_min;
	
	if (logprob != logprob_min()) {
	  const id_type id(pos);
	  
	  if (id >= vocab_map.size())
	    vocab_map.resize(id + 1, utils::piece());
	  if (vocab_map[id].empty())
	    vocab_map[id] = index.vocab()[id];
	  
	  const logprob_type backoff = (pos < backoffs[0].size() ? backoffs[0](pos, 1) : logprob_type(0.0));	  
	  os << (logprob * factor_log_10) << '\t' << vocab_map[id];
	  if (backoff != 0.0)
	    os << '\t' << (backoff * factor_log_10);
	  os << '\n';
	}
      }
    }
    
    // ngrams...
    pqueue_type pqueue;
    for (size_type shard = 0; shard != index.size(); ++ shard) {
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
    
    typedef std::vector<utils::piece, std::allocator<utils::piece> > phrase_type;

    phrase_type phrase;
    int order = 1;
    while (! pqueue.empty()) {
      context_words_queue_ptr_type context_queue(pqueue.top());
      pqueue.pop();
      
      if (static_cast<int>(context_queue->first.size()) + 1 != order) {
	order = context_queue->first.size() + 1;
	os << '\n';
	os << "\\" << order << "-grams:" << '\n';
      }
      
      phrase.clear();
      context_type::const_iterator citer_end = context_queue->first.end();
      for (context_type::const_iterator citer = context_queue->first.begin(); citer != citer_end; ++ citer) {
	if (*citer >= vocab_map.size())
	  vocab_map.resize(*citer + 1, utils::piece());
	if (vocab_map[*citer].empty())
	  vocab_map[*citer] = index.vocab()[*citer];
	
	phrase.push_back(vocab_map[*citer]);
      }

      if (lower && ! logbounds.empty() && order < index.order()) {
	word_set_type::const_iterator witer_end = context_queue->second.first.end();
	for (word_set_type::const_iterator witer = context_queue->second.first.begin(); witer != witer_end; ++ witer) {
	  const id_type id = witer->first;
	  const logprob_type& logprob  = witer->second.logprob;
	  const logprob_type& logbound = witer->second.logbound;
	  const logprob_type& backoff  = witer->second.backoff;
	  
	  if (id >= vocab_map.size())
	    vocab_map.resize(id + 1, utils::piece());
	  if (vocab_map[id].empty())
	    vocab_map[id] = index.vocab()[id];
	  
	  os << (logprob * factor_log_10) << ' ' << (logbound * factor_log_10) << '\t';
	  
	  if (index.backward()) {
	    os << vocab_map[id];
	    phrase_type::const_reverse_iterator piter_end = phrase.rend();
	    for (phrase_type::const_reverse_iterator piter = phrase.rbegin(); piter != piter_end; ++ piter)
	      os << ' ' << *piter;
	  } else {
	    std::copy(phrase.begin(), phrase.end(), std::ostream_iterator<utils::piece>(os, " "));
	    os << vocab_map[id];
	  }
	  
	  if (backoff != 0.0)
	    os << '\t' << (backoff * factor_log_10);
	  os << '\n';
	}
      } else {
	word_set_type::const_iterator witer_end = context_queue->second.first.end();
	for (word_set_type::const_iterator witer = context_queue->second.first.begin(); witer != witer_end; ++ witer) {
	  const id_type id = witer->first;
	  const logprob_type& logprob  = witer->second.logprob;
	  const logprob_type& backoff  = witer->second.backoff;
	  
	  if (id >= vocab_map.size())
	    vocab_map.resize(id + 1, utils::piece());
	  if (vocab_map[id].empty())
	    vocab_map[id] = index.vocab()[id];
	  
	  os << (logprob * factor_log_10) << '\t';
	  
	  if (index.backward()) {
	    os << vocab_map[id];
	    phrase_type::const_reverse_iterator piter_end = phrase.rend();
	    for (phrase_type::const_reverse_iterator piter = phrase.rbegin(); piter != piter_end; ++ piter)
	      os << ' ' << *piter;
	  } else {
	    std::copy(phrase.begin(), phrase.end(), std::ostream_iterator<utils::piece>(os, " "));
	    os << vocab_map[id];
	  }
	  
	  if (backoff != 0.0)
	    os << '\t' << (backoff * factor_log_10);
	  os << '\n';
	}
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
    
    // join all the threads!
    for (size_type shard = 0; shard != threads.size(); ++ shard)
      threads[shard]->join();
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
    typedef expgram::Vocab vocab_type;

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

      // TODO: revise this so that: we compute upper bounds for those with ngram with BOS context
      //       and the ngram with "real" probabilities, not weight after backoff.
      
      const id_type bos_id = ngram.index.vocab()[vocab_type::BOS];
      
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

#if 1
	  // for lower order, we will use only the ngram starting with <s>
	  // since we are in the backward mode, check the last of trie
	  if (ngram.index.backward() && order_prev + 1 != ngram.index.order() && ngram.index[shard][pos_context] != bos_id)
	    continue;
#endif
	  
	  context_type::iterator citer_curr = context.end() - 2;
	  for (size_type pos_curr = pos_context; pos_curr != size_type(-1); pos_curr = ngram.index[shard].parent(pos_curr), -- citer_curr)
	    *citer_curr = ngram.index[shard][pos_curr];
	  
#if 1
	  // for lower order, we will use only the ngram starting with <s>
	  // since we are in the forward mode, check the first of trie
	  if (! ngram.index.backward() && order_prev + 1 != ngram.index.order() && context.front() != bos_id)
	    continue;
#endif
	  
	  for (size_type pos = pos_first; pos != pos_last; ++ pos) {
	    context.back() = ngram.index[shard][pos];
	    
	    const logprob_type logprob = ngram.logprobs[shard](pos, order_prev + 1);
	    if (logprob != ngram.logprob_min()) {
#if 1
	      context_type::const_iterator citer_end   = context.end() - ngram.index.backward();
	      context_type::const_iterator citer_begin = context.begin() + (!ngram.index.backward());
	      
	      for (context_type::const_iterator citer = citer_begin; citer != citer_end; ++ citer) {
		if (citer_end - citer == 1)
		  unigrams[*citer] = std::max(unigrams[*citer], logprob);
		else {
		  const size_type shard_index = ngram.index.shard_index(citer, citer_end);
		  
		  queues[shard_index]->push(std::make_pair(context_type(citer, citer_end), logprob));
		}
	      }
#endif

#if 0
	      context_type::const_iterator citer_end   = context.end() - ngram.index.backward();
	      context_type::const_iterator citer_begin = context.begin() + (!ngram.index.backward());
	      if (citer_end - citer_begin == 1)
		unigrams[*citer_begin] = std::max(unigrams[*citer_begin], logprob);
	      else {
		const size_type shard_index = ngram.index.shard_index(citer_begin, citer_end);
		
		queues[shard_index]->push(std::make_pair(context_type(citer_begin, citer_end), logprob));
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
      
      for (size_type shard = 0; shard != queues.size(); ++ shard)
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
	
	context_type& context = context_logprob.first;
	
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
      
      while (! boost::filesystem::exists(path))
	boost::thread::yield();

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

    if (debug)
      std::cerr << "estimate upper bounds: shard = " << index.size() << std::endl;
    
    queue_ptr_set_type  queues(index.size());
    thread_ptr_set_type threads_mapper(index.size());
    thread_ptr_set_type threads_reducer(index.size());
    
    // first, run reducer...
    logbounds.clear();
    logbounds.resize(index.size());
    for (size_type shard = 0; shard != logbounds.size(); ++ shard) {
      logbounds[shard].offset = logprobs[shard].offset;
      
      queues[shard].reset(new queue_type(1024 * 64));
      threads_reducer[shard].reset(new thread_type(reducer_type(*this, *queues[shard], shard, debug)));
    }
    
    // second, mapper...
    for (size_type shard = 0; shard != logbounds.size(); ++ shard)
      threads_mapper[shard].reset(new thread_type(mapper_type(*this, queues, shard, debug)));
    
    // termination...
    for (size_type shard = 0; shard != logbounds.size(); ++ shard)
      threads_mapper[shard]->join();
    for (size_type shard = 0; shard != logbounds.size(); ++ shard)
      threads_reducer[shard]->join();
  }
  
  struct NGramBackwardMapReduce
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
      
      void swap(logprob_set_type& x)
      {
	std::swap(prob,    x.prob);
	std::swap(bound,   x.bound);
	std::swap(backoff, x.backoff);
      }
    };
    
    typedef std::pair<context_type, logprob_set_type>     context_logprob_set_type;
    
    typedef boost::thread                                                  thread_type;
    typedef boost::shared_ptr<thread_type>                                 thread_ptr_type;
    typedef std::vector<thread_ptr_type, std::allocator<thread_ptr_type> > thread_ptr_set_type;
    
    typedef utils::lockfree_list_queue<context_logprob_set_type, std::allocator<context_logprob_set_type> > queue_type;
    typedef boost::shared_ptr<queue_type>                                                                   queue_ptr_type;
    typedef std::vector<queue_ptr_type, std::allocator<queue_ptr_type> >                                    queue_ptr_set_type;

    typedef boost::iostreams::filtering_ostream                              ostream_type;
    typedef boost::shared_ptr<ostream_type>                                  ostream_ptr_type;
    typedef std::vector<ostream_ptr_type, std::allocator<ostream_ptr_type> > ostream_ptr_set_type;
    
    struct shard_type
    {
      ostream_type os_logprob;
      ostream_type os_logbound;
      ostream_type os_backoff;
      
      path_type path_logprob;
      path_type path_logbound;
      path_type path_backoff;
      
      // we allow no copying!
      shard_type() {}
      shard_type(const shard_type& x) {}
      shard_type& operator=(const shard_type& x) { return *this; }
    };
    
    typedef std::vector<shard_type, std::allocator<shard_type> > shard_set_type;
  };

  inline
  void swap(NGramBackwardMapReduce::context_logprob_set_type& x,
	    NGramBackwardMapReduce::context_logprob_set_type& y)
  {
    x.first.swap(y.first);
    x.second.swap(y.second);
  }

  
  struct NGramBackwardMapper : public NGramBackwardMapReduce
  {
    NGramBackwardMapper(const ngram_type&   _ngram,
			queue_ptr_set_type& _queues,
			const int           _shard,
			const int           _debug)
      : ngram(_ngram),
	queues(_queues),
	shard(_shard),
	debug(_debug) {}
    
    const ngram_type&   ngram;
    queue_ptr_set_type& queues;
    int                 shard;
    int                 debug;

    void operator()()
    {
      const size_type shard_size = ngram.index.size();
      
      std::vector<queue_type*, std::allocator<queue_type*> > queues_shard(shard_size);
      for (size_type shard_index = 0; shard_index != shard_size; ++ shard_index)
	queues_shard[shard_index] = &(*queues[shard_index]);
      
      context_type context;

      size_type order_max = ngram.index.order();
      for (size_type order_prev = 1; order_prev < order_max; ++ order_prev) {
	const size_type pos_context_first = ngram.index[shard].offsets[order_prev - 1];
	const size_type pos_context_last  = ngram.index[shard].offsets[order_prev];
	
	if (debug)
	  std::cerr << "shard: " << shard << " order: " << (order_prev + 1) << std::endl;
	
	const size_type context_size = order_prev + 1;
	
	context.resize(context_size);
	
	size_type pos_last_prev = pos_context_last;
	for (size_type pos_context = pos_context_first; pos_context < pos_context_last; ++ pos_context) {
	  const size_type pos_first = pos_last_prev;
	  const size_type pos_last = ngram.index[shard].children_last(pos_context);
	  pos_last_prev = pos_last;
	  
	  if (pos_first == pos_last) continue;
	  
	  // in the forward mode, if we traverse backward, and store context in left-to-right, it is inversed!
	  context_type::iterator citer_curr = context.begin() + 1;
	  for (size_type pos_curr = pos_context; pos_curr != size_type(-1); pos_curr = ngram.index[shard].parent(pos_curr), ++ citer_curr)
	    *citer_curr = ngram.index[shard][pos_curr];
	  
	  for (size_type pos = pos_first; pos != pos_last; ++ pos) {
	    context.front() = ngram.index[shard][pos];
	    
	    const logprob_type prob    = ngram.logprobs[shard](pos, context_size);
	    const logprob_type bound   = (context_size != order_max
					  ? ngram.logbounds[shard](pos, context_size)
					  : ngram.logprob_min());
	    const logprob_type backoff = (context_size != order_max
					  ? ngram.backoffs[shard](pos, context_size)
					  : ngram.logprob_min());
	    
	    // context is already reversed!
	    const size_type shard_index = ngram.index.shard_index(context.begin(), context.end());
	    
	    queues_shard[shard_index]->push(std::make_pair(context, logprob_set_type(prob, bound, backoff)));
	  }
	}
      }
      
      // termination...
      for (size_type shard_index = 0; shard_index != shard_size; ++ shard_index)
	queues_shard[shard_index]->push(std::make_pair(context_type(), logprob_set_type()));
    }
  };
  
  struct NGramBackwardReducer : public NGramBackwardMapReduce
  {
    typedef std::pair<id_type, logprob_set_type>                                       word_logprob_set_type;
    typedef std::vector<word_logprob_set_type, std::allocator<word_logprob_set_type> > word_logprob_map_type;
    
    typedef utils::packed_vector<id_type, std::allocator<id_type> >  id_vector_type;
    typedef std::vector<logprob_type, std::allocator<logprob_type> > logprob_vector_type;
    typedef std::vector<size_type, std::allocator<size_type> >       size_vector_type;

    NGramBackwardReducer(ngram_type&   _ngram,
			 queue_type&    _queue,
			 shard_type&         _shard_data,
			 const int           _shard,
			 const int           _max_order,
			 const int           _debug)
      : ngram(_ngram),
	queue(_queue),
	shard_data(_shard_data),
	shard(_shard),
	max_order(_max_order),
	debug(_debug) {}
    
    ngram_type&         ngram;
    queue_type&         queue;
    shard_type&         shard_data;
    int                 shard;
    int                 max_order;
    int                 debug;
    
    // local
    id_vector_type      ids;
    logprob_vector_type logprobs;
    logprob_vector_type logbounds;
    logprob_vector_type backoffs;
    size_vector_type    positions_first;
    size_vector_type    positions_last;
    
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
      
      while (first != last) {
	Iterator next = std::min(first + 1024 * 1024, last);
	os.write((char*) &(*first), (next - first) * sizeof(value_type));
	first = next;
      }
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
	  dump(shard_data.os_logprob, logprobs.begin() + pos_first, logprobs.begin() + pos_last);
	  
	  if (order_prev + 1 != max_order) {
	    dump(shard_data.os_logbound, logbounds.begin() + pos_first, logbounds.begin() + pos_last);
	    dump(shard_data.os_backoff,  backoffs.begin() + pos_first,  backoffs.begin() + pos_last);
	  }
	  
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
      logprobs.clear();
      logbounds.clear();
      backoffs.clear();
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
      
      std::sort(words.begin(), words.end(), less_first<word_logprob_set_type>());
      
      word_logprob_map_type::const_iterator witer_end = words.end();
      for (word_logprob_map_type::const_iterator witer = words.begin(); witer != witer_end; ++ witer) {
	ids.push_back(witer->first);
	
	logprobs.push_back(witer->second.prob);
	
	if (order_prev + 1 != max_order) {
	  logbounds.push_back(witer->second.bound);
	  backoffs.push_back(witer->second.backoff != ngram_type::logprob_min() ? witer->second.backoff : logprob_type(0.0));
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

      // perform final indexing
      if (! words.empty()) {
	index_ngram(prefix, words);
	words.clear();
	index_ngram();
      }
    }
    
    struct ngram_data_type
    {
      path_type    path;
      ostream_type os;
      
      ngram_data_type() {}
      ngram_data_type(const ngram_data_type& ) {}
      ngram_data_type& operator=(const ngram_data_type& x) { return *this; }
    };
    typedef std::vector<ngram_data_type, std::allocator<ngram_data_type> > ngram_data_set_type;
    

    void operator()()
    {
      // revise this implementation....!!!!!
      // since bigram will not be re-distributed, this code will not work...!!!
      
      // keep the files for each order
      // put them all!
      // when finished,
      // perform indexing starting from bigram.....

      const size_type shard_size = ngram.index.size();
      const path_type tmp_dir = utils::tempfile::tmp_dir();

      ngram_data_set_type ngrams(ngram.index.order() + 1);
      
      for (int order = 2; order <= ngram.index.order(); ++ order) {
	const path_type path = utils::tempfile::file_name(tmp_dir / "expgram.ngram.XXXXXX");

	utils::tempfile::insert(path);
	
	ngrams[order].path = path;
	ngrams[order].os.push(boost::iostreams::file_sink(path.string(), std::ios_base::out | std::ios_base::trunc), 1024 * 1024);
	ngrams[order].os.exceptions(std::ostream::eofbit | std::ostream::failbit | std::ostream::badbit);
      }

      context_logprob_set_type context_logprobs;
      size_type terminated = 0;
      
      while (terminated != shard_size) {
	queue.pop_swap(context_logprobs);
	
	const context_type&     context  = context_logprobs.first;
	const logprob_set_type& logprobs = context_logprobs.second;

	if (context.empty()) {
	  ++ terminated;
	  continue;
	}
	
	const int order = context.size();
	
	ngrams[order].os.write((char*) &(*context.begin()), sizeof(context_type::value_type) * order);
	ngrams[order].os.write((char*) &logprobs, sizeof(logprob_set_type));
      }
      
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
  };

  void NGram::backward()
  {
    typedef NGramBackwardMapReduce map_reduce_type;
    typedef NGramBackwardMapper    mapper_type;
    typedef NGramBackwardReducer   reducer_type;

    typedef map_reduce_type::thread_type          thread_type;
    typedef map_reduce_type::thread_ptr_set_type  thread_ptr_set_type;
    typedef map_reduce_type::queue_type           queue_type;
    typedef map_reduce_type::queue_ptr_set_type   queue_ptr_set_type;
    typedef map_reduce_type::ostream_type         ostream_type;
    typedef map_reduce_type::ostream_ptr_set_type ostream_ptr_set_type;
    typedef map_reduce_type::path_set_type        path_set_type;
    
    typedef map_reduce_type::shard_type     shard_type;
    typedef map_reduce_type::shard_set_type shard_set_type;
        
    if (index.backward()) return;
    if (index.empty()) return;

    // we make sure that we have computed upper bounds...no...
    bounds();
    
    NGram ngram;

    const size_type shard_size = index.size();

    if (debug)
      std::cerr << "estimate backward trie: shard = " << shard_size << std::endl;
    
    ngram.index.reserve(shard_size);
    ngram.index.resize(shard_size);
    ngram.index.vocab() = index.vocab();
    ngram.index.order() = index.order();
    ngram.index.backward() = true;
    
    ngram.logprobs.reserve(shard_size);
    ngram.logbounds.reserve(shard_size);
    ngram.backoffs.reserve(shard_size);
    
    ngram.logprobs.resize(shard_size);
    ngram.logbounds.resize(shard_size);
    ngram.backoffs.resize(shard_size);
    
    ngram.smooth = smooth;
    ngram.debug  = debug;
    
    // setup paths and streams for logprob/backoff
    const path_type tmp_dir = utils::tempfile::tmp_dir();

    shard_set_type shards(shard_size);
    
    for (size_type shard = 0; shard != shard_size; ++ shard) {
      shards[shard].path_logprob  = utils::tempfile::file_name(tmp_dir / "expgram.logprob.XXXXXX");
      shards[shard].path_logbound = utils::tempfile::file_name(tmp_dir / "expgram.logbound.XXXXXX");
      shards[shard].path_backoff  = utils::tempfile::file_name(tmp_dir / "expgram.backoff.XXXXXX");
      
      utils::tempfile::insert(shards[shard].path_logprob);
      utils::tempfile::insert(shards[shard].path_logbound);
      utils::tempfile::insert(shards[shard].path_backoff);
      
      shards[shard].os_logprob.push(boost::iostreams::file_sink(shards[shard].path_logprob.string()),   1024 * 1024);
      shards[shard].os_logbound.push(boost::iostreams::file_sink(shards[shard].path_logbound.string()), 1024 * 1024);
      shards[shard].os_backoff.push(boost::iostreams::file_sink(shards[shard].path_backoff.string()),   1024 * 1024);
    }
    
    // handling unigram...!
    const size_type unigram_size = index.ngram_size(1);
    
    for (size_type pos = 0; pos != unigram_size; ++ pos) {
      const logprob_type logprob  = logprobs[0](pos, 1);
      const logprob_type logbound = logbounds[0](pos, 1);
      const logprob_type backoff  = backoffs[0](pos, 1);
      
      shards[0].os_logprob.write((char*) &logprob,   sizeof(logprob_type));
      shards[0].os_logbound.write((char*) &logbound, sizeof(logprob_type));
      shards[0].os_backoff.write((char*) &backoff,   sizeof(logprob_type));
    }
    
    for (size_type shard = 0; shard != shard_size; ++ shard) {
      ngram.index[shard].offsets.clear();
      ngram.index[shard].offsets.push_back(0);
      ngram.index[shard].offsets.push_back(unigram_size);
      
      ngram.logprobs[shard].offset  = utils::bithack::branch(shard == 0, size_type(0), unigram_size);
      ngram.logbounds[shard].offset = utils::bithack::branch(shard == 0, size_type(0), unigram_size);
      ngram.backoffs[shard].offset  = utils::bithack::branch(shard == 0, size_type(0), unigram_size);
    }
    
    // start bigram, trigram etc.
    
    // first, we will prepare queues, mappers and reducers
    queue_ptr_set_type  queues(shard_size);
    boost::thread_group mappers;
    boost::thread_group reducers;
    
    for (size_type i = 0; i != shard_size; ++ i)
      queues[i].reset(new queue_type(1024 * 64));
    
    // first, reducer...
    for (size_type shard = 0; shard != shard_size; ++ shard)
      reducers.add_thread(new thread_type(reducer_type(ngram,
						       *queues[shard],
						       shards[shard],
						       shard,
						       index.order(),
						       debug)));
    // second, mapper...
    for (size_type shard = 0; shard != shard_size; ++ shard)
      mappers.add_thread(new thread_type(mapper_type(*this, queues, shard, debug)));
    
    // termination...
    mappers.join_all();
    reducers.join_all();

    if (debug)
      for (size_type shard = 0; shard != shard_size; ++ shard)
	std::cerr << "shard: " << shard
		  << " logprob: " << ngram.logprobs[shard].size()
		  << " logbound: " << ngram.logbounds[shard].size()
		  << " backoff: " << ngram.backoffs[shard].size()
		  << std::endl;
    
    ngram.swap(*this);
  }

  static const std::string& __BOS = static_cast<const std::string&>(Vocab::BOS);
  static const std::string& __EOS = static_cast<const std::string&>(Vocab::EOS);
  static const std::string& __UNK = static_cast<const std::string&>(Vocab::UNK);
  
  inline
  NGram::word_type escape_word(const utils::piece& __word)
  {
    const utils::ipiece word(__word);
    
    if (word == __BOS)
      return Vocab::BOS;
    else if (word == __EOS)
      return Vocab::EOS;
    else if (word == __UNK)
      return Vocab::UNK;
    else
      return __word;
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
    
    struct shard_type
    {
      ostream_type os_logprob;
      ostream_type os_backoff;
      
      path_type path_logprob;
      path_type path_backoff;
      
      // we allow no copying!
      shard_type() {}
      shard_type(const shard_type& x) {}
      shard_type& operator=(const shard_type& x) { return *this; }
    };
    
    typedef std::vector<shard_type, std::allocator<shard_type> > shard_set_type;
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

    typedef map_reduce_type::shard_type     shard_type;
    typedef map_reduce_type::shard_set_type shard_set_type;

    
    typedef std::pair<id_type, logprob_pair_type>                                        word_logprob_pair_type;
    typedef std::vector<word_logprob_pair_type, std::allocator<word_logprob_pair_type> > word_logprob_pair_set_type;
    
    typedef utils::packed_vector<id_type, std::allocator<id_type> >  id_set_type;
    typedef std::vector<logprob_type, std::allocator<logprob_type> > logprob_set_type;
    typedef std::vector<size_type, std::allocator<size_type> >       size_set_type;
    
    ngram_type&   ngram;
    queue_type&   queue;
    shard_type&   shard_data;
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
		      shard_type&   _shard_data,
		      const int     _shard,
		      const int     _max_order,
		      const int     _debug)
      : ngram(_ngram),
	queue(_queue),
	shard_data(_shard_data),
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
      
      while (first != last) {
	Iterator next = std::min(first + 1024 * 1024, last);
	os.write((char*) &(*first), (next - first) * sizeof(value_type));
	first = next;
      }
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
	  dump(shard_data.os_logprob, logprobs.begin() + pos_first, logprobs.begin() + pos_last);
	  
	  if (order_prev + 1 != max_order)
	    dump(shard_data.os_backoff, backoffs.begin() + pos_first, backoffs.begin() + pos_last);
	  
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

      // we need to make sure that this prefix is traversed in backward order!

      std::pair<context_type::const_iterator, size_type> result = ngram.index.traverse(shard, prefix.begin(), prefix.end());
      if (result.first != prefix.end() || result.second == size_type(-1)) {
	std::ostringstream stream;
	
	stream << "No prefix:";
	context_type::const_iterator citer_end = prefix.end();
	for (context_type::const_iterator citer = prefix.begin(); citer != citer_end; ++ citer)
	  stream << ' '  << ngram.index.vocab()[*citer];

	throw std::runtime_error(stream.str());
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
	  backoffs.push_back(witer->second.second == ngram.logprob_min() ? 0.0 : witer->second.second);
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

      const size_type offset = sizeof(logprob_pair_type) / sizeof(id_type);

      if (offset != 2 || sizeof(logprob_pair_type) % sizeof(id_type) != 0)
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
      word_logprob_pair_set_type words;

      size_type inserted = false;
      
      ngram_set_type::const_iterator niter_end = ngrams.end();
      for (ngram_set_type::const_iterator niter = ngrams.begin(); niter != niter_end; ++ niter) {
	const id_type* context = *niter;
	const id_type word = *(context + order - 1);
	const logprob_pair_type& logprobs = *reinterpret_cast<const logprob_pair_type*>(context + order);
	
	if (prefix.empty() || ! std::equal(prefix.begin(), prefix.end(), context)) {
	  if (! words.empty()) {
	    index_ngram(prefix, words);
	    ++ inserted;
	    words.clear();
	  }
	  
	  prefix.clear();
	  prefix.insert(prefix.end(), context, context + order - 1);
	}
	
	if (words.empty() || words.back().first != word)
	  words.push_back(std::make_pair(word, logprobs));
	else {
	  if (words.back().second.first == ngram_type::logprob_min())
	    words.back().second.first = logprobs.first;
	  if (words.back().second.second == ngram_type::logprob_min())
	    words.back().second.second = logprobs.second;
	}
      }
      
      // perform final indexing
      if (! words.empty()) {
	index_ngram(prefix, words);
	++ inserted;
	words.clear();
	
	index_ngram();
      }
    }

    struct ngram_data_type
    {
      path_type    path;
      ostream_type os;
      
      ngram_data_type() {}
      ngram_data_type(const ngram_data_type& ) {}
      ngram_data_type& operator=(const ngram_data_type& x) { return *this; }
    };
    typedef std::vector<ngram_data_type, std::allocator<ngram_data_type> > ngram_data_set_type;

    void operator()()
    {
      const path_type tmp_dir = utils::tempfile::tmp_dir();
      
      ngram_data_set_type ngrams(max_order + 1);

      for (int order = 2; order <= max_order; ++ order) {
	const path_type path = utils::tempfile::file_name(tmp_dir / "expgram.ngram.XXXXXX");

	utils::tempfile::insert(path);
	
	ngrams[order].path = path;
	ngrams[order].os.push(boost::iostreams::file_sink(path.string(), std::ios_base::out | std::ios_base::trunc), 1024 * 1024);
	ngrams[order].os.exceptions(std::ostream::eofbit | std::ostream::failbit | std::ostream::badbit);
      }
            
      const logprob_pair_type logprobs_min(ngram.logprob_min(), ngram.logprob_min());
	
      // we will start from bigram... unigrams...
	
      context_logprob_pair_type context_logprob_pair;

      while (1) {
	queue.pop(context_logprob_pair);
	if (context_logprob_pair.first.empty()) break;
	  
	context_type&      context  = context_logprob_pair.first;
	logprob_pair_type& logprobs = context_logprob_pair.second;
	
	if (context.size() >= ngrams.size())
	  throw std::runtime_error("invalid ngram context size");
	
	// reverse context!
	std::reverse(context.begin(), context.end());
	  
	// TODO: this is spurious.... Can we eliminate this...?
	// insert pseudo context...
	for (size_type order = 2; order < context.size(); ++ order) {
	  ngrams[order].os.write((char*) &(*context.begin()), sizeof(context_type::value_type) * order);
	  ngrams[order].os.write((char*) &logprobs_min, sizeof(logprob_pair_type));
	}
	
	// insert actual context...
	ngrams[context.size()].os.write((char*) &(*context.begin()), sizeof(context_type::value_type) * context.size());
	ngrams[context.size()].os.write((char*) &logprobs, sizeof(logprob_pair_type));
      }
	
      for (size_type order = 2; order != ngrams.size(); ++ order) {
	if (debug)
	  std::cerr << "indexing: shard=" << shard << " order=" << order << std::endl;
	
	ngrams[order].os.reset();
	
	index_ngram(ngrams[order].path, order);
	
	boost::filesystem::remove(ngrams[order].path);
	utils::tempfile::erase(ngrams[order].path);
      }
	
      // finalization...
      shard_data.os_logprob.reset();
      shard_data.os_backoff.reset();
	
      while (! boost::filesystem::exists(shard_data.path_logprob))
	boost::thread::yield();
      while (! boost::filesystem::exists(shard_data.path_backoff))
	boost::thread::yield();
	
      utils::tempfile::permission(shard_data.path_logprob);
      utils::tempfile::permission(shard_data.path_backoff);
	
      ngram.logprobs[shard].logprobs.open(shard_data.path_logprob);
      ngram.backoffs[shard].logprobs.open(shard_data.path_backoff);
    }
  };
  
  template <typename Tp>
  struct greater_second_first
  {
    bool operator()(const Tp& x, const Tp& y) const
    {
      return x.second.first > y.second.first || (!(y.second.first > x.second.first) && x.first < y.first);
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
    
    
    typedef std::vector<utils::piece, std::allocator<utils::piece> > tokens_type;
    typedef boost::tokenizer<utils::space_separator, utils::piece::const_iterator, utils::piece> tokenizer_type;
    
    typedef std::pair<logprob_type, logprob_type>                                        logprob_pair_type;
    typedef std::pair<word_type, logprob_pair_type>                                      word_logprob_pair_type;
    typedef std::vector<word_logprob_pair_type, std::allocator<word_logprob_pair_type> > word_logprob_pair_set_type;
    
    typedef std::vector<id_type, std::allocator<id_type> > vocab_map_type;

    typedef map_reduce_type::shard_type     shard_type;
    typedef map_reduce_type::shard_set_type shard_set_type;
    
    // setup structures...
    clear();
    
    if (path != "-" && ! boost::filesystem::exists(path))
      throw std::runtime_error("no file? " + path.string());

    index.reserve(shard_size);
    logprobs.reserve(shard_size);
    backoffs.reserve(shard_size);
    
    index.resize(shard_size);
    logprobs.resize(shard_size);
    backoffs.resize(shard_size);
    
    // setup paths and streams for logprob/backoff
    const path_type tmp_dir = utils::tempfile::tmp_dir();
    
    shard_set_type shards(shard_size);
    
    ostream_ptr_set_type os_logprobs(shard_size);
    ostream_ptr_set_type os_backoffs(shard_size);
    path_set_type        path_logprobs(shard_size);
    path_set_type        path_backoffs(shard_size);
    
    for (size_type shard = 0; shard != shard_size; ++ shard) {
      shards[shard].path_logprob = utils::tempfile::file_name(tmp_dir / "expgram.logprob.XXXXXX");
      shards[shard].path_backoff = utils::tempfile::file_name(tmp_dir / "expgram.backoff.XXXXXX");
      
      utils::tempfile::insert(shards[shard].path_logprob);
      utils::tempfile::insert(shards[shard].path_backoff);
      
      shards[shard].os_logprob.push(boost::iostreams::file_sink(shards[shard].path_logprob.string()), 1024 * 1024);
      shards[shard].os_backoff.push(boost::iostreams::file_sink(shards[shard].path_backoff.string()), 1024 * 1024);
    }
    
    typedef enum {
      NONE,
      END,
      DATA,
      NGRAMS,
    } mode_type;
    
    const double log_10 = M_LN10;
    
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
      utils::piece line_piece(line);
      tokenizer_type tokenized(line_piece);
      
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
	  max_order = std::max(max_order, utils::lexical_cast<int>(tokens[1].substr(0, pos)));
      }
      
      if (order == 0 || mode != NGRAMS) continue;

      if (tokens.size() < 2) continue;
      
      const logprob_type logprob = utils::lexical_cast<double>(tokens.front()) * log_10;
      const logprob_type logbackoff = (static_cast<int>(tokens.size()) == order + 2 ? (utils::lexical_cast<double>(tokens.back()) * log_10) : 0.0);
      
      
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
	
	shards[0].os_logprob.write((char*) &witer->second.first, sizeof(logprob_type));
	shards[0].os_backoff.write((char*) &witer->second.second, sizeof(logprob_type));
	
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

      for (size_type shard = 0; shard != index.size(); ++ shard) {
	index[shard].offsets.clear();
	index[shard].offsets.push_back(0);
	index[shard].offsets.push_back(unigram_size);
	
	logprobs[shard].offset = utils::bithack::branch(shard == 0, size_type(0), unigram_size);
	backoffs[shard].offset = utils::bithack::branch(shard == 0, size_type(0), unigram_size);
      }
      
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
    for (size_type shard = 0; shard != shard_size; ++ shard) {
      queues[shard].reset(new queue_type(1024 * 64));
      threads[shard].reset(new thread_type(reducer_type(*this, *queues[shard], shards[shard], shard, max_order, debug)));
    }
    
    context_type context;
    
    while (std::getline(is, line)) {
      utils::piece line_piece(line);
      tokenizer_type tokenized(line_piece);
      
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

      if (static_cast<int>(tokens.size()) < order + 1) continue;
      
      const logprob_type logprob = utils::lexical_cast<double>(tokens.front()) * log_10;
      const logprob_type logbackoff = (static_cast<int>(tokens.size()) == order + 2 ? (utils::lexical_cast<double>(tokens.back()) * log_10) : 0.0);
      
      context.clear();
      
      tokens_type::const_iterator titer_begin = tokens.begin() + 1;
      tokens_type::const_iterator titer_end = titer_begin + order;
      for (tokens_type::const_iterator titer = titer_begin; titer != titer_end; ++ titer) {
	const id_type id = escape_word(*titer).id();
	
	if (id >= vocab_map.size() || vocab_map[id] == id_type(-1))
	  throw std::runtime_error("invalid vocbulary");
	
	context.push_back(vocab_map[id]);
      }
      
      // backward sharding...
      const size_type shard = index.shard_index(context[context.size() - 1], context[context.size() - 2]);
      queues[shard]->push(std::make_pair(context, std::make_pair(logprob, logbackoff)));
    }
    
    // termination...
    for (size_type shard = 0; shard != index.size(); ++ shard)
      queues[shard]->push(std::make_pair(context_type(), std::make_pair(0.0, 0.0)));
    
    for (size_type shard = 0; shard != index.size(); ++ shard) {
      threads[shard]->join();

      if (debug)
	std::cerr << "ngram shard: " << shard
		  << " logprob: " << logprobs[shard].size()
		  << " backoff: " << backoffs[shard].size()
		  << std::endl;
    }
    threads.clear();

    // set backward indexing!
    index.backward() = true;
    
    // compute upper bounds...
    bounds();
  }
};
