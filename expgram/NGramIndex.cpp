//
//  Copyright(C) 2009-2012 Taro Watanabe <taro.watanabe@nict.go.jp>
//

#include <iostream>
#include <sstream>
#include <iomanip>

#include "NGramIndex.hpp"

#include <boost/thread.hpp>

#include "utils/lexical_cast.hpp"

namespace expgram
{

  void NGramIndex::Shard::clear_cache()
  {
    //
    // minimum size is 1024 * 64
    //
    
    const size_type cache_size = utils::bithack::max(size_type(utils::bithack::next_largest_power2(ids.size()) >> 11),
						     size_type(1024 * 64));
    
    caches_pos.clear();
    caches_suffix.clear();
    
    caches_pos.reserve(cache_size);
    caches_suffix.reserve(cache_size);
    
    caches_pos.resize(cache_size);
    caches_suffix.resize(cache_size);
  }

  void NGramIndex::Shard::open(const path_type& path)
  {
    typedef utils::repository repository_type;
      
    clear();
      
    repository_type rep(path, repository_type::read);
      
    ids.open(rep.path("index"));
    positions.open(rep.path("position"));
      
    repository_type::const_iterator oiter = rep.find("order");
    if (oiter == rep.end())
      throw std::runtime_error("no order");
    const int order = utils::lexical_cast<int>(oiter->second);
      
    offsets.push_back(0);
    for (int n = 1; n <= order; ++ n) {
      std::ostringstream stream_ngram;
      stream_ngram << n << "-gram-offset";
	
      repository_type::const_iterator iter = rep.find(stream_ngram.str());
      if (iter == rep.end())
	throw std::runtime_error(std::string("no ngram offset? ") + stream_ngram.str());
      
      offsets.push_back(utils::lexical_cast<size_type>(iter->second));
    }

    off_set_type(offsets).swap(offsets);
    
    clear_cache();
  }

  void NGramIndex::Shard::write(const path_type& file) const
  {
    typedef utils::repository repository_type;
      
    // do not allow copying to the same file!
    if (path() == file) return;

    repository_type rep(file, repository_type::write);
      
    ids.write(rep.path("index"));
    positions.write(rep.path("position"));
      
    const int order = offsets.size() - 1;
      
    std::ostringstream stream_order;
    stream_order << order;
    rep["order"] = stream_order.str();
      
    for (int n = 1; n <= order; ++ n) {
      std::ostringstream stream_offset;
      std::ostringstream stream_ngram;
	
      stream_offset << offsets[n];
      stream_ngram << n << "-gram-offset";
	
      rep[stream_ngram.str()] = stream_offset.str();
    }
  }
  
  void NGramIndex::Shard::write(const path_type& file, const int write_order) const
  {
    typedef utils::repository repository_type;
      
    // do not allow copying to the same file!
    if (path() == file) return;

    repository_type rep(file, repository_type::write);
    
    const int order = offsets.size() - 1;
    
    if (write_order >= order) {
      ids.write(rep.path("index"));
      positions.write(rep.path("position"));
    } else {
      // we will write subset of ids... How?
      // it is easier to dump ids, but not positions...
      
    }      
    
    std::ostringstream stream_order;
    stream_order << std::min(order, write_order);
    rep["order"] = stream_order.str();
    
    for (int n = 1; n <= std::min(order, write_order); ++ n) {
      std::ostringstream stream_offset;
      std::ostringstream stream_ngram;
      
      stream_offset << offsets[n];
      stream_ngram << n << "-gram-offset";
      
      rep[stream_ngram.str()] = stream_offset.str();
    }
  }
  
  void NGramIndex::open(const path_type& path)
  {
    typedef utils::repository repository_type;

    close();

    if (path.empty())
      throw std::runtime_error("no ngram index?");
    else if (! boost::filesystem::exists(path))
      throw std::runtime_error("no ngram index? " + path.string());
    
    repository_type rep(path, repository_type::read);
    
    // shard size
    repository_type::const_iterator siter = rep.find("shard");
    if (siter == rep.end())
      throw std::runtime_error("no shard size...");
    __shards.resize(utils::lexical_cast<size_t>(siter->second));
    
    // order
    repository_type::const_iterator oiter = rep.find("order");
    if (oiter == rep.end())
      throw std::runtime_error("no order");
    __order = utils::lexical_cast<int>(oiter->second);
    
    // vocabulary...
    __vocab.open(rep.path("vocab"));
    
    for (size_type shard = 0; shard != __shards.size(); ++ shard) {
      std::ostringstream stream_shard;
      stream_shard << "ngram-" << std::setfill('0') << std::setw(6) << shard;
      
      __shards[shard].open(rep.path(stream_shard.str()));
    }
    
    __path = path;
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

  
  void NGramIndex::write(const path_type& file) const
  {
    typedef utils::repository repository_type;

    typedef TaskWriter<path_type, shard_type> task_type;
    typedef task_type::thread_type            thread_type;
    typedef task_type::thread_ptr_set_type    thread_ptr_set_type;
    
    if (! path().empty() && path() == file) return;
    
    {
      repository_type rep(file, repository_type::write);
      std::ostringstream stream_shard;
      std::ostringstream stream_order;
      stream_shard << __shards.size();
      stream_order << __order;
      rep["shard"] = stream_shard.str();
      rep["order"] = stream_order.str();
    }
    
    thread_ptr_set_type threads(__shards.size());
    
    {
      for (size_type shard = 0; shard != __shards.size(); ++ shard) {
	std::ostringstream stream_shard;
	stream_shard << "ngram-" << std::setfill('0') << std::setw(6) << shard;
	
	threads[shard].reset(new thread_type(task_type(file / stream_shard.str(), __shards[shard])));
      }
      
      // main thread will simply dump vocab
      __vocab.write(file / "vocab");
      
      for (size_type shard = 0; shard != __shards.size(); ++ shard)
	threads[shard]->join();
    }
    
    threads.clear();
  }


  void NGramIndex::open_shard(const path_type& path, int shard)
  {
    typedef utils::repository repository_type;
    
    close();
    
    repository_type rep(path, repository_type::read);
    
    // shard size
    repository_type::const_iterator siter = rep.find("shard");
    if (siter == rep.end())
      throw std::runtime_error("no shard size...");
    __shards.resize(utils::lexical_cast<size_t>(siter->second));

    if (shard >= static_cast<int>(__shards.size()))
      throw std::runtime_error("shard is out of range...");
    
    // order
    repository_type::const_iterator oiter = rep.find("order");
    if (oiter == rep.end())
      throw std::runtime_error("no order");
    __order = utils::lexical_cast<int>(oiter->second);
    
    // vocabulary...
    __vocab.open(rep.path("vocab"));
    
    std::ostringstream stream_shard;
    stream_shard << "ngram-" << std::setfill('0') << std::setw(6) << shard;
    
    __shards[shard].open(rep.path(stream_shard.str()));
    
    __path = path;
  }


  void NGramIndex::write_prepare(const path_type& file) const
  {
    typedef utils::repository repository_type;
    
    if (! path().empty() && path() == file) return;
    
    repository_type rep(file, repository_type::write);
    
    __vocab.write(rep.path("vocab"));
    
    std::ostringstream stream_shard;
    std::ostringstream stream_order;
    stream_shard << __shards.size();
    stream_order << __order;
    rep["shard"] = stream_shard.str();
    rep["order"] = stream_order.str();
  }
  
  void NGramIndex::write_shard(const path_type& file, int shard) const
  {
    typedef utils::repository repository_type;
    
    if (! path().empty() && path() == file) return;
    
    while (! boost::filesystem::exists(file))
      boost::thread::yield();
    
    repository_type rep(file, repository_type::read);
    
    std::ostringstream stream_shard;
    stream_shard << "ngram-" << std::setfill('0') << std::setw(6) << shard;
    
    __shards[shard].write(rep.path(stream_shard.str()));
    
    if (shard == 0) {
      std::ostringstream stream_order;
      stream_order << __order;
      rep["order"] = stream_order.str();
    }
  }
};
