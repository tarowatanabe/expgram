
#include <iostream>
#include <sstream>
#include <iomanip>

#include "NGramIndex.hpp"

namespace expgram
{

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
    const int order = atol(oiter->second.c_str());
      
    offsets.push_back(0);
    for (int n = 1; n <= order; ++ n) {
      std::ostringstream stream_ngram;
      stream_ngram << n << "-gram-offset";
	
      repository_type::const_iterator iter = rep.find(stream_ngram.str());
      if (iter == rep.end())
	throw std::runtime_error(std::string("no ngram offset? ") + stream_ngram.str());
	
      offsets.push_back(atoll(iter->second.c_str()));
    }
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
  
  
  void NGramIndex::open(const path_type& path)
  {
    typedef utils::repository repository_type;

    close();
    
    repository_type rep(path, repository_type::read);
    
    // shard size
    repository_type::const_iterator siter = rep.find("shard");
    if (siter == rep.end())
      throw std::runtime_error("no shard size...");
    __shards.resize(atoi(siter->second.c_str()));
    
    // order
    repository_type::const_iterator oiter = rep.find("order");
    if (oiter == rep.end())
      throw std::runtime_error("no order");
    __order = atoi(oiter->second.c_str());
    
    // vocabulary...
    __vocab.open(rep.path("vocab"));
    
    for (int shard = 0; shard < __shards.size(); ++ shard) {
      std::ostringstream stream_shard;
      stream_shard << "ngram-" << std::setfill('0') << std::setw(6) << shard;
      
      __shards[shard].open(rep.path(stream_shard.str()));
    }
    
    __path = path;
  }
  
  void NGramIndex::write(const path_type& file) const
  {
    typedef utils::repository repository_type;
    
    if (! path().empty() && path() == file) return;
    
    repository_type rep(file, repository_type::write);
    
    //std::cerr << "dump vocab" << std::endl;
    __vocab.write(rep.path("vocab"));
    
    for (int shard = 0; shard < __shards.size(); ++ shard) {
      std::ostringstream stream_shard;
      stream_shard << "ngram-" << std::setfill('0') << std::setw(6) << shard;

      //std::cerr << "dump index shard: " << shard << std::endl;
      
      __shards[shard].write(rep.path(stream_shard.str()));
    }
    
    std::ostringstream stream_shard;
    std::ostringstream stream_order;
    stream_shard << __shards.size();
    stream_order << __order;
    rep["shard"] = stream_shard.str();
    rep["order"] = stream_order.str();
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
    __shards.resize(atoi(siter->second.c_str()));

    if (shard >= __shards.size())
      throw std::runtime_error("shard is out of range...");
    
    // order
    repository_type::const_iterator oiter = rep.find("order");
    if (oiter == rep.end())
      throw std::runtime_error("no order");
    __order = atoi(oiter->second.c_str());
    
    // vocabulary...
    __vocab.open(rep.path("vocab"));
    
    std::ostringstream stream_shard;
    stream_shard << "ngram-" << std::setfill('0') << std::setw(6) << shard;
    
    __shards[shard].open(rep.path(stream_shard.str()));
    
    __path = path;
  }
};
