

#include "NGramCounts.hpp"

namespace expgram
{
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
  
  void NGramCounts::ShardData::open(const path_type& path)
  {
    typedef utils::repository repository_type;
    
    clear();
    
    repository_type rep(path, repository_type::read);
    
    repository_type::const_iterator oiter = rep.find("offset");
    if (oiter == rep.end())
      throw std::runtime_error("no offset?");
    offset = atoll(oiter->second.c_str());
    
    counts.open(rep.path("counts"));
  }
  
  void NGramCounts::ShardData::write(const path_type& file) const
  {
    typedef utils::repository repository_type;
    
    if (path() == file) return;
    
    repository_type rep(file, repository_type::read);
    
    utils::filesystem::copy_files(counts.path(), rep.path("counts"));
    
    std::ostringstream stream_offset;
    stream_offset << offset;
    rep["offset"] = stream_offset.str();
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
    shards.resize(atoi(siter->second.c_str()));
    
    for (int shard = 0; shard < shards.size(); ++ shard) {
      std::ostringstream stream_shard;
      stream_shard << "ngram-" << std::setfill('0') << std::setw(6) << shard;
      
      shards[shard].open(rep.path(stream_shard.str()));
    }
  }
  
  template <typename Path, typename Shards>
  inline
  void write_shards(const Path& path, const Shards& shards)
  {
    typedef utils::repository repository_type;
    
    repository_type rep(path, repository_type::write);
    
    for (int shard = 0; shard < shards.size(); ++ shard) {
      std::ostringstream stream_shard;
      stream_shard << "ngram-" << std::setfill('0') << std::setw(6) << shard;
      
      shards[shard].write(rep.path(stream_shard.str()));
    }
    
    std::ostringstream stream_shard;
    stream_shard << shards.size();
    rep["shard"] = stream_shard.str();
  }
  
  void NGramCounts::open(const path_type& path, const size_type shard_size, const bool unique)
  {
    if (! boost::filesystem::exists(path))
      throw std::runtime_error(std::string("no file: ") + path.file_string());
    
    if (! boost::filesystem::is_directory(path))
      throw std::runtime_error(std::string("invalid path: ") + path.file_string());

    clear();
    
    if (boost::filesystem::exits(path / "prop.list"))
      open_binary(path);
    else
      open_google(path, shard_size, unique);
  }

  void NGramCounts::open_binary(const path_type& path)
  {
    typedef utils::repository repository_type;
    
    clear();
    
    repository_type rep(path, repository_type::read);
    
    index.open(rep.path("index"));
    if (boost::filesystem::exists(rep.path("count")))
      open_shards(rep.path("count"), counts);
    if (boost::filesystem::exists(rep.path("count-modified")))
      open_shards(rep.path("count-modified"), counts_modified);
    
  }
};
