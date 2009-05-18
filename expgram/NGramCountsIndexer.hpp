// -*- mode: c++ -*-

#ifndef __EXPGRAM__NGRAM_COUTNS_INDEXER__HPP__
#define __EXPGRAM__NGRAM_COUTNS_INDEXER__HPP__ 1

#include <stdint.h>

#include <stdexcept>

#include <boost/iostreams/filtering_stream.hpp>

#include <utils/succinct_vector.hpp>
#include <utils/tempfile.hpp>

namespace expgram
{
  
  struct NGramCountsIndexer
  {
    
    template <typename NGram, typename ShardData>
    static inline
    void index_ngram(int shard, NGram& ngram, ShardData& shard_data, int debug)
    {
      typedef NGram ngram_type;
      
      typedef typename ngram_type::size_type  size_type;
      typedef typename ngram_type::count_type count_type;
      
      typedef typename ngram_type::id_type    id_type;
      typedef typename ngram_type::path_type  path_type;

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
      
      // index id and count...
      shard_data.ids.build();
      shard_data.counts.build();
      
      for (size_type i = 0; i < positions_size; ++ i) {
	const size_type pos_first = shard_data.positions_first[i];
	const size_type pos_last  = shard_data.positions_last[i];
	
	for (size_type pos = pos_first; pos != pos_last; ++ pos) {
	  const id_type    id    = shard_data.ids[pos];
	  const count_type count = shard_data.counts[pos];
	  
	  os_id.write((char*) &id, sizeof(id_type));
	  shard_data.os_count.write((char*) &count, sizeof(count_type));
	  positions.set(positions.size(), true);
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
      ngram.index[shard].offsets.push_back(ngram.index[shard].offsets.back() + shard_data.ids.size());
      
      if (debug)
	std::cerr << "shard: " << shard
		  << " index: " << ngram.index[shard].ids.size()
		  << " positions: " << ngram.index[shard].positions.size()
		  << " offsets: "  << ngram.index[shard].offsets.back()
		  << std::endl;
      
      // remove temporary index
      shard_data.ids.clear();
      shard_data.counts.clear();
      shard_data.positions_first.clear();
      shard_data.positions_last.clear();
    }
    
    
    template <typename Tp>
    struct less_first
    {
      bool operator()(const Tp& x, const Tp& y) const
      {
	return x.first < y.first;
      }
    };
    
    template <typename NGram, typename ShardData, typename Context, typename WordCountSet>
    static inline
    void index_ngram(int shard, NGram& ngram, ShardData& shard_data, const Context& prefix, WordCountSet& words)
    {
      typedef NGram ngram_type;
      
      typedef typename ngram_type::size_type  size_type;
      typedef typename ngram_type::count_type count_type;
      
      typedef typename ngram_type::id_type    id_type;
      typedef typename ngram_type::path_type  path_type;
      
      typedef Context      context_type;
      typedef WordCountSet word_count_set_type;
      typedef typename word_count_set_type::value_type word_count_type;
      
      const int order_prev = ngram.index[shard].offsets.size() - 1;
      const size_type positions_size = ngram.index[shard].offsets[order_prev] - ngram.index[shard].offsets[order_prev - 1];
      
      if (shard_data.positions_first.empty())
	shard_data.positions_first.resize(positions_size, size_type(0));
      if (shard_data.positions_last.empty())
	shard_data.positions_last.resize(positions_size, size_type(0));
      
      std::pair<typename context_type::const_iterator, size_type> result = ngram.index.traverse(shard, prefix.begin(), prefix.end());
      if (result.first != prefix.end() || result.second == size_type(-1))
	throw std::runtime_error("no prefix?");
      
      const size_type pos = result.second - ngram.index[shard].offsets[order_prev - 1];
      shard_data.positions_first[pos] = shard_data.ids.size();
      shard_data.positions_last[pos]  = shard_data.ids.size() + words.size();
      
      std::sort(words.begin(), words.end(), less_first<word_count_type>());
      typename word_count_set_type::const_iterator witer_end = words.end();
      for (typename word_count_set_type::const_iterator witer = words.begin(); witer != witer_end; ++ witer) {
	shard_data.ids.push_back(witer->first);
	shard_data.counts.push_back(witer->second);
      }
    }
    
  };
  
};


#endif
