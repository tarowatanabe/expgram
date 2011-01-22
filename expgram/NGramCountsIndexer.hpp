// -*- mode: c++ -*-

#ifndef __EXPGRAM__NGRAM_COUTNS_INDEXER__HPP__
#define __EXPGRAM__NGRAM_COUTNS_INDEXER__HPP__ 1

#include <stdint.h>

#include <stdexcept>
#include <sstream>

#include <boost/iostreams/filtering_stream.hpp>
#include <boost/thread.hpp>

#include <utils/succinct_vector.hpp>
#include <utils/tempfile.hpp>

namespace expgram
{
  
  template <typename NGram>
  struct NGramCountsIndexer
  {
    typedef NGram                     ngram_type;
    typedef NGramCountsIndexer<NGram> indexer_type;
    
    typedef typename ngram_type::size_type       size_type;
    typedef typename ngram_type::difference_type difference_type;
    
    typedef typename ngram_type::count_type      count_type;
    typedef typename ngram_type::word_type       word_type;
    typedef typename ngram_type::id_type         id_type;
    typedef typename ngram_type::vocab_type      vocab_type;
    typedef typename ngram_type::path_type       path_type;
    
    typedef utils::packed_vector<id_type, std::allocator<id_type> >       id_set_type;
    typedef utils::packed_vector<count_type, std::allocator<count_type> > count_set_type;
    typedef std::vector<size_type, std::allocator<size_type> >            size_set_type;
    typedef utils::succinct_vector<std::allocator<int32_t> >              position_set_type;

    id_set_type    ids;
    count_set_type counts;
    size_set_type positions_first;
    size_set_type positions_last;
    
    template <typename Stream>
    void operator()(int shard, ngram_type& ngram, Stream& os_count, int debug)
    {
      const int order_prev = ngram.index[shard].offsets.size() - 1;
      const int order      = order_prev + 1;
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
      ids.build();
      counts.build();
      
      for (size_type i = 0; i < positions_size; ++ i) {
	const size_type pos_first = positions_first[i];
	const size_type pos_last  = positions_last[i];
	
	for (size_type pos = pos_first; pos != pos_last; ++ pos) {
	  const id_type    id    = ids[pos];
	  const count_type count = counts[pos];
	  
	  os_id.write((char*) &id, sizeof(id_type));
	  os_count.write((char*) &count, sizeof(count_type));
	  positions.set(positions.size(), true);
	}
	positions.set(positions.size(), false);
      }
      
      // perform indexing...
      os_id.pop();
      positions.write(path_position);
      
      while (! ngram_type::shard_index_type::shard_type::id_set_type::exists(path_id))
	boost::thread::yield();
      
      while (! ngram_type::shard_index_type::shard_type::position_set_type::exists(path_id))
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
      ngram.index[shard].caches_pos.clear();
      ngram.index[shard].caches_backoff.clear();
      
      ngram.index.order() = order;
      
      if (debug)
	std::cerr << "shard: " << shard
		  << " index: " << ngram.index[shard].ids.size()
		  << " positions: " << ngram.index[shard].positions.size()
		  << " offsets: "  << ngram.index[shard].offsets.back()
		  << std::endl;
      
      // remove temporary index
      ids.clear();
      counts.clear();
      positions_first.clear();
      positions_last.clear();
    }
    
    
    template <typename Tp>
    struct less_first
    {
      bool operator()(const Tp& x, const Tp& y) const
      {
	return x.first < y.first;
      }
    };
    
    template <typename Context, typename WordCountSet>
    void operator()(int shard, ngram_type& ngram, const Context& prefix, WordCountSet& words)
    {
      typedef Context      context_type;
      typedef WordCountSet word_count_set_type;
      typedef typename word_count_set_type::value_type word_count_type;
      
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
      
      std::pair<typename context_type::const_iterator, size_type> result = ngram.index.traverse(shard, prefix.begin(), prefix.end());
      if (result.first != prefix.end() || result.second == size_type(-1)) {
	std::ostringstream stream;
	
	stream << "No prefix:";
	typename Context::const_iterator citer_end = prefix.end();
	for (typename Context::const_iterator citer = prefix.begin(); citer != citer_end; ++ citer)
	  stream << ' ' << ngram.index.vocab()[*citer];
	
	throw std::runtime_error(stream.str());
      }
      
      const size_type pos = result.second - ngram.index[shard].offsets[order_prev - 1];
      positions_first[pos] = ids.size();
      positions_last[pos]  = ids.size() + words.size();
      
      std::sort(words.begin(), words.end(), less_first<word_count_type>());
      typename word_count_set_type::const_iterator witer_end = words.end();
      for (typename word_count_set_type::const_iterator witer = words.begin(); witer != witer_end; ++ witer) {
	ids.push_back(witer->first);
	counts.push_back(witer->second);
      }
    }
    
  };
  
};


#endif
