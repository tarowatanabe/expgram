// -*- mode: c++ -*-
//
//  Copyright(C) 2009-2013 Taro Watanabe <taro.watanabe@nict.go.jp>
//

#ifndef __EXPGRAM__NGRAM_COUNTS__HPP__
#define __EXPGRAM__NGRAM_COUNTS__HPP__ 1

#include <stdint.h>

#include <expgram/Word.hpp>
#include <expgram/Vocab.hpp>
#include <expgram/NGramIndex.hpp>
#include <expgram/NGram.hpp>
#include <expgram/Stat.hpp>

#include <boost/array.hpp>

#include <utils/packed_vector.hpp>
#include <utils/succinct_vector.hpp>
#include <utils/map_file.hpp>
#include <utils/mathop.hpp>

namespace expgram
{
  class NGramCounts
  {
  public:
    typedef Word               word_type;
    typedef Vocab              vocab_type;
    
    typedef size_t             size_type;
    typedef ptrdiff_t          difference_type;
    
    typedef word_type::id_type id_type;
    typedef uint64_t           count_type;
    typedef float              logprob_type;
    typedef double             prob_type;
    typedef uint8_t            quantized_type;
    
    typedef boost::filesystem::path path_type;

    typedef Stat stat_type;

  public:
    struct ShardData
    {
    public:
      typedef utils::packed_vector_mapped<count_type, std::allocator<count_type> > count_set_type;
      
    public:
      ShardData() : counts(), offset(0) {}
      ShardData(const path_type& path) : counts(), offset(0) { open(path); }
      
    public:
      void open(const path_type& path);
      void write(const path_type& file) const;
      path_type path() const { return counts.path().parent_path(); }
      
      void close() { clear(); }
      void clear()
      {
	counts.clear();
	offset = 0;
      }
      
      count_type operator[](size_type pos) const { return counts[pos - offset]; }
      
      size_type size() const { return counts.size() + offset; }
      
      stat_type stat() const
      {
	return stat_type(counts.size_bytes(), counts.size_compressed(), counts.size_cache());
      }

    public:
      friend
      bool operator==(const ShardData& x, const ShardData& y)
      {
	return x.counts == y.counts && x.offset == y.offset;
      }

      friend
      bool operator!=(const ShardData& x, const ShardData& y)
      {
	return !(x == y);
      }
      
    public:
      count_set_type counts;
      size_type      offset;
    };
    
    typedef ShardData                                                      shard_data_type;
    typedef std::vector<shard_data_type, std::allocator<shard_data_type> > shard_data_set_type;
    typedef NGramIndex                                                     shard_index_type;

    typedef NGram                                                          ngram_type;

  public:
    NGramCounts(const int _debug=0) : debug(_debug) { clear(); }
    NGramCounts(const path_type& path,
		const size_type shard_size=16,
		const int _debug=0)  : debug(_debug) { open(path, shard_size); }
    
  public:
    template <typename Iterator>
    bool exists(Iterator first, Iterator last) const
    {
      if (first == last) return false;
      return index.traverse(first, last).first == last;
    }

    template <typename Iterator>
    count_type operator()(Iterator first, Iterator last) const
    {
      return count(first, last);
    }
    
    template <typename Iterator>
    count_type count(Iterator first, Iterator last) const
    {
      const size_type shard_index = index.shard_index(first, last);
      std::pair<Iterator, size_type> result = index.traverse(shard_index, first, last);
      return (result.first == last && result.second != size_type(-1) ? counts[shard_index][result.second] : count_type(0));
    }
    
    template <typename Iterator>
    count_type modified(Iterator first, Iterator last) const
    {
      const size_type shard_index = index.shard_index(first, last);
      std::pair<Iterator, size_type> result = index.traverse(shard_index, first, last);
      return (result.first == last && result.second != size_type(-1) ? types[shard_index][result.second] : count_type(0));
    }

  public:
    void swap(NGramCounts& x)
    {
      index.swap(x.index);
      counts.swap(x.counts);
      types.swap(x.types);
      
      std::swap(debug,  x.debug);
    }

    path_type path() const { return index.path().parent_path(); }
    size_type size() const { return index.size(); }
    bool empty() const { return index.empty(); }
    
    void open(const path_type& path,
	      const size_type shard_size=16);

    void open_shard(const path_type& path, int shard);

    void write_prepare(const path_type& path) const;
    void write_shard(const path_type& path, int shard) const;
    
    void close() { clear(); }
    void clear()
    {
      index.clear();
      counts.clear();
      types.clear();
    }
    
    void write(const path_type& path) const;
    void dump(const path_type& path) const;
    void modify();
    void estimate(ngram_type& ngram, const bool remove_unk=false) const;
    
    bool is_open() const { return index.is_open(); }
    bool is_modified() const { return ! types.empty(); }
    
    stat_type stat_index() const { return index.stat_index(); }
    stat_type stat_pointer() const { return index.stat_pointer(); }
    stat_type stat_vocab() const { return index.stat_vocab(); }
    
    stat_type stat_counts() const 
    {
      stat_type stat;
      for (size_type shard = 0; shard < counts.size(); ++ shard)
	stat += counts[shard].stat();
      return stat;
    }
    stat_type stat_modified() const 
    {
      stat_type stat;
      for (size_type shard = 0; shard < types.size(); ++ shard)
	stat += types[shard].stat();
      return stat;
    }

  public:
    friend
    bool operator==(const NGramCounts& x, const NGramCounts& y)
    {
      return x.index == y.index && x.counts == y.counts && x.types == y.types;
    }

    friend
    bool operator!=(const NGramCounts& x, const NGramCounts& y)
    {
      return !(x == y);
    }

    
  private:
    void open_binary(const path_type& path);
    void open_google(const path_type& path,
		     const size_type shard_size=16);
    
  public:
    shard_index_type    index;
    shard_data_set_type counts;
    shard_data_set_type types;
    
    int debug;
  };
};

#endif
