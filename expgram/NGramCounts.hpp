// -*- mode: c++ -*-

#ifndef __EXPGRAM__NGRAM_COUNTS__HPP__
#define __EXPGRAM__NGRAM_COUNTS__HPP__ 1

#include <stdint.h>

#include <expgram/Word.hpp>
#include <expgram/Vocab.hpp>
#include <expgram/NGramIndex.hpp>
#include <expgram/NGram.hpp>

#include <boost/array.hpp>

#include <utils/hashmurmur.hpp>
#include <utils/packed_vector.hpp>
#include <utils/succinct_vector.hpp>
#include <utils/map_file.hpp>
#include <utils/mathop.hpp>

namespace expgram
{
  class NGramConts
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
      size_type size() const { return counts.sizse() + offset; }
      
    public:
      count_set_type counts;
      size_type      offset;
    };
    typedef ShardData                                                      shard_data_type;
    typedef std::vector<shard_data_type, std::allocator<shard_data_type> > shard_data_set_type;
    typedef NGramIndex                                                     shard_index_type;

    typedef NGram                                                          ngram_type;

  public:
    NGramCounts() { clear(); }
    NGramCounts(const path_type& path,
		const size_type shard_size=16,
		const bool unique=false)  { open(path, shard_size, unique); }
    
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
    count_type count_modified(Iterator first, Iterator last) const
    {
      const size_type shard_index = index.shard_index(first, last);
      std::pair<Iterator, size_type> result = index.traverse(shard_index, first, last);
      return (result.first == last && result.second != size_type(-1) ? counts_modified[shard_index][result.second] : count_type(0));
    }
    
  public:
    path_type path() const { return index.path().parent_path(); }
    size_type size() const { return index.size(); }
    bool empty() const { return index.empty(); }
    
    void open(const path_type& path,
	      const size_type shard_size=16,
	      const bool unique=false);
    void close() { clear(); }
    void clear()
    {
      index.clear();
      counts.clear();
      counts_modified.clear();
    }
    
    void write(const path_type& path) const;
    void dump(const path_type& path) const;
    void modify();
    void estimate(ngram_type& ngram) const;
    
  private:
    void open_binary(const path_type& path);
    void open_google(const path_type& path,
		     const size_type shard_size=16,
		     const bool unique=false);
    
  public:
    shard_index_type    index;
    shard_data_set_type counts;
    shard_data_set_type counts_modified;
  };
};

#endif
