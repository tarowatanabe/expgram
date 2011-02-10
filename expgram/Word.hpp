// -*- mode: c++ -*-

#ifndef __EXPGRAM__WORD__HPP__
#define __EXPGRAM__WORD__HPP__ 1

#include <stdint.h>

#include <iostream>
#include <iterator>
#include <string>
#include <vector>

#include <boost/filesystem.hpp>
#include <boost/thread.hpp>

#include <utils/indexed_set.hpp>
#include <utils/hashmurmur.hpp>
#include <utils/spinlock.hpp>
#include <utils/piece.hpp>
#include <utils/chunk_vector.hpp>

namespace expgram
{
  class Word
  {
  public:
    typedef std::string  word_type;
    typedef utils::piece piece_type;
    typedef uint32_t     id_type;
    
    typedef word_type::size_type              size_type;
    typedef word_type::difference_type        difference_type;
    
    typedef word_type::const_iterator         const_iterator;
    typedef word_type::const_reverse_iterator const_reverse_iterator;
    typedef word_type::const_reference        const_reference;

    typedef boost::filesystem::path path_type;
    
  private:
    typedef utils::spinlock              mutex_type;
    typedef utils::spinlock::scoped_lock lock_type;
    
  public:
    Word() : __id(__allocate_empty()) { }
    Word(const word_type& x) : __id(__allocate(x)) { }
    Word(const piece_type& x) : __id(__allocate(x)) {}
    Word(const char* x) : __id(__allocate(x)) {}
    Word(const id_type& x) : __id(x) {}
    template <typename Iterator>
    Word(Iterator first, Iterator last) : __id(__allocate(piece_type(first, last))) { }
    
    void assign(const piece_type& x) { __id = __allocate(x); }
    template <typename Iterator>
    void assign(Iterator first, Iterator last) { __id = __allocate(piece_type(first, last)); }
    
  public:
    void swap(Word& x) { std::swap(__id, x.__id); }
    
    const id_type   id() const { return __id; }
    operator const word_type&() const { return word(); }
    operator piece_type() const { return word(); }
    
    const word_type& word() const
    {
      word_map_type& maps = __word_maps();
      
      if (__id >= maps.size())
	maps.resize(__id + 1, 0);
      if (! maps[__id]) {
	lock_type lock(__mutex_data);
	maps[__id] = &(__words()[__id]);
      }
      
      return *maps[__id];
    }
    
    const_iterator begin() const { return word().begin(); }
    const_iterator end() const { return word().end(); }
    
    const_reverse_iterator rbegin() const { return word().rbegin(); }
    const_reverse_iterator rend() const { return word().rend(); }
    
    const_reference operator[](size_type x) const { return word()[x]; }
    
    size_type size() const { return word().size(); }
    bool empty() const { return word().empty(); }
    
  public:
    // boost hash
    friend
    size_t  hash_value(Word const& x);
    
    // iostreams
    friend
    std::ostream& operator<<(std::ostream& os, const Word& x);
    friend
    std::istream& operator>>(std::istream& is, Word& x);
    
    // comparison...
    friend
    bool operator==(const Word& x, const Word& y);
    friend
    bool operator!=(const Word& x, const Word& y);
    friend
    bool operator<(const Word& x, const Word& y);
    friend
    bool operator>(const Word& x, const Word& y);
    friend
    bool operator<=(const Word& x, const Word& y);
    friend
    bool operator>=(const Word& x, const Word& y);
    
  private:
    typedef utils::indexed_set<piece_type, boost::hash<piece_type>, std::equal_to<piece_type>, std::allocator<piece_type> > word_index_type;
    typedef utils::chunk_vector<word_type, 4096 / sizeof(word_type), std::allocator<word_type> > word_set_type;
    typedef std::vector<const word_type*, std::allocator<const word_type*> > word_map_type;
    
  public:
    static bool exists(const piece_type& x)
    {
      lock_type lock(__mutex_index);
      
      const word_index_type& index = __index();
      
      return index.find(x) != index.end();
    }
    static size_t allocated()
    {
      lock_type lock(__mutex_data);
      
      return __words().size();
    }
    static void write(const path_type& path);
    
  private:
    static mutex_type    __mutex_index;
    static mutex_type    __mutex_data;
    
    static word_map_type& __word_maps();
    
    static word_set_type& __words()
    {
      static word_set_type words;
      return words;
    }

    static word_index_type& __index()
    {
      static word_index_type index;
      return index;
    }
    
    static const id_type& __allocate_empty()
    {
      static const id_type __id = __allocate("");
      return __id;
    }
    
    static id_type __allocate(const piece_type& x)
    {
      lock_type lock(__mutex_index);

      word_index_type& index = __index();
      
      std::pair<word_index_type::iterator, bool> result = index.insert(x);
      
      if (result.second) {
	lock_type lock(__mutex_data);
	
	word_set_type& words = __words();
	words.push_back(x);
	const_cast<piece_type&>(*result.first) = words.back();
      }
      
      return result.first - index.begin();
    }
    
  private:
    id_type __id;
  };
  
  inline
  size_t hash_value(Word const& x)
  {
    return utils::hashmurmur<size_t>()(x.__id);
  }
  
  inline
  std::ostream& operator<<(std::ostream& os, const Word& x)
  {
    os << x.word();
    return os;
  }
  
  inline
  std::istream& operator>>(std::istream& is, Word& x)
  {
    std::string word;
    is >> word;
    x.assign(word);
    return is;
  }
  
  inline
   bool operator==(const Word& x, const Word& y)
  {
    return x.__id == y.__id;
  }
  inline
  bool operator!=(const Word& x, const Word& y)
  {
    return x.__id != y.__id;
  }
  inline
  bool operator<(const Word& x, const Word& y)
  {
    return x.__id < y.__id;
  }
  inline
  bool operator>(const Word& x, const Word& y)
  {
    return x.__id > y.__id;
  }
  inline
  bool operator<=(const Word& x, const Word& y)
  {
    return x.__id <= y.__id;
  }
  inline
  bool operator>=(const Word& x, const Word& y)
  {
    return x.__id >= y.__id;
  }

};

#endif

