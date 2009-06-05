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

namespace expgram
{
  class Word
  {
  public:
    typedef std::string word_type;
    typedef uint32_t    id_type;
    
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
    Word(const id_type& x) : __id(x) {}
    template <typename Iterator>
    Word(Iterator first, Iterator last) : __id(__allocate(word_type(first, last))) { }
    
    void assign(const word_type& x) { __id = __allocate(x); }
    template <typename Iterator>
    void assign(Iterator first, Iterator last) { __id = __allocate(word_type(first, last)); }
    
  public:
    void swap(Word& x) { std::swap(__id, x.__id); }
    
    const id_type   id() const { return __id; }
    operator const word_type&() const { return word(); }
    
    const word_type& word() const
    {
      word_map_type& maps = __word_maps();
      
      if (__id >= maps.size())
	maps.resize(__id + 1, 0);
      if (! maps[__id]) {
	lock_type lock(__mutex);
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
    struct hasher
    {
      size_t operator()(const word_type& x) const
      {
	return __hasher(x.begin(), x.end(), size_t(0));
      }
      utils::hashmurmur<size_t> __hasher;
    };
    typedef utils::indexed_set<word_type, hasher, std::equal_to<word_type>, std::allocator<word_type> > word_set_type;

    typedef std::vector<const word_type*, std::allocator<const word_type*> > word_map_type;
    
  public:
    static bool exists(const word_type& x)
    {
      lock_type lock(__mutex);
      return __words().find(x) == __words().end();
    }
    static size_t allocated()
    {
      lock_type lock(__mutex);
      return __words().size();
    }
    static void write(const path_type& path);
    
  private:
    static mutex_type    __mutex;
    
    static word_map_type& __word_maps()
    {
      static boost::thread_specific_ptr<word_map_type> __maps;
      
      if (! __maps.get()) {
	__maps.reset(new word_map_type());
	__maps->reserve(allocated());
      }
      
      return *__maps;
    }
    
    static word_set_type& __words()
    {
      static word_set_type words;
      return words;
    }
    
    static const id_type& __allocate_empty()
    {
      static const id_type __id = __allocate(word_type());
      return __id;
    }
    static id_type __allocate(const word_type& x)
    {
      lock_type lock(__mutex);
      word_set_type::iterator witer = __words().insert(x).first;
      return witer - __words().begin();
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

