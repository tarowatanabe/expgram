// -*- mode: c++ -*-

#ifndef __UTILS__COMPACT_TRIE__HPP__
#define __UTILS__COMPACT_TRIE__HPP__ 1

#include <google/sparse_hash_map>

#include <utils/chunk_vector.hpp>

#include <boost/functional/hash.hpp>

namespace utils
{
  template <typename Key,
	    typename Data,
	    typename Hash=boost::hash<Key>,
	    typename Equal=std::equal_to<Key>,
	    typename Alloc=std::allocator<std::pair<const Key, Data> > >
  class compact_trie
  {
  public:
    typedef Key                        key_type;
    typedef Data                       data_type;
    typedef Data                       mapped_type;
    typedef std::pair<const Key, Data> value_type;
    
    typedef Hash                       hash_type;
    typedef Equal                      equal_type;
    
    typedef size_t                     size_type;
    typedef ptrdiff_t                  difference_type;
    
    typedef uint32_t                   id_type;
    
  private:
    
    typedef typename Alloc::template rebind<std::pair<const key_type, id_type> >::other id_map_alloc_type;
    typedef google::sparse_hash_map<key_type, id_type, hash_type, equal_type, id_map_alloc_type> id_map_type;
  
    struct Node
    {
      id_map_type __map;
      mapped_type __data;

      Node() : __map(), __data() {}
      Node(const mapped_type& data) : __map(), __data(data) {}
    };
    typedef Node node_type;
    
    typedef typename Alloc::template rebind<node_type>::other node_alloc_type;
    typedef utils::chunk_vector<node_type, 4096 / sizeof(node_type), node_alloc_type> node_set_type;

  public:
    typedef typename id_map_type::const_iterator const_iterator;
    typedef typename id_map_type::const_iterator       iterator;
    
  public:
    compact_trie() {}
    
  public:
    const_iterator begin(id_type __id) const { return __nodes[__id].__map.begin(); }
    const_iterator end(id_type __id) const { return __nodes[__id].__map.end(); }
    
    inline const mapped_type& operator[](id_type __id) const { return __nodes[__id].__data; }
    inline       mapped_type& operator[](id_type __id)       { return __nodes[__id].__data; }
    
    void clear() { __nodes.clear(); }
    
    bool empty() const { return __nodes.empty(); }
    bool empty(id_type __id) const { return __nodes[__id].__map.empty(); }
    
    bool is_root(id_type __id) const { return __id == 0; }
    
    id_type root() const { return 0; }

    id_type find(id_type __id, const key_type& key) const
    {
      if (__nodes.empty())
	return 0;
      
      typename id_map_type::const_iterator niter = __nodes[__id].__map.find(key);
      if (niter != __nodes[__id].__map.end())
	return niter->second;
      else
	return 0;
    }
    
    template <typename Iterator>
    id_type find(Iterator first, Iterator last) const
    {
      if (__nodes.empty())
	return 0;
      
      id_type __id = 0;
      for (/**/; first != last; ++ first) {
	typename id_map_type::const_iterator niter = __nodes[__id].__map.find(*first);
	if (niter != __nodes[__id].__map.end())
	  __id = niter->second;
	else
	  return 0;
      }
      return __id;
    }
    
    id_type insert(id_type __id, const key_type& key)
    {
      return __insert_key(__id, key);
    }
    
    template <typename Iterator>
    id_type insert(Iterator first, Iterator last)
    {
      typedef typename boost::is_integral<Iterator>::type __integral;
      return __insert_dispatch(first, last, __integral());
    }
    
    
    
  private:
    template <typename Integer>
    id_type __insert_dispatch(Integer __id, Integer __key, boost::true_type)
    {
      return __insert_key(__id, __key);
    }
    
    template <typename Iterator>
    id_type __insert_dispatch(Iterator first, Iterator last, boost::false_type)
    {
      return __insert_range(first, last);
    }
    
    
    id_type __insert_key(id_type __id, const key_type& key)
    {
      if (__nodes.empty())
	__nodes.push_back(node_type());
      
      typename id_map_type::iterator niter = __nodes[__id].__map.find(key);
      if (niter != __nodes[__id].__map.end())
	return niter->second;
      else {
	__nodes[__id].__map.insert(std::make_pair(key, __nodes.size()));
	__id = __nodes.size();
	__nodes.push_back(node_type());
	return __id;
      }
    }

    template <typename Iterator>
    id_type __insert_range(Iterator first, Iterator last)
    {
      if (__nodes.empty())
	__nodes.push_back(node_type());
      
      id_type __id = 0;
      for (/**/; first != last; ++ first) {
	typename id_map_type::iterator niter = __nodes[__id].__map.find(*first);
	if (niter != __nodes[__id].__map.end())
	  __id = niter->second;
	else {
	  __nodes[__id].__map.insert(std::make_pair(*first, __nodes.size()));
	  __id = __nodes.size();
	  __nodes.push_back(node_type());
	}
      }
      
      return __id;
    }
    
    
  private:
    node_set_type __nodes;
  };
};

#endif
