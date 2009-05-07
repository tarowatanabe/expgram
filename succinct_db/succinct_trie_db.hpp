// -*- mode: c++ -*-

#ifndef __SUCCINCT_DB__SUCCINCT_TRIE_DB__HPP__
#define __SUCCINCT_DB__SUCCINCT_TRIE_DB__HPP__ 1

//
// directly store the fixed-sized data into succinct-trie data structure
//

#include <stdint.h>

#include <iostream>
#include <algorithm>
#include <vector>

#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/device/file.hpp>
#include <boost/filesystem.hpp>
#include <boost/shared_ptr.hpp>

#include <succinct_db/succinct_trie.hpp>

#include <utils/repository.hpp>
#include <utils/tempfile.hpp>

namespace succinctdb
{
  // do we use template-only implementaion...?
  // yes, for simplicity..
  
  template <typename Key, typename Data, typename Alloc=std::allocator<std::pair<Key, Data> > >
  struct __succinct_trie_db_writer
  {
    typedef size_t    size_type;
    typedef ptrdiff_t difference_type;
    typedef int64_t   off_type;
    
    typedef Key       key_type;
    typedef Data      data_type;
    typedef Data      mapped_type;

    typedef boost::filesystem::path path_type;

    typedef typename Alloc::template rebind<key_type>::other  key_alloc_type;
    typedef typename Alloc::template rebind<data_type>::other data_alloc_type;
    typedef typename Alloc::template rebind<char>::other      byte_alloc_type;
    
    __succinct_trie_db_writer(const path_type& path) { open(path); }
    ~__succinct_trie_db_writer() { close(); }
    
    size_type insert(const key_type* buf, size_type buf_size, const data_type* data)
    {
      __os_key_data->write((char*) buf, sizeof(key_type) * buf_size);
      __os_key_data->write((char*) data, sizeof(data_type));
      __os_size->write((char*) &buf_size, sizeof(size_type));
      return __size ++;
    }
    size_type size() const { return __size; }
    
    
    void open(const path_type& path)
    {
      path_output = path;
      path_key_data = utils::tempfile::file_name(utils::tempfile::tmp_dir() / "succinct-db.key-data.XXXXXX");
      path_size = utils::tempfile::file_name(utils::tempfile::tmp_dir() / "succinct-db.size.XXXXXX");
      
      utils::tempfile::insert(path_key_data);
      utils::tempfile::insert(path_size);
      
      __os_key_data.reset(new boost::iostreams::filtering_ostream());
      __os_size.reset(new boost::iostreams::filtering_ostream());
      
      __os_key_data->push(boost::iostreams::file_sink(path_key_data.file_string()), 1024 * 1024);
      
      __os_size->push(boost::iostreams::gzip_compressor());
      __os_size->push(boost::iostreams::file_sink(path_size.file_string()), 1024 * 1024);
      
      __size = 0;
    }


    struct __value_type
    {
      const key_type* first;
      const key_type* last;
      
      size_type size() const { return last - first; }
      const key_type& operator[](size_type pos) const { return *(first + pos); }

      __value_type() {}
    };
    typedef typename Alloc::template rebind<__value_type>::other __value_alloc_type;
    typedef std::vector<__value_type, __value_alloc_type> __value_set_type;
    
    struct __extract_key
    {
      const __value_type& operator()(const __value_type& x) const { return x; }
    };

    struct __extract_data
    {
      const data_type& operator()(const __value_type& x) const { return *reinterpret_cast<const data_type*>(x.last);}
    };

    struct __less_value
    {
      bool operator()(const __value_type& x, const __value_type& y) const {
	return std::lexicographical_compare(x.first, x.last, y.first, x.last);
      }
    };
    
    void close()
    {
      // close...
      __os_key_data.reset();
      __os_size.reset();
      
      typedef utils::map_file<char, byte_alloc_type> map_file_type;
      
      map_file_type map_key_data(path_key_data);
      __value_set_type values(__size);
      
      {
	const char* iter = reinterpret_cast<const char*>(&(*map_key_data.begin()));
	boost::iostreams::filtering_istream is;
	is.push(boost::iostreams::gzip_decompressor());
	is.push(boost::iostreams::file_source(path_size.file_string()));
	for (size_t i = 0; i < __size; ++ i) {
	  size_type key_size = 0;
	  is.read((char*) &key_size, sizeof(size_type));
	  
	  values[i].first = reinterpret_cast<const key_type*>(iter);
	  values[i].last = values[i].first + key_size;

	  iter += sizeof(key_type) * key_size + sizeof(data_type);
	}
      }
      boost::filesystem::remove(path_size);
      utils::tempfile::erase(path_size);
      
      typedef succinct_trie<key_type, data_type, Alloc> succinct_trie_type;
      
      std::sort(values.begin(), values.end(), __less_value());
      
      // we will build and dump simultaneously..
      succinct_trie_type succinct_trie;
      succinct_trie.build(path_output, values.begin(), values.end(), __extract_key(), __extract_data());
      //succinct_trie.write(path_output);
      
      map_key_data.clear();
      values.clear();
      
      boost::filesystem::remove(path_key_data);
      utils::tempfile::erase(path_key_data);
    }

  private:
    path_type path_output;
    path_type path_key_data;
    path_type path_size;
    boost::shared_ptr<boost::iostreams::filtering_ostream> __os_key_data;
    boost::shared_ptr<boost::iostreams::filtering_ostream> __os_size;
    size_type __size;
  };
  
  template <typename Key, typename Data, typename Alloc=std::allocator<std::pair<Key, Data> > >
  class succinct_trie_db
  {
  public:
    typedef enum {
      read,
      write,
    } mode_type;
    
    typedef size_t    size_type;
    typedef ptrdiff_t difference_type;
    
    typedef Key  key_type;
    typedef Data data_type;
    typedef Data mapped_type;
    
    typedef boost::filesystem::path path_type;
    
  private:
    typedef succinct_trie_mapped<Key, Data, Alloc>     succinct_trie_type;
    typedef __succinct_trie_db_writer<Key,Data,Alloc> succinct_writer_type;
    
  public:
    succinct_trie_db() {}
    succinct_trie_db(const path_type& path, const mode_type mode=read) { open(path, mode); }
    ~succinct_trie_db() { close(); }
    
  public:
    // methods supported by both read/write mode
    void open(const path_type& path, const mode_type mode=read)
    {
      clear();
      if (mode == read)
	__succinct_trie.reset(new succinct_trie_type(path));
      else
	__succinct_writer.reset(new succinct_writer_type(path));
    }
    
    void close() { __succinct_trie.reset(); __succinct_writer.reset(); }
    void clear() { close(); }
    
    bool is_open() const { return __succinct_trie || __succinct_writer; }
    bool is_writer() const { return __succinct_writer; }
    bool is_reader() const { return __succinct_trie; }
    
    size_type size() const { return (is_open() ? (is_reader() ? __succinct_trie->size() : __succinct_writer->size()) : size_type(0)); }
    bool empty() const { return size() == 0; }
    
  public:
    // the only method supported by write-mode
    // close() will invoke index-building
    size_type insert(const key_type* key, size_type key_size, const data_type& data)
    {
      return insert(key, key_size, &data);
    }
    size_type insert(const key_type* key, size_type key_size, const data_type* data)
    {
      return __succinct_writer->insert(key, key_size, data);
    }
    
  public:    
    // structures supported by read-mode
    typedef typename succinct_trie_type::const_iterator         const_iterator;
    typedef typename succinct_trie_type::iterator               iterator;
    typedef typename succinct_trie_type::const_reverse_iterator const_reverse_iterator;
    typedef typename succinct_trie_type::reverse_iterator       reverse_iterator;
    typedef typename succinct_trie_type::const_cursor           const_cursor;
    typedef typename succinct_trie_type::cursor                 cursor;
    
  public:
    // operations supported by read-mode
    
    const_iterator begin(size_type node_pos) const { return __succinct_trie->begin(node_pos); }
    const_iterator end(size_type node_pos)   const { return __succinct_trie->end(node_pos); }
    const_iterator begin() const { return __succinct_trie->begin(); }
    const_iterator end()   const { return __succinct_trie->end(); }
    
    const_reverse_iterator rbegin(size_type node_pos) const { return __succinct_trie->rbegin(node_pos); }
    const_reverse_iterator rend(size_type node_pos)   const { return __succinct_trie->rend(node_pos); }
    const_reverse_iterator rbegin() const { return __succinct_trie->rbegin(); }
    const_reverse_iterator rend()   const { return __succinct_trie->rend(); }

    const_cursor cbegin(size_type node_pos) const { return __succinct_trie->cbegin(node_pos); }
    const_cursor cend(size_type node_pos)   const { return __succinct_trie->cend(node_pos); }
    const_cursor cbegin() const { return __succinct_trie->cbegin(); }
    const_cursor cend()   const { return __succinct_trie->cend(); }

    size_type find(const key_type* key_buf, size_type key_size) const
    {
      size_type node_pos = 0;
      size_type key_pos = 0;
      return traverse(key_buf, node_pos, key_pos, key_size);
    }
    
    size_type traverse(const key_type* key_buf, size_type& node_pos, size_type& key_pos, size_type key_size) const
    {
      return __succinct_trie->traverse(key_buf, node_pos, key_pos, key_size);
    }
    
    bool exists(size_type node_pos) const { return __succinct_trie->has_data(node_pos); }
    bool is_valid(size_type node_pos) const { return node_pos != succinct_trie_type::out_of_range(); }
    const data_type& operator[](size_type node_pos) const { return __succinct_trie->data(node_pos); }
    
  private:
    boost::shared_ptr<succinct_trie_type>   __succinct_trie;
    boost::shared_ptr<succinct_writer_type> __succinct_writer;
  };
};

#endif
