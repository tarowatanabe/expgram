// -*- mode: c++ -*-

#ifndef __CODEC__BLOCK_FILE__H__
#define __CODEC__BLOCK_FILE__H__ 1

#include <string>
#include <algorithm>
#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>

#include <boost/filesystem.hpp>
#include <boost/array.hpp>
#include <boost/thread.hpp>
#include <boost/shared_array.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/zlib.hpp>
#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/device/array.hpp>

#include <utils/repository.hpp>
#include <utils/map_file.hpp>
#include <utils/arc_list.hpp>
#include <utils/bithack.hpp>
#include <utils/spinlock.hpp>

namespace codec
{
  
  class __block_file_byte_base
  {
  public:
    typedef int64_t        off_type;
    typedef size_t         size_type;
    typedef char           byte_type;
    typedef ptrdiff_t      difference_type;
    
    typedef utils::map_file<byte_type> data_type;
    typedef utils::map_file<off_type>  index_type;
    typedef boost::filesystem::path    path_type;
  
  public:
    static const size_type block_size = 1024 * 8;
    static const size_type block_mask = block_size - 1;
    static const size_type block_shift = utils::bithack::static_bit_count<block_mask>::result;
    
  public:
    __block_file_byte_base() : data(), index(), filesize(0), filename() { }
    __block_file_byte_base(const path_type& path) : data(), index(), filesize(0), filename() { open(path); }
    ~__block_file_byte_base() { close(); }
    
    bool is_open() const { return data.is_open(); }
    bool is_compressed() const { return index.is_open(); }
    
    const void* begin() const { return data.begin(); }
    const void* end() const { return data.end(); }
    
    off_type size() const { return filesize; }
    off_type file_size() const { return filesize; }
    const boost::filesystem::path& path() const { return filename; }

    void open(const path_type& path)
    {
      typedef utils::repository repository_type;
      
      close();
      if (boost::filesystem::is_directory(path)) {
	repository_type rep(path, repository_type::read);
	
	data.open(rep.path("data"));
	index.open(rep.path("index"));
	
	repository_type::const_iterator siter = rep.find("size");
	if (siter == rep.end())
	  throw std::runtime_error("no file size?");
	filesize = atoll(siter->second.c_str());
	
	repository_type::const_iterator titer = rep.find("type");
	if (titer == rep.end())
	  throw std::runtime_error("no type...");
      } else {
	data.open(path);
	filesize = data.file_size();
      }
      filename = path;
    }
    
    void close()
    {
      data.close();
      index.close();
      filesize = 0;
      filename = path_type();
    }
    void clear() { close(); }
    
  public:
    data_type  data;
    index_type index;
    
    off_type filesize;
    path_type filename;
  };
  

  template <typename _Codec, typename _Alloc>
  class __block_file_byte_impl : public __block_file_byte_base
  {
    // implementation of block_file
    // if not compressed, use raw bytes via begin()/end() interface...
    // otherwise, use read() interface with internal caching...
    
  private:
    typedef __block_file_byte_base         base_type;
    typedef __block_file_byte_impl<_Codec,_Alloc> self_type;
        
  public:
    typedef base_type::size_type       size_type;
    typedef base_type::difference_type difference_type;
    typedef base_type::off_type        off_type;
    typedef base_type::byte_type       byte_type;
    typedef base_type::path_type       path_type;
    
  private:
    typedef boost::shared_array<byte_type> block_type;
    typedef std::pair<off_type, block_type> block_value_type;
    typedef typename _Alloc::template rebind<block_value_type>::other block_value_alloc_type;
    typedef utils::arc_list<off_type, block_type, 16, std::equal_to<off_type>, block_value_alloc_type> cache_type;
    typedef typename cache_type::iterator cache_iterator;

    typedef std::vector<block_value_type, block_value_alloc_type> block_value_set_type;
    
    struct spinlock_type
    {
      spinlock_type() {}
      spinlock_type(const spinlock_type&) {}
      spinlock_type& operator=(const spinlock_type&) { return *this; }
      
      utils::spinlock& lock() const { return const_cast<utils::spinlock&>(__lock); }
      utils::spinlock __lock;
    };
    typedef typename _Alloc::template rebind<spinlock_type>::other spinlock_alloc_type;
    typedef std::vector<spinlock_type, spinlock_alloc_type > spinlock_set_type;
    typedef utils::spinlock::scoped_lock lock_type;
    
    typedef _Codec codec_type;
    typedef typename codec_type::buffer_type buffer_type;
    
    struct __specific_impl
    {
      cache_type cache;
      codec_type codec;
      buffer_type buffer;
      
      __specific_impl() {}
    };
    
  public:
    __block_file_byte_impl(const path_type& path)
      : base_type(path) { __initialize(); }
    
    off_type read(void* __buffer, size_type nbytes, off_type offset) const
    {
      // we will read from offset to offset + nbytes...
      // we will use chunk-vector like interface...

      // cast to bytes...
      byte_type* buffer = static_cast<byte_type*>(__buffer);
      
      off_type first = std::min(offset, filesize);
      off_type last = std::min(off_type(offset + nbytes), off_type(filesize));
      const off_type read_size = last - first;
      
      if (read_size <= 0) return 0;
      
      off_type  first_offset = first >> block_shift;
      size_type first_pos = first & block_mask;
      
      const off_type  last_offset = last >> block_shift;
      const size_type last_pos = last & block_mask;
      
      while (first_offset < last_offset) {
	
	block_type block(read(first_offset));
	
	const byte_type* biter = (block.get() + first_pos);
	const byte_type* biter_end = (block.get() + (1024 * 8));
	
	std::copy(biter, biter_end, buffer);
	
	buffer += biter_end - biter;
	++ first_offset;
	first_pos = 0;
      }
      
      block_type block(read(first_offset));
      std::copy(block.get() + first_pos, block.get() + last_pos, buffer);
      
      return read_size;
    }
    
  private:
    block_type read(const off_type offset) const
    {
      block_value_type block_value;
      {
	lock_type lock(__spinlock[offset & (__spinlock.size() - 1)].lock());
	block_value = __nodes[offset & (__nodes.size() - 1)];
      }
      
      if (block_value.first == offset && block_value.second)
	return block_value.second;
      
      block_value = __read(offset);
      
      if (__spinlock[offset & (__spinlock.size() - 1)].lock().try_lock()) {
	const_cast<block_value_type&>(__nodes[offset & (__nodes.size() - 1)]) = block_value;
	__spinlock[offset & (__spinlock.size() - 1)].lock().unlock();
      }
      
      return block_value.second;
    }
    
    block_value_type __read(const off_type offset) const 
    {
      if (! __cache.get())
	const_cast<self_type&>(*this).__cache.reset(new __specific_impl());
      
      std::pair<cache_iterator, bool> result = const_cast<cache_type&>(__cache->cache).find(offset);
      if (! result.second) {
	result.first->second.reset(new byte_type[(1024 * 8)]);
	
	if (offset < index.size()) {
	  const off_type first = (offset == 0 ? off_type(0) : index[offset - 1]);
	  const off_type last = index[offset];
	  
	  __cache->codec.decompress(data.begin() + first, last - first, __cache->buffer);
	  
	  // range checking,  but I don't think we need this...
	  std::copy(__cache->buffer.begin(),
		    __cache->buffer.begin() + std::min(__cache->buffer.size(), size_type(1024 * 8)),
		    result.first->second.get());
	}
      }
      return *(result.first);
    }
    
    void __initialize()
    {
      __spinlock.clear();
      __nodes.clear();
      __cache.reset();
      
      if (! is_compressed()) return;
      
      const size_type node_size = std::max(size_type(utils::bithack::next_largest_power2((data.size() / (1024 * 8)) >> 5)),
					   size_type(16));
      __nodes.reserve(node_size);
      __nodes.resize(node_size);
      
      const size_type spinlock_size = std::max(node_size >> 3, size_type(1));
      __spinlock.reserve(spinlock_size);
      __spinlock.resize(spinlock_size);
    }
    
  private:
    spinlock_set_type    __spinlock;
    block_value_set_type __nodes;
    
    boost::thread_specific_ptr<__specific_impl> __cache;
  };
  
  template <typename _Tp, typename _Codec, typename _Alloc, size_t _Size, size_t _ArcSize>
  class __block_file_impl
  {
  public:
    typedef _Tp    value_type;
    
    typedef typename _Alloc::template rebind<char>::other __impl_alloc_type;
    typedef __block_file_byte_impl<_Codec, __impl_alloc_type> impl_type;
    
    typedef typename impl_type::size_type       size_type;
    typedef typename impl_type::difference_type difference_type;
    typedef typename impl_type::off_type        off_type;
    typedef typename impl_type::byte_type       byte_type;
    typedef typename impl_type::path_type       path_type;
    
  public:
    static const bool      __node_is_power2 = utils::bithack::static_is_power2<_Size>::result;
    static const size_type __node_next_power2 = utils::bithack::static_next_largest_power2<_Size>::result;
    static const size_type node_size = (_Size == 0 ? size_type(1) : (__node_is_power2 ? _Size : __node_next_power2));
    static const size_type node_mask = node_size - 1;
    static const size_type node_shift = utils::bithack::static_bit_count<node_mask>::result;
    static const size_type node_byte_size = node_size * sizeof(value_type);
    
  public:
    typedef boost::shared_array<value_type> node_type;
    typedef std::pair<size_type, node_type> node_value_type;
    typedef typename _Alloc::template rebind<node_value_type>::other node_value_alloc_type;
    typedef utils::arc_list<size_type, node_type, _ArcSize, std::equal_to<size_type>, node_value_alloc_type > cache_type;
    typedef typename cache_type::iterator cache_iterator;

    typedef std::vector<node_value_type, node_value_alloc_type> node_value_set_type;
    
    struct spinlock_type
    {
      spinlock_type() {}
      spinlock_type(const spinlock_type&) {}
      spinlock_type& operator=(const spinlock_type&) { return *this; }
      
      utils::spinlock& lock() const { return const_cast<utils::spinlock&>(__lock); }
      utils::spinlock __lock;
    };
    typedef typename _Alloc::template rebind<spinlock_type>::other spinlock_alloc_type;
    typedef std::vector<spinlock_type, spinlock_alloc_type > spinlock_set_type;
    typedef utils::spinlock::scoped_lock lock_type;
    
    typedef __block_file_impl<_Tp,_Codec, _Alloc,_Size,_ArcSize> self_type;
    
    struct __specific_impl
    {
      cache_type cache;
      __specific_impl() {}
    };
    
  public:
    __block_file_impl(const path_type& path) : __impl(path) { __initialize(); }
    
    bool is_open() const { return __impl.is_open(); }
    bool is_compressed() const { return __impl.is_compressed(); }
    
    // only for non-compressed data...
    const value_type* begin() const { return static_cast<const value_type*>(__impl.begin()); }
    const value_type* end()   const { return static_cast<const value_type*>(__impl.end()); }
    
    size_type size() const { return __impl.size() / sizeof(value_type); }
    off_type file_size() const { return __impl.size(); }
    const path_type& path() const { return __impl.path(); }
    
    // use of compressed data...
    const value_type& operator[](size_type __pos) const
    { 
      return find(__pos).second[__pos & node_mask];
    }
    
    node_value_type find(size_type __pos) const
    {
      const size_type __node_offset = __pos >> node_shift;
      node_value_type node_value;
      
      {
	lock_type lock(__spinlock[__node_offset & (__spinlock.size() - 1)].lock());
	node_value = __nodes[__node_offset & (__nodes.size() - 1)];
      }
      
      if (node_value.first == __node_offset && node_value.second)
	return node_value;
      
      node_value = __find(__pos);
      
      if (__spinlock[__node_offset & (__spinlock.size() - 1)].lock().try_lock()) {
	const_cast<node_value_type&>(__nodes[__node_offset & (__nodes.size() - 1)]) = node_value;
	__spinlock[__node_offset & (__spinlock.size() - 1)].lock().unlock();
      }
	
      return node_value;
    }
    
    node_value_type __find(size_type __pos) const
    {
      if (! __cache.get())
	const_cast<self_type&>(*this).__cache.reset(new __specific_impl());
      
      const size_type __node_offset = __pos >> node_shift;
      
      std::pair<cache_iterator, bool> result = const_cast<cache_type&>(__cache->cache).find(__node_offset);
      if (! result.second) {
	result.first->second.reset(new value_type[node_size]);
	__impl.read(result.first->second.get(), node_byte_size, off_type(__node_offset) * node_byte_size);
      }
      return *(result.first);
    }

  private:
    void __initialize()
    {
      __spinlock.clear();
      __nodes.clear();
      __cache.reset();
      
      if (! __impl.is_compressed()) return;
      
      const size_type node_size = std::max(size_type(utils::bithack::next_largest_power2((__impl.data.size() / node_byte_size) >> 5)),
					   size_type(64));
      __nodes.reserve(node_size);
      __nodes.resize(node_size);
      
      const size_type spinlock_size = std::max(node_size >> 3, size_type(1));
      __spinlock.reserve(spinlock_size);
      __spinlock.resize(spinlock_size);
    }
    
  private:
    impl_type           __impl;
    spinlock_set_type   __spinlock;
    node_value_set_type __nodes;
    
    boost::thread_specific_ptr<__specific_impl> __cache;
  };
  
  template <typename _Impl>
  struct __block_file_iterator
  {
    typedef _Impl impl_type;
    
    typedef typename impl_type::value_type      value_type;
    typedef typename impl_type::size_type       size_type;
    typedef typename impl_type::difference_type difference_type;
    typedef const value_type&                   reference;
    typedef const value_type*                   pointer;
    
    typedef __block_file_iterator<_Impl>  self_type;
    
    typedef std::random_access_iterator_tag   iterator_category;

    typedef typename impl_type::node_value_type node_value_type;
    
    __block_file_iterator()
      : __pointer(), __pos(), __iter(), __impl() {}
    __block_file_iterator(pointer __x)
      : __pointer(__x), __pos(), __iter(), __impl() {}
    __block_file_iterator(size_type pos, const impl_type* impl)
      : __pointer(), __pos(pos), __iter(), __impl(impl) {}

    // access...
    reference operator*() const 
    { 
      if (__impl) {
	read();
	return __iter.second[__pos & impl_type::node_mask];
      } else
	return *__pointer;
    }
    pointer operator->() const
    { 
      if (__impl) {
	read();
	return &(__iter.second[__pos & impl_type::node_mask]);
      } else
	return __pointer;
    }
    
    // for faster computation (w/o branching) we increment/decrement both pointers...
    self_type& operator++()
    { 
      const bool __impl_empty = (__impl == 0);
      __pos += (! __impl_empty);
      __pointer += __impl_empty;
      return *this;
    }
    self_type& operator--()
    {
      const bool __impl_empty = (__impl == 0);
      __pos -= (! __impl_empty);
      __pointer -= __impl_empty;
      return *this;
    }
    
    self_type& operator+=(difference_type __n) 
    { 
      const bool __impl_empty = (__impl == 0);
      __pos += __n * (! __impl_empty);
      __pointer += __n * (__impl_empty);
      return *this;
    }
    self_type& operator-=(difference_type __n)
    { 
      const bool __impl_empty = (__impl == 0);
      __pos -= __n * (! __impl_empty);
      __pointer -= __n * (__impl_empty);
      return *this;
    }
    
    self_type operator++(int) { self_type __tmp = *this; ++ *this; return __tmp; }
    self_type operator--(int) { self_type __tmp = *this; -- *this; return __tmp; }
    
    self_type operator+(difference_type __n) const { self_type __tmp = *this; return __tmp += __n; }
    self_type operator-(difference_type __n) const { self_type __tmp = *this; return __tmp -= __n; }
    
    void read() const
    {
      if (! __impl) return;
      
      const size_type offset = __pos >> impl_type::node_shift;
      if (__iter.first != offset || ! __iter.second)
	const_cast<node_value_type&>(__iter) = __impl->find(__pos);
    }
    
    // for raw access...
    pointer __pointer;
    
    // for zlib-block access...
    size_type       __pos;
    node_value_type __iter;
    
    // underlying impl...
    const impl_type* __impl;
  };
  
  template <typename _Impl>
  inline bool
  operator==(const __block_file_iterator<_Impl>& x,
	     const __block_file_iterator<_Impl>& y)
  {
    return x.__impl == y.__impl && x.__pointer == y.__pointer && x.__pos == y.__pos;
  }
  
  template <typename _Impl>
  inline bool
  operator!=(const __block_file_iterator<_Impl>& x,
	     const __block_file_iterator<_Impl>& y)
  {
    return x.__impl != y.__impl || x.__pointer != y.__pointer || x.__pos != y.__pos;
  }
  
  
  template <typename _Impl>
  inline bool
  operator<(const __block_file_iterator<_Impl>& x,
	    const __block_file_iterator<_Impl>& y)
  {
    return (x.__impl == y.__impl
	    ? (x.__impl == 0
	       ? x.__pointer < y.__pointer
	       : x.__pos < y.__pos)
	    : x.__impl < y.__impl);
  }
  
  template <typename _Impl>
  inline bool
  operator>(const __block_file_iterator<_Impl>& x,
	    const __block_file_iterator<_Impl>& y)
  {
    return y < x;
  }
  
  
  template <typename _Impl>
  inline bool
  operator<=(const __block_file_iterator<_Impl>& x,
	     const __block_file_iterator<_Impl>& y)
  {
    return ! (y < x);
  }
  
  template <typename _Impl>
  inline bool
  operator>=(const __block_file_iterator<_Impl>& x,
	     const __block_file_iterator<_Impl>& y)
  {
    return ! (x < y);
  }

  template <typename _Impl>
  inline ptrdiff_t
  operator-(const __block_file_iterator<_Impl>& x,
	    const __block_file_iterator<_Impl>& y)
  {
    return (x.__impl == y.__impl 
	    ? (x.__impl == 0 ? x.__pointer - y.__pointer : x.__pos - y.__pos)
	    : x.__impl - y.__impl);
  }

  template <typename _Impl>
  inline __block_file_iterator<_Impl>
  operator+(ptrdiff_t __n, const __block_file_iterator<_Impl>& __x)
  {
    return __x + __n;
  }
  
  
  template <typename _Tp, typename _Codec, typename _Alloc=std::allocator<_Tp> >
  class block_file
  {
  public:
    typedef _Tp    value_type;
    typedef const value_type& const_reference;
    typedef const_reference   reference;
    
  private:
    // we use pre-fixed parameters...
    typedef __block_file_impl<_Tp, _Codec, _Alloc, 128 / sizeof(_Tp), 8> impl_type;
    
  public:
    typedef __block_file_iterator<impl_type> const_iterator;
    typedef const_iterator                   iterator;
    
  private:
    typedef typename impl_type::size_type  size_type;
    typedef typename impl_type::off_type   off_type;
    typedef typename impl_type::byte_type  byte_type;
    typedef typename impl_type::path_type  path_type;
    
  public:
    block_file(const path_type&   file) : pimpl(new impl_type(file)) { }
    block_file() {}
    
  public:
    const_reference front() const { return operator[](0); }
    const_reference back() const { return operator[](size() - 1); }
    const_reference operator[](size_type pos) const
    {
      return (pimpl->is_compressed()
	      ? pimpl->operator[](pos)
	      : *(pimpl->begin() + pos));
    }
    const_iterator begin() const
    { 
      return (pimpl->is_compressed()
	      ? const_iterator(0, &(*pimpl))
	      : const_iterator(pimpl->begin()));
    }
    const_iterator end() const
    { 
      return (pimpl->is_compressed()
	      ? const_iterator(pimpl->size(), &(*pimpl))
	      : const_iterator(pimpl->end()));
    }
    
    bool is_open() const { return pimpl && pimpl->is_open(); }
    bool empty() const { return ! is_open() || size() == 0; }
    size_type size() const { return pimpl->size(); }
    off_type file_size() const { return pimpl->file_size(); }
    
    const path_type& path() const { return pimpl->path(); }
    
    void open(const std::string& file) { open(path_type(file)); }
    void open(const path_type& file) { pimpl.reset(new impl_type(file)); }
    
    void close() { pimpl.reset(); }
    void clear() { close(); }
    
  private:
    boost::shared_ptr<impl_type> pimpl;
  };
};


#endif
