// -*- mode: c++ -*-

//
// taken from Bringing Practical Lock-Free Synchronization to 64-Bit Applications
//

#ifndef __UTILS__LOCKFREE_LIST_QUEUE__HPP__
#define __UTILS__LOCKFREE_LIST_QUEUE__HPP__ 1

#include <stdint.h>

#include <memory>
#include <algorithm>

#include <boost/thread.hpp>

#include <utils/memory.hpp>
#include <utils/atomicop.hpp>

namespace utils
{
  
  template <typename Tp, typename Alloc>
  struct __lockfree_list_queue_base
  {
    typedef Tp value_type;

    struct entry_tag_type
    {
      int32_t version;
      int32_t count;
      
      //entry_tag_type() : version(0), count(0) {}
      
      //void clear() { version = 0; count = 0; }
    };
    
    struct exit_tag_type
    {
      int32_t count;
      int32_t transfers_left:30;
      int32_t nl_p:1;
      int32_t to_be_freed:1;
      
      //exit_tag_type() : count(0), transfers_left(2), nl_p(false), to_be_freed(false) {}
      
      void clear()
      {
	count = 0;
	transfers_left = 2;
	nl_p = false;
	to_be_freed = false;
      }
      bool clean() { return count == 0 && transfers_left == 0; }
      bool freeable() { return clean() && nl_p && to_be_freed; }
    };

    template <typename __Tp>
    struct tag_holder
    {
      typedef __Tp    value_type;
      typedef int64_t integer_type;

      union {
	value_type   value;
	integer_type integer;
      } data;

      inline volatile value_type& value() volatile { return data.value; }
      inline const    value_type& value() const    { return data.value; }
      inline          value_type& value()          { return data.value; }
      
      inline volatile integer_type& integer() volatile { return data.integer; }
      inline const    integer_type& integer() const    { return data.integer; }
      inline          integer_type& integer()          { return data.integer; }
    };
    
    typedef tag_holder<entry_tag_type> entry_holder_type;
    typedef tag_holder<exit_tag_type>  exit_holder_type;
    
    
    struct node_type
    {
      value_type    value;
      
      volatile node_type*    next;
      volatile node_type*    pred;
      volatile exit_holder_type exit;
    };
    
    struct local_data_type
    {
      node_type* node;
      int32_t    version;
    };
    
    struct llsc_type
    {
      typedef node_type* pointer;
      
      volatile pointer        ptr0;
      volatile pointer        ptr1;
      volatile entry_holder_type entry;
      
      volatile pointer current(int32_t version) { return ((version & 0x01) ? ptr1 : ptr0); }
      volatile pointer* non_current_ptr(int32_t version) { return ((version & 0x01) ? &ptr0 : &ptr1); } 
    };
    
    typedef typename Alloc::template rebind<node_type>::other node_alloc_type;
  };
  
  
  template <typename Tp, typename Alloc=std::allocator<Tp> >
  class lockfree_list_queue : public __lockfree_list_queue_base<Tp, Alloc>::node_alloc_type
  {
  public:
    typedef Tp        value_type;
    typedef Tp*       pointer;
    typedef size_t    size_type;
    typedef ptrdiff_t difference_type;
    
  private:    
    typedef __lockfree_list_queue_base<Tp, Alloc> base_type;
    
    typedef typename base_type::entry_tag_type    entry_tag_type;
    typedef typename base_type::exit_tag_type     exit_tag_type;
    typedef typename base_type::entry_holder_type entry_holder_type;
    typedef typename base_type::exit_holder_type  exit_holder_type;
    
    typedef typename base_type::node_type       node_type;
    typedef typename base_type::local_data_type local_data_type;
    typedef typename base_type::llsc_type       llsc_type;
    typedef typename base_type::node_alloc_type node_alloc_type;
    
  public:
    lockfree_list_queue(size_type max_size=0) : head(), tail()
    {
      __size = 0;
      __max_size = max_size;

      tail.entry.value().version = 0;
      tail.entry.value().count = 0;
      
      node_init0 = allocate(value_type());
      node_init1 = allocate(value_type());
      
      tail.ptr0 = node_init0;
      tail.ptr1 = node_init1;
      
      tail.ptr0->exit.value().count = 0;
      tail.ptr0->exit.value().transfers_left = 2;
      tail.ptr0->exit.value().nl_p = false;
      tail.ptr0->exit.value().to_be_freed = false;
      tail.ptr0->pred = tail.ptr1;
      tail.ptr0->next = 0;
      
      tail.ptr1->exit.value().count = 0;
      tail.ptr1->exit.value().transfers_left = 0;
      tail.ptr1->exit.value().nl_p = false;
      tail.ptr1->exit.value().to_be_freed = false;
      
      // we do not have to set this...?
      tail.ptr1->pred = 0;
      tail.ptr1->next = 0;

      //std::cerr << "init: " << tail.ptr0 << " " << tail.ptr1 << std::endl;
      
      head = tail;
    }
    ~lockfree_list_queue()
    {
      
      node_type* nodes[4] = {head.ptr0, head.ptr1, node_init0, node_init1};
      std::sort(nodes, nodes + 4);
      
      if (nodes[0])
	deallocate(nodes[0]);
      if (nodes[1] && nodes[1] != nodes[0])
	deallocate(nodes[1]);
      if (nodes[2] && nodes[2] != nodes[1])
	deallocate(nodes[2]);
      if (nodes[3] && nodes[3] != nodes[2])
	deallocate(nodes[3]);      
    }

  private:
    lockfree_list_queue(const lockfree_list_queue&) {}
    lockfree_list_queue& operator=(const lockfree_list_queue&) {}
    
  public:
    bool push(const value_type& x, const bool no_wait=false)
    {
      if (no_wait)
	return __push(x);
      else {
	for (;;) {
	  for (int i = 0; i < 50; ++ i) {
	    if (__push(x))
	      return true;
	    else
	      boost::thread::yield();
	  }
	  __sleep_long();
	}
	return true;
      }
    }
    
    bool push_swap(value_type& x, const bool no_wait=false)
    {
      if (no_wait)
	return __push_swap(x);
      else {
	for (;;) {
	  for (int i = 0; i < 50; ++ i) {
	    if (__push_swap(x))
	      return true;
	    else
	      boost::thread::yield();
	  }
	  __sleep_long();
	}
	return true;
      }
    }
    
    bool pop(value_type& x, const bool no_wait=false)
    {
      if (no_wait)
	return __pop(x);
      else {
	for (;;) {
	  for (int i = 0; i < 50; ++ i) {
	    if (__pop(x))
	      return true;
	    else
	      boost::thread::yield();
	  }
	  __sleep_long();
	}
	return true;
      }
    }
    
    bool pop_swap(value_type& x, const bool no_wait=false)
    {
      if (no_wait)
	return __pop_swap(x);
      else {
	for (;;) {
	  for (int i = 0; i < 50; ++ i) {
	    if (__pop_swap(x))
	      return true;
	    else
	      boost::thread::yield();
	  }
	  __sleep_long();
	}
	return true;
      }
    }    
    
  private:
    void __sleep_long()
    {
      struct timespec tm;
      tm.tv_sec = 0;
      tm.tv_nsec = 2000001;
      nanosleep(&tm, NULL);
    }

    bool __push_swap(value_type& __value) {
      atomicop::memory_barrier();
      
      if (__max_size > 0 && __size == __max_size) return false;

      local_data_type local_data;
      node_type* node = allocate(value_type());
      using namespace boost;
      using namespace std;
      swap(node->value, __value);
      
      for (;;) {
	atomicop::memory_barrier();
	node_type* node_tail = ll_op(tail, local_data);
	node->pred = node_tail;
	
	if (atomicop::compare_and_swap((volatile void**) &(node_tail->next), (void*) 0, (void*) node)) {
	  sc_op(tail, node, local_data);
	  atomicop::fetch_and_add(__size, difference_type(1));
	  return true;
	} else
	  sc_op(tail, const_cast<node_type*>(node_tail->next), local_data);
      }
    }
    
    bool __push(const value_type& __value) {
      atomicop::memory_barrier();
      
      if (__max_size > 0 && __size == __max_size) return false;
      
      local_data_type local_data;
      node_type* node = allocate(__value);
      
      for (;;) {
	atomicop::memory_barrier();
	node_type* node_tail = ll_op(tail, local_data);
	node->pred = node_tail;
	
	if (atomicop::compare_and_swap((volatile void**) &(node_tail->next), (void*) 0, (void*) node)) {
	  sc_op(tail, node, local_data);
	  atomicop::fetch_and_add(__size, difference_type(1));
	  return true;
	} else
	  sc_op(tail, const_cast<node_type*>(node_tail->next), local_data);
      }
    }
    
    bool __pop_swap(value_type& __value) {
      local_data_type local_data;
      
      for (;;) {
	node_type* node_head = ll_op(head, local_data);
	node_type* node_next = const_cast<node_type*>(node_head->next);
	
	if (node_next == 0) {
	  //unlink_op(head, local_data);
	  release_op(local_data.node);
	  return false;
	}
	if (sc_op(head, node_next, local_data)) {
	  using namespace boost;
	  using namespace std;
	  swap(__value, node_next->value);
	  set_to_be_freed(node_next);
	  atomicop::fetch_and_add(__size, difference_type(-1));	  
	  return true;
	}
      }
    }
    
    bool __pop(value_type& __value) {
      local_data_type local_data;
      
      for (;;) {
	node_type* node_head = ll_op(head, local_data);
	node_type* node_next = const_cast<node_type*>(node_head->next);
	if (node_next == 0) {
	  //unlink_op(head, local_data);
	  release_op(local_data.node);
	  return false;
	}
	if (sc_op(head, node_next, local_data)) {
	  __value = node_next->value;
	  set_to_be_freed(node_next);
	  atomicop::fetch_and_add(__size, difference_type(-1));
	  return true;
	}
      }
    }

  private:

    node_type* ll_op(llsc_type& llsc, local_data_type& local_data)
    {
      entry_holder_type entry_prev;
      entry_holder_type entry_post;
      
      do {
	atomicop::memory_barrier();
	entry_prev.integer() = llsc.entry.integer();
	
	local_data.version = entry_prev.value().version;
	local_data.node = llsc.current(entry_prev.value().version);
	
	entry_post.value().version = entry_prev.value().version;
	entry_post.value().count   = entry_prev.value().count + 1;
	
      } while (! atomicop::compare_and_swap(llsc.entry.integer(), entry_prev.integer(), entry_post.integer()));
      
      return local_data.node;
    }
      
    bool sc_op(llsc_type& llsc, node_type* node, local_data_type& local_data) {
      atomicop::memory_barrier();
      node_type* pred_node = const_cast<node_type*>(local_data.node->pred);
      const bool success = atomicop::compare_and_swap((volatile void**) llsc.non_current_ptr(local_data.version), 
						      (void*) pred_node,
						      (void*) node);
      
      // in the original paper, if not success, we will free new node...?
      
      entry_holder_type entry_prev;
      entry_holder_type entry_post;
	
      for (;;) {
	atomicop::memory_barrier();
	entry_prev.integer() = llsc.entry.integer();
	
	if (entry_prev.value().version != local_data.version) break;
	
	entry_post.value().version = entry_prev.value().version + 1;
	entry_post.value().count = 0;
	
	if (atomicop::compare_and_swap(llsc.entry.integer(), entry_prev.integer(), entry_post.integer()))
	  transfer_op(local_data.node, entry_prev.value().count);
      }
      
      release_op(local_data.node);
	
      return success;
    }

    void unlink_op(llsc_type& llsc, local_data_type& local_data)
    {
      entry_holder_type entry_prev;
      entry_holder_type entry_post;
	
      for (;;) {
	atomicop::memory_barrier();
	entry_prev.integer() = llsc.entry.integer();
	
	if (entry_prev.value().version != local_data.value().version) break;
	  
	entry_post.value().version = entry_prev.value().version;
	entry_post.value().count   = entry_prev.value().count - 1;
	
	if (atomicop::compare_and_swap(llsc.entry.integer(), entry_prev.integer(), entry_post.integer())) return;
      }
      release_op(local_data.node);
    }
      
    void transfer_op(node_type* node, int count)
    {
      exit_holder_type exit_prev;
      exit_holder_type exit_post;
      
      do {
	atomicop::memory_barrier();
	exit_prev.integer() = node->exit.integer();
	
	exit_post.value().count          = exit_prev.value().count + count;
	exit_post.value().transfers_left = exit_prev.value().transfers_left - 1;
	exit_post.value().nl_p           = exit_prev.value().nl_p;
	exit_post.value().to_be_freed    = exit_prev.value().to_be_freed;
      } while (! atomicop::compare_and_swap(node->exit.integer(), exit_prev.integer(), exit_post.integer()));
    }
      
    void release_op(node_type* node)
    {
      atomicop::memory_barrier();
      node_type* node_pred = const_cast<node_type*>(node->pred);
      exit_holder_type exit_prev;
      exit_holder_type exit_post;
      do {
	atomicop::memory_barrier();
	exit_prev.integer() = node->exit.integer();
	  
	exit_post.value().count          = exit_prev.value().count - 1;
	exit_post.value().transfers_left = exit_prev.value().transfers_left;
	exit_post.value().nl_p           = exit_prev.value().nl_p;
	exit_post.value().to_be_freed    = exit_prev.value().to_be_freed;
      } while (! atomicop::compare_and_swap(node->exit.integer(), exit_prev.integer(), exit_post.integer()));
      
      atomicop::memory_barrier();
      if (exit_post.value().clean())
	set_nl_pred(node_pred);
      if (exit_post.value().freeable())
	deallocate(node);
    }
      
    void set_nl_pred(node_type* node)
    {
      // if returned true, then, we can free
      exit_holder_type exit_prev;
      exit_holder_type exit_post;
      do {
	atomicop::memory_barrier();
	exit_prev.integer() = node->exit.integer();
	
	exit_post.value().count          = exit_prev.value().count;
	exit_post.value().transfers_left = exit_prev.value().transfers_left;
	exit_post.value().nl_p           = true;
	exit_post.value().to_be_freed    = exit_prev.value().to_be_freed;
	
      } while (! atomicop::compare_and_swap(node->exit.integer(), exit_prev.integer(), exit_post.integer()));
      
      atomicop::memory_barrier();
      if (exit_post.value().freeable())
	deallocate(node);
    }
    
    void set_to_be_freed(node_type* node)
    {
      // if returned true, then, we can free
      exit_holder_type exit_prev;
      exit_holder_type exit_post;
      do {
	atomicop::memory_barrier();
	exit_prev.integer() = node->exit.integer();
	  
	exit_post.value().count          = exit_prev.value().count;
	exit_post.value().transfers_left = exit_prev.value().transfers_left;
	exit_post.value().nl_p           = exit_prev.value().nl_p;
	exit_post.value().to_be_freed    = true;
	
      } while (! atomicop::compare_and_swap(node->exit.integer(), exit_prev.integer(), exit_post.integer()));
      
      atomicop::memory_barrier();
      if (exit_post.value().freeable())
	deallocate(node);
    }

    
    // allocator and deallocator with constructor...
    
    node_type* allocate(const value_type& __value)
    {
      atomicop::memory_barrier();
      node_type* node = alloc().allocate(1);
      utils::construct_object(&(node->value), __value);
      node->next = 0;
      
      node->exit.value().count = 0;
      node->exit.value().transfers_left = 2;
      node->exit.value().nl_p = false;
      node->exit.value().to_be_freed = false;
      
      //utils::construct_object(&const_cast<exit_tag_type&>(node->exit.value()), exit_tag_type());
      
      //atomicop::fetch_and_add(__num_allocated, size_type(1));

      return node;
    }

    void deallocate(node_type* node)
    {
      atomicop::memory_barrier();
      utils::destroy_object(&(node->value));
      alloc().deallocate(node, 1);
      
      //atomicop::fetch_and_add(__num_deallocated, size_type(1));
    }
    
    inline const node_alloc_type& alloc() const { return static_cast<const node_alloc_type&>(*this); }
    inline       node_alloc_type& alloc()       { return static_cast<node_alloc_type&>(*this); }
    
  private:
    llsc_type head;
    llsc_type tail;
    
    node_type* node_init0;
    node_type* node_init1;
    
    volatile difference_type __size;
    size_type __max_size;
  };
};

#endif
