// -*- mode: c++ -*-

#ifndef __UTILS__MPI_STREAM__HPP__
#define __UTILS__MPI_STREAM__HPP__ 1

//
// basically, we maintain two streams, one to keep track of data size
// the other, data stream, by streaming data in a stream, meaning that larger data is split into chunk.
//

#include <string>
#include <vector>
#include <algorithm>

#include <boost/thread.hpp>

#include <mpi.h>

namespace utils
{
  struct __basic_mpi_stream_base
  {
    static const int tag_shift = 8;
    
    static const int tag_ack         = 0;
    static const int tag_size        = 1;
    static const int tag_buffer      = 2;
  };

  template <typename Alloc=std::allocator<char> >
  class basic_mpi_ostream
  {
  public:
    basic_mpi_ostream(MPI::Comm& comm, int rank, int tag, size_t buffer_size=4096) : pimpl(new impl()) { open(comm, rank, tag, buffer_size); }
    basic_mpi_ostream(int rank, int tag, size_t buffer_size=4096) : pimpl(new impl()) { open(rank, tag, buffer_size); }

  public:
    void open(MPI::Comm& comm, int rank, int tag, size_t buffer_size=4096) { pimpl->open(comm, rank, tag, buffer_size); }
    void open(int rank, int tag, size_t buffer_size=4096) { pimpl->open(MPI::COMM_WORLD, rank, tag, buffer_size); }
    
    basic_mpi_ostream& write(const std::string& data) { pimpl->write(data); return *this; }
    void close() { pimpl->close(); }
    
    bool test() { return pimpl->test(); }
    void wait() { pimpl->wait(); };
   
    void terminate() { pimpl->terminate(); }
    bool terminated() { return pimpl->terminated(); }

    operator bool() const { return pimpl->is_open(); }

  private:
    struct impl : public __basic_mpi_stream_base
    {
      typedef std::vector<char, Alloc> buffer_type;
      
      impl() : comm(0) {}
      ~impl() { close(); }

      void open(MPI::Comm& comm, int rank, int tag, size_t buffer_size=4096);
      void close();

      void write(const std::string& data);

      bool test();
      void wait();
      
      void terminate();
      bool terminated();

      bool is_open() const;

      MPI::Comm* comm;
      int        rank;
      int        tag;
      
      buffer_type  buffer;
      volatile int buffer_size;
      
      MPI::Prequest request_ack;
      MPI::Prequest request_size;
      MPI::Request  request_buffer;
    };

    boost::shared_ptr<impl> pimpl;
  };

  template <typename Alloc>
  inline
  void basic_mpi_ostream<Alloc>::impl::write(const std::string& data)
  {
    wait();
    
    buffer_size = data.size();
    buffer.clear();
    buffer.insert(buffer.end(), data.begin(), data.end());
    
    if (! buffer.empty())
      request_buffer = comm->Isend(&(*buffer.begin()), buffer.size(), MPI::CHAR, rank, (tag << tag_shift) | tag_buffer);
    
    request_size.Start();
    request_ack.Start();
  }
  
  template <typename Alloc>
  inline
  void basic_mpi_ostream<Alloc>::impl::terminate()
  {
    wait();
    
    if (buffer_size >= 0) {
      buffer.clear();
      buffer_size = -1;
      request_size.Start();
    }
  }
  
  template <typename Alloc>
  inline
  void basic_mpi_ostream<Alloc>::impl::open(MPI::Comm& __comm, int __rank, int __tag, size_t __buffer_size)
  {
    close();
    
    comm = &__comm;
    rank = __rank;
    tag  = __tag;
    
    buffer.clear();
    buffer_size = 0;
    
    request_ack  = comm->Recv_init(0, 0, MPI::INT, rank, (tag << tag_shift) | tag_ack);
    request_size = comm->Send_init(const_cast<int*>(&buffer_size), 1, MPI::INT, rank, (tag << tag_shift) | tag_size);
    
    request_ack.Start();
  }

  template <typename Alloc>
  inline
  void basic_mpi_ostream<Alloc>::impl::close()
  {
    if (comm) {
      if (! terminated())
	terminate();
      
      wait();
    }
    
    buffer.clear();
    buffer_size = -1;
    
    comm = 0;
    rank = 0;
    tag = 0;
  }
  
  
  template <typename Alloc>
  bool basic_mpi_ostream<Alloc>::impl::terminated()
  {
    return test() && buffer_size < 0;
  }
  
  template <typename Alloc>
  bool basic_mpi_ostream<Alloc>::impl::is_open() const
  {
    return comm;
  }
  
  template <typename Alloc>
  bool basic_mpi_ostream<Alloc>::impl::test()
  {
    if (buffer.empty()) {
      if (request_size.Test() && request_ack.Test())
	return true;
      else {
	request_ack.Test();
	request_size.Test();
	return false;
      }
    } else {
      if (request_size.Test() && request_ack.Test() && request_buffer.Test())
	return true;
      else {
	request_buffer.Test();
	request_ack.Test();
	request_size.Test();
	return false;
      }
    }
  }
  
  template <typename Alloc>
  void basic_mpi_ostream<Alloc>::impl::wait()
  {
    if (buffer.empty()) {
      while (! request_size.Test() || ! request_ack.Test()) {
	request_ack.Test();
	request_size.Test();
	boost::thread::yield();
      }
    } else {
      while (! request_size.Test() || ! request_ack.Test() || ! request_buffer.Test()) {
	request_buffer.Test();
	request_ack.Test();
	request_size.Test();
	boost::thread::yield();
      }
    }
  }
  
  template <typename Alloc=std::allocator<char> >
  class basic_mpi_istream
  {
  public:
    basic_mpi_istream(MPI::Comm& comm, int rank, int tag, size_t buffer_size=4096, bool no_ready=false)
      : pimpl(new impl()) { open(comm, rank, tag, buffer_size, no_ready); }
    basic_mpi_istream(int rank, int tag, size_t buffer_size=4096, bool no_ready=false)
      : pimpl(new impl()) { open(rank, tag, buffer_size, no_ready); }
    
  public:
    void open(MPI::Comm& comm, int rank, int tag, size_t buffer_size=4096, bool no_ready=false) { pimpl->open(comm, rank, tag, buffer_size, no_ready); }
    void open(int rank, int tag, size_t buffer_size=4096, bool no_ready=false) { pimpl->open(MPI::COMM_WORLD, rank, tag, buffer_size, no_ready); }

    basic_mpi_istream& read(std::string& data) { pimpl->read(data); return *this; }
    void close() { pimpl->close(); }

    bool test() { return pimpl->test(); }
    void wait() { pimpl->wait(); };
    void ready() { pimpl->ready(); }

    operator bool() const { return pimpl->is_open(); }

  private: 
    struct impl : public __basic_mpi_stream_base
    {
      typedef std::vector<char, Alloc> buffer_type;

      impl() : comm(0) {}
      ~impl() { close(); }

      void open(MPI::Comm& comm, int rank, int tag, size_t buffer_size=4096, bool __no_ready=false);
      void close();

      void read(std::string& data);

      bool test();
      void wait();
      void ready();

      bool is_open() const;

      MPI::Comm* comm;
      int        rank;
      int        tag;

      buffer_type  buffer;
      volatile int buffer_size;

      MPI::Prequest request_ack;
      MPI::Prequest request_size;
      MPI::Request  request_buffer;
      
      bool no_ready;
    };

    boost::shared_ptr<impl> pimpl;
  };


  template <typename Alloc>
  void basic_mpi_istream<Alloc>::impl::open(MPI::Comm& __comm, int __rank, int __tag, size_t __buffer_size, bool __no_ready)
  {
    close();

    comm = &__comm;
    rank = __rank;
    tag  = __tag;
    
    no_ready = __no_ready;
    
    buffer.clear();
    buffer_size = -1;
    
    request_size = comm->Recv_init(const_cast<int*>(&buffer_size), 1, MPI::INT, rank, (tag << tag_shift) | tag_size);
    request_ack  = comm->Send_init(0, 0, MPI::INT, rank, (tag << tag_shift) | tag_ack);
    
    request_size.Start();
    request_ack.Start();
  }

  template <typename Alloc>
  inline
  void basic_mpi_istream<Alloc>::impl::close()
  {
    if (comm)
      wait();
    
    buffer.clear();
    buffer_size = -1;
    
    comm = 0;
    rank = 0;
    tag = 0;
    
    no_ready = false;
  }

  template <typename Alloc>
  inline
  void basic_mpi_istream<Alloc>::impl::read(std::string& data)
  {
    wait();

    data.clear();
    
    if (buffer_size < 0)
      close();
    else {
      data.insert(data.end(), buffer.begin(), buffer.end());
      buffer.clear();
      buffer_size = -1;
      
      if (! no_ready) {
	request_size.Start();
	request_ack.Start();
      }
    }
  }
  
  template <typename Alloc>
  inline
  void basic_mpi_istream<Alloc>::impl::ready()
  {
    if (! no_ready) return;
    
    wait();
    
    request_size.Start();
    request_ack.Start();
  }
  
  template <typename Alloc>
  inline
  bool basic_mpi_istream<Alloc>::impl::is_open() const
  {
    return comm;
  }

  template <typename Alloc>
  inline
  bool basic_mpi_istream<Alloc>::impl::test()
  {
    if (! request_size.Test() || ! request_ack.Test()) {
      request_ack.Test();
      request_size.Test();
      return false;
    }
    
    if (buffer_size <= 0) return true;
    
    if (buffer.empty()) {
      buffer.resize(buffer_size);
      request_buffer = comm->Irecv(&(*buffer.begin()), buffer.size(), MPI::CHAR, rank, (tag << tag_shift) | tag_buffer);
    }
    
    return request_buffer.Test();
  }
  
  template <typename Alloc>
  inline
  void basic_mpi_istream<Alloc>::impl::wait()
  {
    while (! request_size.Test() || ! request_ack.Test()) {
      request_ack.Test();
      request_size.Test();
      
      boost::thread::yield();
    }
    
    if (buffer_size <= 0) return;
    
    if (buffer.empty()) {
      buffer.resize(buffer_size);
      request_buffer = comm->Irecv(&(*buffer.begin()), buffer.size(), MPI::CHAR, rank, (tag << tag_shift) | tag_buffer);
    }
    
    request_buffer.Wait();
  }


  // basic_mpi_{i,o}stream
  typedef basic_mpi_ostream<> mpi_ostream;
  typedef basic_mpi_istream<> mpi_istream;
};

#endif
