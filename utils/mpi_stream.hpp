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

#include <mpi.h>

namespace utils
{
  template <typename Alloc=std::allocator<char> >
  class basic_mpi_ostream
  {
  public:
    basic_mpi_ostream(MPI::Comm& comm, int rank, int tag, size_t buffer_size=4096) : pimpl(new impl()){ open(comm, rank, tag, buffer_size); }
    basic_mpi_ostream(int rank, int tag, size_t buffer_size=4096) : pimpl(new impl()){ open(rank, tag, buffer_size); }
    
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
    struct impl
    {
      typedef std::vector<char, Alloc> buffer_type;
      
      impl() {}
      
      void open(MPI::Comm& comm, int rank, int tag, size_t buffer_size=4096);
      void close();
      
      void write(const std::string& data);
      
      bool test();
      void wait();
      size_t flush();

      void terminate();
      bool terminated();
      
      bool is_open() const;
      
      buffer_type buffer;
      int         buffer_size;
      
      buffer_type buffer_send;
      int         buffer_send_size;
      
      MPI::Prequest request_ack;
      MPI::Prequest request_size;
      
      MPI::Prequest request_buffer;
      MPI::Prequest request_buffer_size;
    };
    
    boost::shared_ptr<impl> pimpl;
  };

  template <typename Alloc>
  inline
  void basic_mpi_ostream<Alloc>::impl::write(const std::string& data)
  {
    wait();
    
    buffer_size = data.size();
    buffer.insert(buffer.end(), data.begin(), data.end());
    
    request_size.Start();
    request_ack.Start();
    
    while (flush());
  }

  template <typename Alloc>
  inline
  void basic_mpi_ostream<Alloc>::impl::terminate()
  {
    wait();
    
    if (buffer_size >= 0) {
      buffer_size = -1;
      request_size.Start();
    }
    
    while (flush());
  }


  template <typename Alloc>
  inline
  void basic_mpi_ostream<Alloc>::impl::open(MPI::Comm& comm, int rank, int tag, size_t __buffer_size)
  {
    close();
    
    buffer.clear();
    buffer_size = 0;
    
    buffer_send.reserve(__buffer_size);
    buffer_send.resize(__buffer_size);
    buffer_send_size = 0;
    
    request_ack  = comm.Recv_init(0, 0, MPI::INT, rank, (tag << 8) | 0);
    request_size = comm.Send_init(&buffer_size, 1, MPI::INT, rank, (tag << 8) | 1);
    
    request_buffer      = comm.Send_init(&(*buffer_send.begin()), buffer_send.size(), MPI::CHAR, rank, (tag << 8) | 2);
    request_buffer_size = comm.Send_init(&buffer_send_size, 1, MPI::INT, rank, (tag << 8) | 3);
    
    request_ack.Start();
  }
  
  template <typename Alloc>
  inline
  void basic_mpi_ostream<Alloc>::impl::close()
  {
    request_size.Cancel();
    request_ack.Cancel();
    request_buffer.Cancel();
    request_buffer_size.Cancel();
    
    buffer.clear();
    buffer_send.clear();
  }

  template <typename Alloc>
  inline
  size_t basic_mpi_ostream<Alloc>::impl::flush()
  {
    if (! request_buffer_size.Test() || ! request_buffer.Test()) return 0;
    if (buffer.empty()) return 0;
    
    buffer_send_size = std::min(buffer_send.size(), buffer.size());
    std::copy(buffer.begin(), buffer.begin() + buffer_send_size, buffer_send.begin());
    buffer.erase(buffer.begin(), buffer.begin() + buffer_send_size);
    
    request_buffer_size.Start();
    request_buffer.Start();
    
    return buffer_send_size;
  }
  
  template <typename Alloc>
  bool basic_mpi_ostream<Alloc>::impl::terminated()
  {
    while (flush());
    return request_size.Test() && request_ack.Test() && buffer_size < 0 && buffer.empty() && request_buffer_size.Test() && request_buffer.Test();
  }
  
  template <typename Alloc>
  bool basic_mpi_ostream<Alloc>::impl::is_open() const
  {
    return ! buffer_send.empty();
  }
  
  template <typename Alloc>
  bool basic_mpi_ostream<Alloc>::impl::test()
  {
    while (flush());
    return request_size.Test() && request_ack.Test();
  }
  
  template <typename Alloc>
  void basic_mpi_ostream<Alloc>::impl::wait()
  {
    while (flush());
    
    if (! request_size.Test())
      request_size.Wait();
    if (! request_ack.Test())
      request_ack.Wait();
  }
  
  
  
  template <typename Alloc=std::allocator<char> >
  class basic_mpi_istream
  {
  public:
    basic_mpi_istream(MPI::Comm& comm, int rank, int tag, size_t buffer_size=4096) : pimpl(new impl()){ open(comm, rank, tag, buffer_size); }
    basic_mpi_istream(int rank, int tag, size_t buffer_size=4096) : pimpl(new impl()){ open(rank, tag, buffer_size); }
    
  public:
    void open(MPI::Comm& comm, int rank, int tag, size_t buffer_size=4096) { pimpl->open(comm, rank, tag, buffer_size); }
    void open(int rank, int tag, size_t buffer_size=4096) { pimpl->open(MPI::COMM_WORLD, rank, tag, buffer_size); }
    
    basic_mpi_istream& read(std::string& data) { pimpl->read(data); return *this; }
    void close() { pimpl->close(); }
    
    bool test() { return pimpl->test(); }
    void wait() { pimpl->wait(); };
    
    operator bool() const { return pimpl->is_open(); }
   
  private: 
    struct impl
    {
      typedef std::vector<char, Alloc> buffer_type;
      
      impl() {}
      
      void open(MPI::Comm& comm, int rank, int tag, size_t buffer_size=4096);
      void close();
      
      void read(std::string& data);
      
      bool test();
      void wait();
      size_t fill();
      
      bool is_open() const;
      
      buffer_type buffer;
      int         buffer_size;
      
      buffer_type buffer_recv;
      int         buffer_recv_size;
      
      MPI::Prequest request_ack;
      MPI::Prequest request_size;
      
      MPI::Prequest request_buffer;
      MPI::Prequest request_buffer_size;
    };
    
    boost::shared_ptr<impl> pimpl;
  };

  
  template <typename Alloc>
  void basic_mpi_istream<Alloc>::impl::open(MPI::Comm& comm, int rank, int tag, size_t __buffer_size)
  {
    close();
    
    buffer.clear();
    buffer_size = -1;
    
    buffer_recv.reserve(__buffer_size);
    buffer_recv.resize(__buffer_size);
    buffer_recv_size = 0;
    
    request_ack  = comm.Send_init(0, 0, MPI::INT, rank, (tag << 8) | 0);
    request_size = comm.Recv_init(&buffer_size, 1, MPI::INT, rank, (tag << 8) | 1);
    
    request_buffer      = comm.Recv_init(&(*buffer_recv.begin()), buffer_recv.size(), MPI::CHAR, rank, (tag << 8) | 2);
    request_buffer_size = comm.Recv_init(&buffer_recv_size, 1, MPI::INT, rank, (tag << 8) | 3);
    
    request_ack.Start();
    request_size.Start();
    request_buffer.Start();
    request_buffer_size.Start();
  }
  
  template <typename Alloc>
  inline
  void basic_mpi_istream<Alloc>::impl::close()
  {
    request_size.Cancel();
    request_ack.Cancel();
    request_buffer.Cancel();
    request_buffer_size.Cancel();
    
    buffer.clear();
    buffer_recv.clear();
  }

  template <typename Alloc>
  inline
  void basic_mpi_istream<Alloc>::impl::read(std::string& data)
  {
    data.clear();
    
    wait();
    
    while (! test());
    
    if (buffer_size < 0) {
      // do nothing?
      
      close();
      
    } else {
      data.insert(data.end(), buffer.begin(), buffer.begin() + buffer_size);
      buffer.erase(buffer.begin(), buffer.begin() + buffer_size);
      
      request_size.Start();
      request_ack.Start();
    }
  }

  template <typename Alloc>
  inline
  size_t basic_mpi_istream<Alloc>::impl::fill()
  {
    if (! request_buffer_size.Test() || ! request_buffer.Test())
      return 0;
    
    const int copy_size = buffer_recv_size;
    buffer.insert(buffer.end(), buffer_recv.begin(), buffer_recv.begin() + copy_size);
    
    request_buffer_size.Start();
    request_buffer.Start();
    
    return copy_size;
  }
  
  template <typename Alloc>
  inline
  bool basic_mpi_istream<Alloc>::impl::is_open() const
  {
    return ! buffer_recv.empty();
  }
  
  template <typename Alloc>
  inline
  bool basic_mpi_istream<Alloc>::impl::test()
  {
    while (fill());

    if (! request_ack.Test() || ! request_size.Test()) return false;
    if (buffer_size == 0 || buffer_size < 0) return true;
    
    while (fill());
    
    return buffer.size() >= buffer_size;
  }
  
  template <typename Alloc>
  inline
  void basic_mpi_istream<Alloc>::impl::wait()
  {
    while (fill());
    
    if (! request_ack.Test())
      request_ack.Wait();
    if (! request_size.Test())
      request_size.Wait();
    
    while (fill());
  }
  
  
  // basic_mpi_{i,o}stream
  typedef basic_mpi_ostream<> mpi_ostream;
  typedef basic_mpi_istream<> mpi_istream;
};


#endif
