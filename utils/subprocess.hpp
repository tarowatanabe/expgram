// -*- mode: c++ -*-

#ifndef __UTILS__SUBPROCESS__HPP__
#define __UTILS__SUBPROCESS__HPP__ 1

#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>

#include <cerrno>
#include <cstring>

#include <string>
#include <stdexcept>

namespace utils
{
  
  class subprocess
  {
    
  public:
    subprocess(const std::string& command)
      : __pid(-1), __pread(-1), __pwrite(-1) { open(command); }
    ~subprocess() { close(); }
  private:
    subprocess(const subprocess& x) {}
    subprocess& operator=(const subprocess& x) { return *this; }
    
  public:
    int desc_read() const { return __pread; }
    int desc_write() const { return __pwrite; }
    

    void open(const std::string& command)
    {
      int pipe_exec[2] = {-1, -1};
      
      if (::pipe(pipe_exec) < 0)
	throw std::runtime_error(std::string("pipe(): ") + strerror(errno));
      
      try {
	__close_on_exec(pipe_exec[0]);
	__close_on_exec(pipe_exec[1]);
      }
      catch (const std::exception& err) {
	::close(pipe_exec[0]);
	::close(pipe_exec[1]);
	throw err;
      }
      
      if (fork() == 0) {
	// child process
	
	// execlp
	::execlp(command.c_str(), command.c_str(), 0);
	
	// we will reach here if execlp failed...
	int error = errno;
	::write(pipe_exec[1], &error, sizeof(error));
	
	::close(pipe_exec[1]);
	::close(pipe_exec[0]);
	
	pipe_exec[0] = -1;
	pipe_exec[1] = -1;
	
	::_exit(error);  // not exit(error)!
      } else {
	// parent process
	::close(pipe_exec[1]);
	pipe_exec[1] = -1;
	
	int error = 0;
	switch (::read(pipe_exec[0], &error, sizeof(error))) {
	case  0: break; // success!
	case -1:
	  ::close(pipe_exec[0]);
	  pipe_exec[0] = -1;
	  throw std::runtime_error(std::string("read(): ") + strerror(errno));
	  break;
	default:
	  ::close(pipe_exec[0]);
	  pipe_exec[0] = -1;
	  throw std::runtime_error(std::string("read(): ") + strerror(error));
	  break; // error in child...
	}
	
	::close(pipe_exec[0]);
	pipe_exec[0] = -1;
      }
    }
    
    pid_t fork()
    {
      int pin[2] = {-1, -1};
      int pout[2] = {-1, -1};
    
      if (::pipe(pin) < 0)
	throw std::runtime_error(std::string("pipe(): ") + strerror(errno));
      if (::pipe(pout) < 0) {
	::close(pin[0]);
	::close(pin[1]);
	throw std::runtime_error(std::string("pipe(): ") + strerror(errno));
      }
    
      const pid_t pid = ::fork();
      if (pid < 0) {
	::close(pin[0]);
	::close(pin[1]);
	::close(pout[0]);
	::close(pout[1]);
	throw std::runtime_error(std::string("fork(): ") + strerror(errno));
      }
      
      if (pid == 0) {
	// child process...
	// redirect input...
	::close(pin[1]);
	::dup2(pin[0], STDIN_FILENO);
	::close(pin[0]);
	
	// redirect output...
	::close(pout[0]);
	::dup2(pout[1], STDOUT_FILENO);
	::close(pout[1]);
      } else {
	// parent process...
	::close(pin[0]);
	::close(pout[1]);
	
	__pid = pid;
	__pread = pout[0];
	__pwrite = pin[1];
      }
    }
    
    void close() {
      if (__pwrite >= 0) {
	::close(__pwrite);
	__pwrite = -1;
      }
      
      if (__pread >= 0) {
	::close(__pread);
	__pread  = -1;
      }
      
      if (__pid >= 0) {
	int status = 0;
	const int result = ::waitpid(__pid, &status, 0);
	__pid = -1;
      }
    }
    
    void __close_on_exec(int fd)
    {
      int flags = ::fcntl(fd, F_GETFD);
      if (flags == -1)
	throw std::runtime_error(std::string("fcntl(): ") + strerror(errno));
      flags |= FD_CLOEXEC;
      if (::fcntl(fd, F_SETFD, flags) == -1)
	throw std::runtime_error(std::string("fcntl(): ") + strerror(errno));
    }
    
  public:
    pid_t __pid;
    int   __pread;
    int   __pwrite;
  };
  
};

#endif
