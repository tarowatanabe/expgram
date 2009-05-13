// -*- mode: c++ -*-

//
// temporry file management
//

#ifndef __UTILS_TEMPFILE__HPP__
#define __UTILS_TEMPFILE__HPP__ 1

#include <iostream>

#include <signal.h>

#include <sys/types.h>
#include <sys/stat.h>

#include <unistd.h>
#include <errno.h>
#include <cstdlib>

#include <string>
#include <vector>
#include <algorithm>
#include <stdexcept>

#include <boost/thread/mutex.hpp>
#include <boost/filesystem.hpp>
#include <utils/filesystem.hpp>

//
// do we remove singnal installer...?
// we will install signal when tempfile object is created (as static?)
//

namespace utils
{
  class tempfile
  {
  public:
    typedef boost::filesystem::path   path_type;

  private:    
    typedef struct sigaction sigaction_type;
    
    typedef std::vector<path_type>    file_set_type;
    typedef std::vector<sigaction_type> handle_set_type;
    
    typedef boost::mutex              mutex_type;
    typedef boost::mutex::scoped_lock lock_type;

    static mutex_type mutex;
    
    
    // file management...
    struct __signal_blocker
    {
      __signal_blocker()
      {
	sigemptyset(&mask);
	
	sigaddset(&mask, SIGHUP);
	sigaddset(&mask, SIGINT);
	sigaddset(&mask, SIGQUIT);
	sigaddset(&mask, SIGILL);
	sigaddset(&mask, SIGABRT);
	sigaddset(&mask, SIGKILL);
	sigaddset(&mask, SIGSEGV);
	sigaddset(&mask, SIGTERM);
	sigaddset(&mask, SIGBUS);
	
	sigprocmask(SIG_BLOCK, &mask, &mask_saved);
      }
      
      ~__signal_blocker()
      {
	sigprocmask(SIG_SETMASK, &mask_saved, 0);
      }

      sigset_t mask;
      sigset_t mask_saved;
    };

    class __files_type
    {
      file_set_type files;
      
    public:
      __files_type() {}
      ~__files_type() throw() { clear(); }
      
      void insert(const path_type& file)
      { 
	if (file.empty()) return;
	
	__signal_blocker __block;
	
	lock_type lock(mutex);
	
	file_set_type::iterator fiter = std::find(files.begin(), files.end(), file);
	if (fiter == files.end())
	  files.push_back(file);
      }
      
      void erase(const path_type& file)
      {
	if (file.empty()) return;
	
	__signal_blocker __block;
	
	lock_type lock(mutex);
	
	while (! files.empty()) {
	  file_set_type::iterator fiter = std::find(files.begin(), files.end(), file);
	  if (fiter == files.end()) break;
	  files.erase(fiter);
	}
      }
      
      void clear()
      {
	__signal_blocker __block;
	
	lock_type lock(mutex);
	
	if (files.empty()) return;
	
	for (file_set_type::const_iterator riter = files.begin(); riter != files.end(); ++ riter) {
	  try {
	    if (boost::filesystem::exists(*riter))
	      utils::filesystem::remove_all(*riter);
	    errno = 0;
	  }
	  catch (...) { }
	} 
	files.clear();
      }
    };
    
    // signal management...
    struct __signals_type
    {
      __signals_type() {}
      
      handle_set_type handles;
    };
    
    static __files_type   __files;
    static __signals_type __signals;
    
    // callback functions...
    static void callback(int sig) throw()
    {
      // actual clear...
      __files.clear();
      
      // is this safe without mutex???
      const handle_set_type& handles = __signals.handles;
      ::sigaction(sig, &handles[sig], 0);
      ::kill(getpid(), sig);
    }

    struct __signal_installer
    {
      __signal_installer() 
      {
	handle_set_type& handles = __signals.handles;
      
	int sig_max = SIGHUP;
	sig_max = std::max(sig_max, SIGINT);
	sig_max = std::max(sig_max, SIGQUIT);
	sig_max = std::max(sig_max, SIGILL);
	sig_max = std::max(sig_max, SIGABRT);
	sig_max = std::max(sig_max, SIGKILL);
	sig_max = std::max(sig_max, SIGSEGV);
	sig_max = std::max(sig_max, SIGTERM);
	sig_max = std::max(sig_max, SIGBUS);
      
	handles.reserve(sig_max);
	handles.resize(sig_max);
      
	sigaction_type sa;
	sa.sa_handler = callback;
	sa.sa_flags = SA_RESTART;
	sigemptyset(&sa.sa_mask);
      
	::sigaction(SIGHUP, &sa, &handles[SIGHUP]);
	::sigaction(SIGINT, &sa, &handles[SIGINT]);
	::sigaction(SIGQUIT, &sa, &handles[SIGQUIT]);
	::sigaction(SIGILL, &sa, &handles[SIGILL]);
	::sigaction(SIGABRT, &sa, &handles[SIGABRT]);
	::sigaction(SIGKILL, &sa, &handles[SIGKILL]);
	::sigaction(SIGSEGV, &sa, &handles[SIGSEGV]);
	::sigaction(SIGTERM, &sa, &handles[SIGTERM]);
	::sigaction(SIGBUS, &sa, &handles[SIGBUS]);
      }
    };

    static void install_signal() {
      static __signal_installer __installed;
    }
    
  public:
    
    // expose only two funcs...
    
    static void insert(const path_type& path)
    {
      install_signal();
      __files.insert(path);
    }
    
    static void erase(const path_type& path)
    {
      install_signal();
      __files.erase(path);
    }
    
    static path_type tmp_dir()
    {
      // wired to /tmp... is this safe?
      std::string tmpdir("/tmp");
      
      const char* tmpdir_env = getenv("TMPDIR");
      if (! tmpdir_env)
	return path_type(tmpdir);
      
      const path_type tmpdir_env_path(tmpdir_env);
      if (boost::filesystem::exists(tmpdir_env_path) && boost::filesystem::is_directory(tmpdir_env_path))
	return tmpdir_env_path;
      else
	return path_type(tmpdir);
    }

    static path_type file_name(const std::string& file)
    {
      std::vector<char, std::allocator<char> > buffer(file.size() + 1, 0);
      std::copy(file.begin(), file.end(), buffer.begin());
      
      int fd = ::mkstemp(&(*buffer.begin()));
      if (fd < 0)
	throw std::runtime_error(std::string("mkstemp failure: ") + ::strerror(errno));
      ::close(fd);
      
      return path_type(std::string(buffer.begin(), buffer.begin() + file.size()));
    }
    
    static path_type directory_name(const std::string& dir)
    {
      std::vector<char, std::allocator<char> > buffer(dir.size() + 1, 0);
      std::copy(dir.begin(), dir.end(), buffer.begin());
      
      char* tmp = ::mkdtemp(&(*buffer.begin()));
      if (! tmp)
	throw std::runtime_error(std::string("mkdtemp failure: ") + ::strerror(errno));
      
      return path_type(std::string(buffer.begin(), buffer.begin() + dir.size()));
    }
    
    static path_type file_name(const path_type& file) { return file_name(file.file_string()); }
    static path_type directory_name(const path_type& file) { return directory_name(file.file_string()); }

    static void permission(const path_type& path)
    {
      struct stat buf;
      
      if (::stat(path.file_string().c_str(), &buf) != 0) return;
      
      ::chmod(path.file_string().c_str(), buf.st_mode | S_IRUSR | S_IRGRP | S_IROTH);
    }
  };
};

#endif
