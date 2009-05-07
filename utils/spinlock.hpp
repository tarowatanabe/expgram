// -*- mode: c++ -*-

#ifndef __UTILS__SPINLOCK__HPP__
#define __UTILS__SPINLOCK__HPP__ 1

#include <stdexcept>
#include <cassert>
#include <errno.h>
#include <pthread.h>

#include <boost/thread/detail/config.hpp>
#include <boost/utility.hpp>

#include <boost/thread/locks.hpp>
#include <boost/thread/exceptions.hpp>

#include <utils/config.hpp>

#ifdef HAVE_LIBKERN_OSATOMIC_H
  #include <libkern/OSAtomic.h>
#endif

namespace utils
{
  class spinlock : private boost::noncopyable
  {
  public:
    typedef boost::unique_lock<spinlock> scoped_lock;
    
    spinlock() : m_spinlock()
    {
#ifdef HAVE_OSSPINLOCK
      m_spinlock = OS_SPINLOCK_INIT;
#else
  #ifdef HAVE_PTHREAD_SPINLOCK
      const int res = pthread_spin_init(&m_spinlock, PTHREAD_PROCESS_SHARED);
  #else
      const int res = pthread_mutex_init(&m_spinlock, 0);
  #endif
      if (res != 0)
	throw boost::thread_resource_error();
#endif
    }
    ~spinlock() { 
#ifdef HAVE_OSSPINLOCK
      // do nothing...
#else
  #ifdef HAVE_PTHREAD_SPINLOCK
      const int res = pthread_spin_destroy(&m_spinlock);
  #else
      const int res = pthread_mutex_destroy(&m_spinlock);
  #endif
      assert(res == 0);
#endif
    }
    
  public:
    bool try_lock()
    {
#ifdef HAVE_OSSPINLOCK
      return OSSpinLockTry(&m_spinlock);
#else
  #ifdef HAVE_PTHREAD_SPINLOCK
      const int res = pthread_spin_trylock(&m_spinlock);
  #else
      const int res = pthread_mutex_trylock(&m_spinlock);
  #endif
      return ! res;
#endif
    }
    void lock()
    {
#ifdef HAVE_OSSPINLOCK
      OSSpinLockLock(&m_spinlock);
#else
  #ifdef HAVE_PTHREAD_SPINLOCK
      const int res = pthread_spin_lock(&m_spinlock);
  #else
      const int res = pthread_mutex_lock(&m_spinlock); 
  #endif
      if (res == EDEADLK)
	throw boost::lock_error();
      assert(res == 0);
#endif
    }
    void unlock()
    {
#ifdef HAVE_OSSPINLOCK
      OSSpinLockUnlock(&m_spinlock);
#else
  #ifdef HAVE_PTHREAD_SPINLOCK
      const int res = pthread_spin_unlock(&m_spinlock);
  #else
      const int res = pthread_mutex_unlock(&m_spinlock);
  #endif
      if (res == EPERM)
	throw boost::lock_error();
      assert(res == 0);
#endif
    }
    
  private:
#ifdef HAVE_OSSPINLOCK
    OSSpinLock m_spinlock;
#else
  #ifdef HAVE_PTHREAD_SPINLOCK
    pthread_spinlock_t m_spinlock;
  #else
    pthread_mutex_t m_spinlock;
  #endif
#endif
  };
};

#endif
