#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.61)

AC_INIT(expgram, 0.0.1, taro.wtnb@gmail.com)

AC_CONFIG_AUX_DIR(config)

AM_INIT_AUTOMAKE(expgram, 0.0.1)

AC_CONFIG_HEADER(utils/config.h)

# Checks for programs.
AC_PROG_LIBTOOL
AC_PROG_CXX

AC_LANG_CPLUSPLUS

AC_SYS_LARGEFILE

# Checks for libraries.

AC_ARG_WITH(sparsehash,
	[AC_HELP_STRING([--with-sparsehash=DIR], [sparsehash in DIR])],
	[sparsehash_dir="${withval}"], [sparsehash_dir=''])

if test "x$sparsehash_dir" != "x"; then
  SPARSEHASH_CPPFLAGS="-I${sparsehash_dir}/include"
else
  SPARSEHASH_CPPFLAGS=""
fi
AC_SUBST(SPARSEHASH_CPPFLAGS)

AC_MSG_CHECKING(for sparsehash)
save_CXXFLAGS="$CXXFLAGS"
CXXFLAGS="$CXXFLAGS $SPARSEHASH_CPPFLAGS"
AC_TRY_LINK([#include <google/sparse_hash_map>], [], [ac_have_libsparsehash=yes], [ac_have_libsparsehash=no])
CXXFLAGS="$save_CXXFLAGS"
AC_MSG_RESULT($ac_have_libsparsehash)
if test "x$ac_have_libsparsehash" = "xno"; then
  AS_ERROR("no sparsehash")
fi

AC_ARG_WITH(tcmalloc,
	[AC_HELP_STRING([--with-tcmalloc=DIR], [tcmalloc in DIR])],
	[tcmalloc_dir="${withval}"], [tcmalloc_dir=''])

if test "x$tcmalloc_dir" != "x"; then
  TCMALLOC_CPPFLAGS="-I${tcmalloc_dir}/include"
  TCMALLOC_LDFLAGS="-L${tcmalloc_dir}/lib -ltcmalloc"
else
  TCMALLOC_CPPFLAGS=""
  TCMALLOC_LDFLAGS="-ltcmalloc"
fi
AC_SUBST(TCMALLOC_CPPFLAGS)
AC_SUBST(TCMALLOC_LDFLAGS)

AC_MSG_CHECKING(for tcmalloc)
save_LDFLAGS="$LDFLAGS"
LDFLAGS="$LDFLAGS $TCMALLOC_LDFLAGS"
AC_TRY_LINK([], [], [ac_have_libtcmalloc=yes], [ac_have_libtcmalloc=no])
LDFLAGS="$save_LDFLAGS"
AC_MSG_RESULT($ac_have_libtcmalloc)
if test "x$ac_have_libtcmalloc" = "xno"; then
  AS_ERROR("no tcmallo")
fi

## --with-profiler
AC_ARG_WITH(profiler,
	[AC_HELP_STRING([--with-profiler=DIR], [profiler in DIR])],
	[profiler_dir="${withval}"], [profiler_dir=''])

if test "x$profiler_dir" != "x"; then
  PROFILER_CPPFLAGS="-I${profiler_dir}/include"
  PROFILER_LDFLAGS="-L${profiler_dir}/lib -lprofiler"
else
  PROFILER_CPPFLAGS=""
  PROFILER_LDFLAGS="-lprofiler"
fi

AC_ARG_ENABLE(profiler,
	[AC_HELP_STRING([--enable-profiler], [enable profiling via google's libprofiler])],
	[ac_enable_profiler=yes], [ac_enable_profiler=no])

if test "x$ac_enable_profiler" = "xyes"; then
   AC_MSG_CHECKING(for -lprofiler)
   save_LDFLAGS="$LDFLAGS"
   LDFLAGS="$LDFLAGS $PROFILER_LDFLAGS"
   AC_TRY_LINK([],[],[ac_have_libprofiler=yes],[ac_have_libprofiler=no])
   LDFLAGS="$save_LDFLAGS"
   AC_MSG_RESULT($ac_have_libprofiler)
   if test "x$ac_have_libprofiler" = "xno"; then
     PROFILER_CPPFLAGS=""
     PROFILER_LDFLAGS=""
   fi
else
  PROFILER_CPPFLAGS=""
  PROFILER_LDFLAGS=""
fi
AC_SUBST(PROFILER_CPPFLAGS)
AC_SUBST(PROFILER_LDFLAGS)

## --with-hunspell
AC_ARG_WITH(hunspell,
	[AC_HELP_STRING([--with-hunspell=DIR], [hunspell in DIR])],
	[hunspell_dir="${withval}"], [hunspell_dir=''])
hunspell_versions="1.4 1.3 1.2 1.1"

for hunspell_version in $hunspell_versions; do
  if test "x$hunspell_dir" != "x"; then
    HUNSPELL_CPPFLAGS="-I${hunspell_dir}/include"
    HUNSPELL_LDFLAGS="-L${hunspell_dir}/lib -lhunspell-${hunspell_version}"
  else
    HUNSPELL_CPPFLAGS=""
    HUNSPELL_LDFLAGS="-lhunspell-${hunspell_version}"
  fi

  AC_MSG_CHECKING(for -lhunspell-${hunspell_version})
  save_LDFLAGS="$LDFLAGS"
  LDFLAGS="$LDFLAGS $HUNSPELL_LDFLAGS"
  AC_TRY_LINK([], [], [ac_have_libhunspell=yes], [ac_have_libhunspell=no])
  LDFLAGS="$save_LDFLAGS"
  AC_MSG_RESULT($ac_have_libhunspell)

  if test "x$ac_have_libhunspell" = "xyes"; then
    AC_SUBST(HUNSPELL_CPPFLAGS)
    AC_SUBST(HUNSPELL_LDFLAGS)
    break
  fi
done
   
#if test "x$ac_have_libhunspell" = "xno"; then
#  AS_ERROR("no hunspell...")
#fi

# Boost libraries...
BOOST_REQUIRE([1.38])
BOOST_PROGRAM_OPTIONS
BOOST_THREADS
BOOST_FILESYSTEM
BOOST_IOSTREAMS
BOOST_SYSTEM

## save pthred-flag, computed by-product of boost-threads
PTHREAD_FLAGS=$boost_cv_pthread_flag
AC_SUBST(PTHREAD_FLAGS)

##
## check for the pthread_spin_lock
##
AC_MSG_CHECKING(POSIX thread spin lock)
orig_CXXFLAGS="$CXXFLAGS"
CXXFLAGS="$orig_CXXFLAGS $PTHREAD_FLAGS"
AC_TRY_LINK([#include <pthread.h>],
        [pthread_spinlock_t spin;
        pthread_spin_init(&spin, PTHREAD_PROCESS_SHARED);
        pthread_spin_lock(&spin); pthread_spin_unlock(&spin);
        pthread_spin_destroy(&spin);],
        [pthread_spin=yes], [pthread_spin=no])
AC_MSG_RESULT($pthread_spin)
if test "x$pthread_spin" = "xyes"; then
   AC_DEFINE(HAVE_PTHREAD_SPINLOCK, 1, [Define if you have spinlock])
fi
CXXFLAGS="$orig_CXXFLAGS"

###
###  check for Mac OS X SpinLOck
### 
AC_CHECK_HEADERS([libkern/OSAtomic.h])

AC_CHECKING(OSSpinLock)
AC_TRY_LINK([#include <libkern/OSAtomic.h>],
	    [OSSpinLock spin = OS_SPINLOCK_INIT;
	     OSSpinLockLock(&spin);
	     OSSpinLockUnlock(&spin);],
	    [osspinlock=yes], [osspinlock=no])
AC_MSG_RESULT($osspinlock)
if test "$osspinlock" = "yes"; then
  AC_DEFINE(HAVE_OSSPINLOCK, 1, [Define if you have Mac OS X SpinLock])
fi


### check for ICU libs
AC_PATH_PROG(ICU_CONFIG, icu-config, no)
if test "x$ICU_CONFIG" = "xno"; then
  AS_ERROR("no icu...")
fi

AC_MSG_CHECKING(ICU cppflags)
ICU_CPPFLAGS=`$ICU_CONFIG --cppflags`
AC_MSG_RESULT($ICU_CPPFLAGS)
AC_SUBST(ICU_CPPFLAGS)

AC_MSG_CHECKING(ICU ldflags)
ICU_LDFLAGS=`$ICU_CONFIG --ldflags`
AC_MSG_RESULT($ICU_LDFLAGS)
AC_SUBST(ICU_LDFLAGS)

### check for MPI
AC_PATH_PROGS(MPI_CXX_COMPILER, [mpic++ mpicxx mpiCC], no)
if test "x$MPI_CXX_COMPILER" = "xno"; then
  AS_ERROR("no mpi...")
fi 

AC_MSG_CHECKING(mpi cppflags)
MPI_CPPFLAGS=`$MPI_CXX_COMPILER -showme | awk '{for(i=1;i<NF;i++){if($i~/^-I/ || $i~/^-D/){printf "%s ", $i;}}}'`
AC_MSG_RESULT($MPI_CPPFLAGS)
AC_SUBST(MPI_CPPFLAGS)

AC_MSG_CHECKING(mpi ldflags)
MPI_LDFLAGS=`$MPI_CXX_COMPILER -showme | awk '{for(i=1;i<NF;i++){if($i~/^-L/ || $i~/^-l/ || $i~/^-Wl/){printf "%s ", $i;}}}'`
AC_MSG_RESULT($MPI_LDFLAGS)
AC_SUBST(MPI_LDFLAGS)

AC_MSG_CHECKING([for __thread])
AC_LINK_IFELSE([AC_LANG_PROGRAM([#if defined(__GNUC__) && (defined(__i386__) || defined(__x86_64__)) && ((__GNUC__ < 4) || (__GNUC__ == 4 && __GNUC_MINOR__ < 1) || (__GNUC__ == 4 && __GNUC_MINOR__ == 1 && __GNUC_PATCHLEVEL__ < 2))
#error gcc has this bug: http://gcc.gnu.org/ml/gcc-bugs/2006-09/msg02275.html
#endif], [static __thread int p = 0])],
               [AC_DEFINE(HAVE_TLS, 1, Define to 1 if compiler supports __thread)
                AC_MSG_RESULT([yes])],
               [AC_MSG_RESULT([no])])

# Checks for header files.
AC_HEADER_DIRENT
AC_HEADER_STDC
AC_CHECK_HEADERS([fcntl.h stdint.h stdlib.h string.h sys/time.h unistd.h])

# hasn_{map,set}
AC_CHECK_HEADERS([tr1/unordered_map tr1/unordered_set ext/hash_map ext/hash_set hash_map hash_set])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_CONST
AC_C_INLINE
AC_TYPE_INT16_T
AC_TYPE_INT32_T
AC_TYPE_INT64_T
AC_TYPE_INT8_T
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_HEADER_TIME
AC_STRUCT_TM
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T
AC_C_VOLATILE
AC_CHECK_TYPES([ptrdiff_t])

# Checks for library functions.
AC_FUNC_CLOSEDIR_VOID
AC_FUNC_LSTAT
AC_FUNC_LSTAT_FOLLOWS_SLASHED_SYMLINK
AC_FUNC_MMAP
AC_FUNC_SELECT_ARGTYPES
AC_FUNC_STAT
AC_CHECK_FUNCS([getpagesize gettimeofday memset munmap pow select sqrt strcasecmp strerror])

AC_CONFIG_FILES([Makefile
		 codec/Makefile
                 expgram/Makefile
		 progs/Makefile
		 scripts/Makefile
		 stemmer/Makefile
                 succinct_db/Makefile
                 utils/Makefile])
AC_OUTPUT
