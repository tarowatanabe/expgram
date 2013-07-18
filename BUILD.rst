Building expgram
================

Souce Codes
-----------

Get the source code from `expgram <...>`_.

::

  git clone ...

Or, grab the latest tar archive from...


Compile
-------

::

   ./autogen.sh (required when you get the code by git clone)
   ./configure
   make
   make install (optional)

You can set several options. For details see the dependencies section.
::

  --enable-jemalloc       enable jemalloc
  --enable-tcmalloc       enable tcmalloc
  --enable-profiler       enable profiling via google's libprofiler
  --enable-static-boost   Prefer the static boost libraries over the shared
                          ones [no]

  --with-jemalloc=DIR     jemalloc in DIR
  --with-tcmalloc=DIR     tcmalloc in DIR
  --with-profiler=DIR     profiler in DIR
  --with-boost=DIR        prefix of Boost 1.42 [guess]


Dependencies
------------

expgram is dependent on `boost library <http://boost.org>`_. The
minimum requirement is boost version 1.42.
Optionally, following libraries are recommended:

  - MPI impllementation

    We strongly recommends `open mpi <http://www.open-mpi.org>`_
    which is regularly tested by the author.
    The MPI libraries are automatically detected by the `configure`
    script by finding either mpic++, mpicxx or mpiCC. Thus, those mpi
    specific compilers should be on the path.

  - Fast malloc replacement (recommended for Linux)

    In general, Linux is very slow for malloc and it is recommended
    to link with a faster malloc implementation. Some of the
    recommended mallocs are:

     - `jemalloc <http://www.canonware.com/jemalloc/>`_
     - `gperftools (tcmalloc) <http://code.google.com/p/gperftools/>`_

    They are configured by --with-{jemalloc,tcmalloc} and should be
    enabled using --enable-{jemalloc,tcmalloc}
