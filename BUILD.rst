Building expgram
================

Get the cutting-edge source code from `github.com <http://github.com/tarowatanabe/expgram>`_:

.. code:: bash

  git clone https://github.com/tarowatanabe/expgram.git

Or, grab the stable tar archive from `expgram <http://www2.nict.go.jp/univ-com/multi_trans/expgram>`_.

Compile
-------

.. code:: bash

   ./autogen.sh (required when you get the code by git clone)
   ./configure
   make
   make install (optional)

You can set several configure options. For details see the dependencies section.
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

In addition to configuration options, it is better to set ``-O3`` for
the ``CFLAGS`` and ``CXXFLAGS`` environment variables for faster
execution, when compiled by gcc and clang.


Dependencies
------------

expgram is dependent on `boost library <http://boost.org>`_. The
minimum requirement is boost version 1.42.
Optionally, following libraries are recommended:

- MPI implementation

  We strongly recommend `open mpi <http://www.open-mpi.org>`_
  which is regularly tested by the author.
  The MPI libraries are automatically detected by the `configure`
  script by finding either `mpic++`, `mpicxx` or `mpiCC`. Thus, those
  mpi specific compilers should be on the executable path.

- Fast malloc replacement (recommended for Linux)

  In general, Linux is very slow for malloc and it is recommended
  to link with a faster malloc implementation. Some of the
  recommended mallocs are:

  - `jemalloc <http://www.canonware.com/jemalloc/>`_
  - `gperftools (tcmalloc) <http://code.google.com/p/gperftools/>`_

  They are configured by ``--with-{jemalloc,tcmalloc}`` and should be
  enabled using ``--enable-{jemalloc,tcmalloc}``

- `docutils <http://docutils.sourceforge.net>`_

  Manpages are written in reStructuredText format, and if you want
  manpages, you need to install docutils.
