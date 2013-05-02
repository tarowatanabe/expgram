Building expgram
================

Souce Codes
-----------

Get the source code from `expgram <...>`_.

::

  git clone ...


Compile
-------

::

   ./autogen.sh
   ./configure --prefix <prefix where you want to install. detault = /usr/local>
   make
   make install (optional)

Dependencies
-----------

expgram is dependent on `boost library <http://boost.org>`_.
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
