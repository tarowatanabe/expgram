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
------------

expgram is dependent on `boost library <http://boost.org>`_.
Optionally, following libraries are recommended:

  - MPI impllementation
    We strongly recommends `open mpi <http://www.open-mpi.org>`_
    which is regularly tested by the author.
    The MPI libraries are automatically detected by the `configure`
    script by finding either mpic++, mpicxx or mpiCC. Thus, those mpi
    specific compilers should be on the path.

    If you encounter a problem like, "mca_btl_tcp_frag_recv: readv
    failed: Connection timed out" then, try this patch:

*** ompi/mca/btl/tcp/btl_tcp_frag.c.org	2012-04-03 23:30:11.000000000 +0900
--- ompi/mca/btl/tcp/btl_tcp_frag.c	2013-05-02 10:43:21.571867286 +0900
***************
*** 201,206 ****
--- 201,207 ----
  	switch(opal_socket_errno) {
  	case EINTR:
  	    continue;
+ 	case ETIMEDOUT:
  	case EWOULDBLOCK:
  	    return false;
  	case EFAULT:

    This will force open-mpi to re-reading buffer again, even after
    timeout.

  - Fast malloc replacement (recommended for Linux)
    In general, Linux is very slow for malloc and it is recommended
    to link with a faster malloc implementation. Some of the
    recommended mallocs are:

     - `jemalloc <http://www.canonware.com/jemalloc/>`_
     - `gperftools (tcmalloc) <http://code.google.com/p/gperftools/>`_

    They are configured by --with-{jemalloc,tcmalloc} and should be
    enabled using --enable-{jemalloc,tcmalloc}
