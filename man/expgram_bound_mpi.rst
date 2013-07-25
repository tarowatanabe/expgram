=================
expgram_bound_mpi
=================

-----------------------------------------------------------
estiamte upper bounds in ngram language model (MPI version)
-----------------------------------------------------------

:Author: Taro Watanabe <taro.watanabe@nict.go.jp>
:Date:   2013-2-8
:Manual section: 1

SYNOPSIS
--------

**expgram_bound_mpi** [*options*]

DESCRIPTION
-----------



OPTIONS
-------

  **--ngram** `arg (="")`      ngram language model in expgram format

  **--output** `arg (="")`     output in expgram format with upper bound estimation

  **--temporary** `arg`       temporary directory

  **--prog** `arg`            this binary

  **--host** `arg`             host name

  **--hostfile** `arg`         hostfile name

  **--debug** `[=arg(=1)]`     debug level

  **--help** help message

ENVIRONMENT
-----------

TMPDIR
  Temporary directory.

TMPDIR_SPEC
  An alternative temporary directory. If **TMPDIR_SPEC** is specified,
  this is preferred over **TMPDIR**. In addition, if
  **--temporary** is specified, program option is preferred over
  environment variables.

EXAMPLES
--------



SEE ALSO
--------
