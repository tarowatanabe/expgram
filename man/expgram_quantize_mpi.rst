====================
expgram_quantize_mpi
====================

-------------------------------------------
quantize ngram language model (MPI version)
-------------------------------------------

:Author: Taro Watanabe <taro.watanabe@nict.go.jp>
:Date:   2013-7-29
:Manual section: 1

SYNOPSIS
--------

**expgram_quantize_mpi** [*options*]

DESCRIPTION
-----------

Perform 8-bit quantization.

OPTIONS
-------

  **--ngram** `arg (="")`      ngram language model in expgram format

  **--output** `arg (="")`     output in expgram format

  **--temporary** `arg`        temporary directory

  **--prog** `arg`             this binary

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

  The temporary directory specified either by **TMPDIR_SPEC** or by
  **--temporary** has a special treatment in that the keyword
  %host is replaced by the host of running machine. For instance, you
  can set:

    /temporary/%host/tmp

  and your running machine is run005, then, the temporary directory
  will be /temporary/run005/tmp.

EXAMPLES
--------



SEE ALSO
--------

`expgram_counts_estimate(1)`, `expgram_counts_estimate_mpi(1)`,
