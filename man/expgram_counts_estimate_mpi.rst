===========================
expgram_counts_estimate_mpi
===========================

-----------------------------------------------------------------
estimate ngram language model from collected counts (MPI version)
-----------------------------------------------------------------

:Author: Taro Watanabe <taro.watanabe@nict.go.jp>
:Date:   2013-2-8
:Manual section: 1

SYNOPSIS
--------

**expgram_counts_estimate_mpi** [*options*]

DESCRIPTION
-----------



OPTIONS
-------

  **--ngram** `arg (="")`      ngram counts in expgram format

  **--output** `arg (="")`     output in binary format

  **--temporary** `arg`       temporary directory

  **--prog** `arg`            this binary

  **--host** `arg`             host name

  **--hostfile** `arg`         hostfile name

  **--remove-unk** remove UNK when estimating language model

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
