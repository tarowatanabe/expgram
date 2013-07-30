=========================
expgram_counts_modify_mpi
=========================

---------------------------------------------------------------
modify ngram counts to prepare for KN discounting (MPI version)
---------------------------------------------------------------

:Author: Taro Watanabe <taro.watanabe@nict.go.jp>
:Date:   2013-7-29
:Manual section: 1

SYNOPSIS
--------

**expgram_counts_modify_mpi** [*options*]

DESCRIPTION
-----------

`expgram_counts_modify_mpi` computes the type counts in the ngram counts
in order to prepare for Kneser-Ney smoothing. The ngram counts
specified by **--ngram** must be the ngram counts indexed either by
`expgram_counts_index(1)` or `expgram_counts_index_mpi(1)`.

OPTIONS
-------

  **--ngram** `arg (="")`      ngram counts in expgram format

  **--output** `arg (="")`     output in binary format

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

  The temporary directory specified either by **TMPDIR_SPEC** or by
  **--temporary** has a special treatment in that the keyword
  %host is replaced by the host of running machine. For instance, you
  can set:

    /temporary/%host/tmp

  and your running machine is run005, then, the temporary directory
  will be /temporary/run005/tmp.

EXAMPLES
--------

::
   
  mpirun --np 8 expgram_counts_modify_mpi \
    --ngram <indexed ngram counts> \
    --output <indexed ngram with modified counts>

SEE ALSO
--------

`expgram_counts_index(1)`, `expgram_counts_index_mpi(1)`, `expgram_counts_modify(1)`
