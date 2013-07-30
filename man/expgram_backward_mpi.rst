====================
expgram_backward_mpi
====================

-------------------------------------------------
transform ngram language model in backward format
-------------------------------------------------

:Author: Taro Watanabe <taro.watanabe@nict.go.jp>
:Date:   2013-7-29
:Manual section: 1

SYNOPSIS
--------

**expgram_backward_mpi** [*options*]

DESCRIPTION
-----------

The ngram langauge model estimated by `expgram_counts_estimate_mpi(1)`
(not by `expgram_counts_estimate(1)`) is temporary in that the ngram
counts are sorted in forward order, which is inefficient for ngram
query.
`expgram_backward_mpi` re-sort in backward order, like kenlm, for
efficient query.

OPTIONS
-------

  **--ngram** `arg (="")`      ngram language model in expgram format

  **--output** `arg (="")`     output in expgram format with an efficient backward 
                        trie structure

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

::
   
  mpirun --np 8 expgram_backward_mpi \
    --ngram <ngram language omdel> \
    --output <backward order ngram language model>


SEE ALSO
--------

`expgram_counts_estimate_mpi(1)`, `expgram_backward.rst`
