===========================
expgram_counts_estimate_mpi
===========================

-----------------------------------------------------------------
estimate ngram language model from collected counts (MPI version)
-----------------------------------------------------------------

:Author: Taro Watanabe <taro.watanabe@nict.go.jp>
:Date:   2013-7-29
:Manual section: 1

SYNOPSIS
--------

**expgram_counts_estimate_mpi** [*options*]

DESCRIPTION
-----------

Estimate ngram language modes using the ngram counts (**--ngram**) and
output Kneser-Ney smoothed ngram language model (**--output**). 
The estimated language model also contains parameters for lower-order
ngram language model which are used as estimates in chart-based
decoding.
Note that the ngram language model output by
`expgram_counts_estimate_mpi` is temporary in that the ngrams are
sorted in forward order, which is problematic for efficient ngram
query. Thus, you will need to run either `expgram_backward(1)` or
`expgram_backward_mpi(1)` to transform into backward sorted order.

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
   
  mpirun --np 8 expgram_counts_estimate_mpi \
    --ngram <indexed ngram counts> \
    --output <estimate ngram langauge model>

SEE ALSO
--------

`expgra_counts_modify_mpi(1)`, `expgram_counts_estimate(1)`,
`expgram_backward(1)`, `expgram_backward_mpi(1)`
