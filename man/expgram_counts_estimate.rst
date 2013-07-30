=======================
expgram_counts_estimate
=======================

---------------------------------------------------
estimate ngram language model from collected counts
---------------------------------------------------

:Author: Taro Watanabe <taro.watanabe@nict.go.jp>
:Date:   2013-7-29
:Manual section: 1

SYNOPSIS
--------

**expgram_counts_estimate** [*options*]

DESCRIPTION
-----------

Estimate ngram language modes using the ngram counts (**--ngram**) and
output Kneser-Ney smoothed ngram language model (**--output**). The
ngram counts can be either the counts in Google format or in an
expgram binary format. If the ngram counts is in Google format, it is
indexed, and modifed for preparing Kneser-Ney smoothing, and output
language model.
The estimated language model also contains parameters for lower-order
ngram language model which are used as estimates in chart-based
decoding.

OPTIONS
-------

  **--ngram** `arg (="")`      ngram counts in Google format or expgram format

  **--output** `arg (="")`     output in binary format

  **--temporary** `arg`        temporary directory

  **--remove-unk** remove UNK when estimating language model

  **--shard** `arg`            # of shards (or # of threads)

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

  expgram_counts_estimate \
    --ngram <indexed ngram counts or ngram counts in Google format> \
    --output <estimate ngram langauge model>


SEE ALSO
--------

`expgra_counts_modify(1)`, `expgra_counts_estimate_mpi(1)`
