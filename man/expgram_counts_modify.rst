=====================
expgram_counts_modify
=====================

-------------------------------------------------
modify ngram counts to prepare for KN discounting
-------------------------------------------------

:Author: Taro Watanabe <taro.watanabe@nict.go.jp>
:Date:   2013-7-29
:Manual section: 1

SYNOPSIS
--------

**expgram_counts_modify** [*options*]

DESCRIPTION
-----------

`expgram_counts_modify` computes the type counts in the ngram counts
in order to prepare for Kneser-Ney smoothing. If the ngram file
specified via **--ngram** is an
ngram counts in expgram format, we simply perform counts
modification. If the ngram counts is in Google format, we will, first,
perfomr ngram counts indexing, then, modify counts.

OPTIONS
-------

  **--ngram** `arg (="")`      ngram counts in Google or expgram format

  **--output** `arg (="")`     output in binary format

  **--temporary** `arg`        temporary directory

  **--shard** `arg (=4)`       # of shards (or # of threads)

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
   
  expgram_counts_modify \
    --ngram <indexed ngram counts> \
    --output <indexed ngram with modified counts>

SEE ALSO
--------

`expgram_counts_index(1)`, `expgram_counts_modify_mpi(1)`
