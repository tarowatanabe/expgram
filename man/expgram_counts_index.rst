====================
expgram_counts_index
====================

---------------------------------------
index ngram counts into a binary format
---------------------------------------

:Author: Taro Watanabe <taro.watanabe@nict.go.jp>
:Date:   2013-7-29
:Manual section: 1

SYNOPSIS
--------

**expgram_counts_index** [*options*]

DESCRIPTION
-----------

`expgram_counts_index` perform indexing of ngram counts in Google
format (**--ngram**) and output an indexed ngram counts (**--output**).
The shard option(**--shard**) specifies the number of shard for
parallel indexing.

OPTIONS
-------

  **--ngram** `arg (="")`      ngram counts in Google format

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
   expgram_counts_index \
     --ngram <ngram counts in Google format> \
     --output <index ngram counts>   

SEE ALSO
--------

`expgram_counts_index_mpi(1)`
