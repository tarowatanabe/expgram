=============
expgram_bound
=============

---------------------------------------------
estiamte upper bounds in ngram language model
---------------------------------------------

:Author: Taro Watanabe <taro.watanabe@nict.go.jp>
:Date:   2013-7-29
:Manual section: 1

SYNOPSIS
--------

**expgram_bound** [*options*]

DESCRIPTION
-----------

**expgram_bound** computes the upper bound estimates which is used
during chart-based decoding for better rest-cost estimation. Note that
this is used when reading ngram language model in ARPA format which
was created by other applications, such as SRILM or KENLM. expgram
already supports better lower order estimtes when ngram language model
is estimated from a trining data.


OPTIONS
-------

  **--ngram** `arg (="-")`     ngram in ARPA or expgram format

  **--output** `arg (="")`     output in expgram format with ngram bound estimation

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

  expgram_bound \
    --ngram <ngram> \
    --output <ngram with upper bound estimates>


SEE ALSO
--------

`expgram_bound_mpi(1)`
