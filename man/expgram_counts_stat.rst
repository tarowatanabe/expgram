===================
expgram_counts_stat
===================

---------------------------------------
dump statistics of indexed ngram counts
---------------------------------------

:Author: Taro Watanabe <taro.watanabe@nict.go.jp>
:Date:   2013-2-8
:Manual section: 1

SYNOPSIS
--------

**expgram_counts_stat** [*options*]

DESCRIPTION
-----------



OPTIONS
-------

  **--ngram** `arg (="")`      ngram counts in Google or expgram format

  **--output** `arg (="-")`    output statistics

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

EXAMPLES
--------



SEE ALSO
--------
