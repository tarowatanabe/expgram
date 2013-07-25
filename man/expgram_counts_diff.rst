===================
expgram_counts_diff
===================

------------------------------------
check the difference of ngram counts
------------------------------------

:Author: Taro Watanabe <taro.watanabe@nict.go.jp>
:Date:   2013-3-14
:Manual section: 1

SYNOPSIS
--------

**expgram_counts_diff** [*options*] *ngram(s)*

DESCRIPTION
-----------



OPTIONS
-------

  **--output** `arg (="-")`    output result

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
