=============
expgram_bound
=============

---------------------------------------------
estiamte upper bounds in ngram language model
---------------------------------------------

:Author: Taro Watanabe <taro.watanabe@nict.go.jp>
:Date:   2013-2-8
:Manual section: 1

SYNOPSIS
--------

**expgram_bound** [*options*]

DESCRIPTION
-----------



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

EXAMPLES
--------



SEE ALSO
--------
