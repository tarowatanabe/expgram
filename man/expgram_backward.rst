================
expgram_backward
================

-------------------------------------------------
transform ngram language model in backward format
-------------------------------------------------

:Author: Taro Watanabe <taro.watanabe@nict.go.jp>
:Date:   2013-6-30
:Manual section: 1

SYNOPSIS
--------

**expgram_backward** [*options*]

DESCRIPTION
-----------



OPTIONS
-------

  **--ngram** `arg (="-")`     ngram in ARPA or expgram format

  **--output** `arg (="")`     output in expgram format with an efficient backward 
                        trie structure

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


