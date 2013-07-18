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

TMPDIR_SPEC
  temporary directory


EXAMPLES
--------



SEE ALSO
--------


