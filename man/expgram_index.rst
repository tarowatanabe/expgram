=============
expgram_index
=============

-------------------------------------
index ARPA language model for expgram
-------------------------------------

:Author: Taro Watanabe <taro.watanabe@nict.go.jp>
:Date:   2013-7-29
:Manual section: 1

SYNOPSIS
--------

**expgram_index** [*options*]

DESCRIPTION
-----------

`expgram_index` read an ngram language model in ARPA
format, and output the model in expgram's binary format for efficient
query.

OPTIONS
-------

  **--ngram** `arg (="-")`     ngram in ARPA or expgram format

  **--output** `arg (="")`     output in binary format

  **--temporary** `arg`        temporary directory

  **--quantize** perform quantization

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
   expgram_index \
     --ngram <ngram language model in ARPA or in expgram> \
     --input <ngram language model in expgram format>

SEE ALSO
--------

`expgram(1)`, `expgram_perplexity(1)`
