==================
expgram_perplexity
==================

-------------------------------
compute perplexity for a corpus
-------------------------------

:Author: Taro Watanabe <taro.watanabe@nict.go.jp>
:Date:   2013-7-29
:Manual section: 1

SYNOPSIS
--------

**expgram_perplexity** [*options*]

DESCRIPTION
-----------

Compute the perplexicy of a corpus (specified via **--input**) using
the ngram lanaugage model (**--ngram**).

OPTIONS
-------

  **--ngram** `arg (="")`      ngram in ARPA or expgram format

  **--input** `arg (="-")`     input

  **--output** `arg (="-")`    output

  **--temporary** `arg`        temporary directory

  **--shard** `arg (=4)`       # of shards (or # of threads)

  **--populate** perform memory population

  **--precompute** assume precomputed word-id input

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
   expgram_perplexity \
     --ngram <ngram language model> \
     --input <corpus to compute perplexity>


SEE ALSO
--------

`expgram(1)`
