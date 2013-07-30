============
expgram_diff
============

--------------------------------------------
check the difference of ngram language model
--------------------------------------------

:Author: Taro Watanabe <taro.watanabe@nict.go.jp>
:Date:   2013-7-30
:Manual section: 1

SYNOPSIS
--------

**expgram_diff** [*options*] *ngram(s)*

DESCRIPTION
-----------

Read two or more ngram language modep and check if they are identical
or not.

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

  expgram_diff <ngram LM 1> <ngrma LM 2>

SEE ALSO
--------

`expgram_index(1)`, `expgram_counts_diff(1)`
