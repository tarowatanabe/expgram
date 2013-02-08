=======================
expgram_counts_estimate
=======================

---------------------------------------------------
estimate ngram language model from collected counts
---------------------------------------------------

:Author: Taro Watanabe <taro.watanabe@nict.go.jp>
:Date:   2013-2-8
:Manual section: 1

SYNOPSIS
--------

**expgram_counts_estimate** [*options*]

DESCRIPTION
-----------



OPTIONS
-------

  --ngram arg           ngram counts in Google format or expgram format
  --output arg          output in binary format
  --temporary arg       temporary directory
  --remove-unk          remove UNK when estimating language model
  --shard arg           # of shards (or # of threads)
  --debug               debug level
  --help                help message


ENVIRONMENT
-----------

TMPDIR_SPEC
  temporary directory

EXAMPLES
--------



SEE ALSO
--------
