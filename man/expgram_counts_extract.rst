======================
expgram_counts_extract
======================

--------------------------------
extract ngram counts from corpus
--------------------------------

:Author: Taro Watanabe <taro.watanabe@nict.go.jp>
:Date:   2013-7-25
:Manual section: 1

SYNOPSIS
--------

**expgram_counts_extract** [*options*]

DESCRIPTION
-----------

`expgram_counts_extract` extracts ngram counts in corpura, and/or
merge ngram counts in either ARPA format or Google format.
The extracted or merged counts are write in a Google format.

OPTIONS
-------

  **--corpus** `arg`           corpus file

  **--counts** `arg`           counts file

  **--corpus-list** `arg`      corpus list file

  **--counts-list** `arg`      counts list file

  **--vocab** `arg`            vocabulary file (list of words)

  **--output** `arg`           output directory

  **--filter** `arg`           filtering script

  **--order** `arg (=5)`       ngram order

  **--map-line** map by lines, not by files

  **--threads** `arg`          # of threads

  **--max-malloc** `arg`       maximum malloc in GB

  **--debug** `[=arg(=1)]`     debug level

  **--help** help message


EXAMPLES
--------

::
   
   expgram_counts_extract \
       --corpus      <corpus> \
       --corpus-list <list of corpus> \
       --counts      <counts> \
       --counts-list <list of counts> \
       --output <output>

SEE ALSO
--------

`expgram.py(1)`, `expgram_counts_extract_mpi(1)`
