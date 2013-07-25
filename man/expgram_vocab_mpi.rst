=================
expgram_vocab_mpi
=================

--------------------------------------------
compute vocabulary from corpus (MPI version)
--------------------------------------------

:Author: Taro Watanabe <taro.watanabe@nict.go.jp>
:Date:   2013-7-25
:Manual section: 1

SYNOPSIS
--------

**expgram_vocab_mpi** [*options*]

DESCRIPTION
-----------

`expgram_vocab_mpi` enumerates all the words in corpura, and/or
ngram counts in either ARPA format or Google format. The output is a
list of words with frequency.


OPTIONS
-------

  **--corpus** `arg`           corpus file

  **--counts** `arg`           counts file

  **--corpus-list** `arg`      corpus list file

  **--counts-list** `arg`      counts list file

  **--output** `arg (="-")`    output directory

  **--filter** `arg`           filtering script

  **--prog** `arg`             this binary

  **--host** `arg`             host name

  **--hostfile** `arg`         hostfile name

  **--map-line**             map by lines, not by files

  **--debug** `[=arg(=1)]`     debug level

  **--help** help message


EXAMPLES
--------

::
   
   mpirun --np 8 expgram_vocab_mpi \
       --corpus      <corpus> \
       --corpus-list <list of corpus> \
       --counts      <counts> \
       --counts-list <list of counts> \
       --output <output>


SEE ALSO
--------

`expgram.py(1)`, `expgram_vocab(1)`
