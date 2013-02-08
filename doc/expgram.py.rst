==========
expgram.py
==========

----------------------------------------------------
an expgram toolkit for learning ngram language model
----------------------------------------------------

:Author: Taro Watanabe <taro.watanabe@nict.go.jp>
:Date:   2013-2-8
:Manual section: 1

SYNOPSIS
--------

**expgram.py** [*options*]

DESCRIPTION
-----------



OPTIONS
-------

  --counts=COUNTS       counts in Google format
  --counts-list=COUNTS_LIST
                        list of ngram counts either in Google format or in a
                        plain format
  --corpus=CORPUS       corpus
  --corpus-list=CORPUS_LIST
                        list of corpus
  --order=ORDER         ngram order (default: 5)
  --output=OUTPUT       ngram output
  --cutoff=CUTOFF       count cutoff threshold (default: 1 == keep all the
                        counts)
  --kbest=KBEST         kbest vocabulary (default: 0 == keep all the counts)
  --vocab=VOCAB         vocabulary
  --tokenizer=TOKENIZER
                        tokenizer applied to data
  --remove-unk          remove unk from lm estimation
  --erase-temporary     erase temporary allocated disk space
  --first-step=STEP     first step (default: 1):
                        1 = vocabulary,
			2 = counts extraction,
			3 = counts index,
			4 = counts modification,
                        5 = estimation,
			6 = quantization
  --last-step=STEP      last step (default: 6):
                        1 = vocabulary,
			2 = counts extraction,
			3 = counts index,
			4 = counts modification,
                        5 = estimation,
			6 = quantization
  --expgram-dir=DIRECTORY
                        expgram directory
  --mpi-dir=DIRECTORY   MPI directory
  --temporary-dir=DIRECTORY
                        expgram directory
  --max-malloc=MALLOC   maximum memory in GB (default: 8)
  --mpi=MPI             # of processes for MPI-based parallel processing.
                        Identical to --np for mpirun
  --mpi-host=HOSTS      list of hosts to run job. Identical to --host for
                        mpirun
  --mpi-host-file=FILE  host list file to run job. Identical to --hostfile for
                        mpirun
  --threads=THREADS     # of thrads for thread-based parallel processing
  --pbs                 PBS for launching processes
  --pbs-queue=NAME      PBS queue for launching processes (default: ltg)
  --debug=DEBUG         debug level
  -h, --help            show this help message and exit

ENVIRONMENT
-----------

TMPDIR_SPEC
  temporary directory

EXAMPLES
--------



SEE ALSO
--------
