==========
expgram.py
==========

----------------------------------------------------
an expgram toolkit for learning ngram language model
----------------------------------------------------

:Author: Taro Watanabe <taro.watanabe@nict.go.jp>
:Date:   2013-7-25
:Manual section: 1

SYNOPSIS
--------

**expgram.py** [*options*]

DESCRIPTION
-----------

A python wrapper script to use expgram toolkit. 
You can learn ngram language model by:

1. Compute a vocabulary file (Optional).

  - You need to specify either counts in Google format (**--counts**)
    and/or corpus consisting of sentences (**--corpus**). 
    Alternatively, you can specify a list of counts
    (**--counts-list**) and/or list of
    corpus (**--corpus-list**).
  - You can threshold by the number of words (**--kbest**), or by the
    frequency (**--cutoff**).
  - This will output the files, *<output>.vocab* and
    *<output>.vocab.<cutoff>* or *<output>.vocab.<kbest>*.
  - For details, see `expgram_vocab(1)` or `expgram_vocab_mpi(1)`.

2. Extract ngram counts.

  - You need to specify either counts in Google format (**--counts**)
    and/or corpus consisting of sentences (**--corpus**). 
    Alternatively, you can specify a list of counts
    (**--counts-list**) and/or list of
    corpus (**--corpus-list**).
  - Extracted ngram counts are output as *<output>.counts* in Google
    format.
  - If an option **--vocab** is specified with the file pointing to
    a list of words, then, extraction is restricted to those found in
    the list. OOVs are mapped to *<unk>*.
    Or, if **--cutoff** or **--kbest** is specified, then, the
    computed vocabulary is used.
  - For details, see `expgram_counts_extract(1)` or `expgram_counts_extract_mpi(1)`.

3. Index ngram counts, and output as *<output>.index*.

  - For details, see `expgram_counts_index(1)` or `expgram_counts_index_mpi(1)`.

4. Compute suffix counts to prepare for modified Kneser-Ney smoothing.

  - This will result in an indexed counts as *<output>.modified*.
  - For details, see `expgram_counts_modify(1)` or `expgram_counts_modify_mpi(1)`.

5. Estimate ngram language model and output as *<output>.estimated*.

  - For details, see `expgram_counts_estimate(1)` or `expgram_counts_estimate_mpi(1)`.

6. Transform into backward trie structure for efficient query, and
   output as *<output>.lm*

  - For details, see `expgram_counts_backward(1)` or `expgram_counts_backward_mpi(1)`.

7. Quantize estimated ngram language model and output as *<output>.lm.quantize*.

  -  We perofmr 8-bit quantization.
  - For details, see `expgram_counts_quantize(1)` or `expgram_counts_quantize_mpi(1)`.

You can perform the whole pipeline in parallel either by specifying
the number of threads (via **--threads** option), or by specifying the
number of MPI nodes (via **--mpi** option with either **--mpi-host**
or **--mpi-host-file** to specify the list of hosts).
If PBS is set up in your environment, you can run on pbs nodes (via
**--pbs** option with **--pbs-queue** to specify the batch queue).
During indexing and or estimation, we use temporary disk space,
specified either by **--temporary-dir** or the environment varialbles,
**TMPDIR_SPEC** and/or **TMPDIR**.

OPTIONS
-------

Usage: expgram.py [options]

Options:
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
  --first-step=STEP     first step (default: 1): 1 = vocabulary, 2 = counts
                        extraction, 3 = counts index, 4 = counts modification,
                        5 = estimated language model, 6 = backward trie, 7 =
                        quantization
  --last-step=STEP      last step (default: 7): 1 = vocabulary, 2 = counts
                        extraction, 3 = counts index, 4 = counts modification,
                        5 = estimated language model, 6 = backward trie, 7 =
                        quantization
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

TMPDIR
  Temporary directory.

TMPDIR_SPEC
  An alternative temporary directory. If **TMPDIR_SPEC** is specified,
  this is preferred over **TMPDIR**. In addition, if
  **--temporary-dir** is specified, program option is preferred over
  environment variables.

EXAMPLES
--------
::

  expgram.py \
       	   --corpus <corpus> or --corpus-list <list of corpus> \
	   --output <output> \
	   --order  <order of ngram lm> \
	   --temporary-dir <temporary disk space>

This will dump 6 data:

::

     <output>.counts		extracted ngram counts
     <output>.index		indexed ngram counts
     <output>.modified		indexed modified counts for modified-KN smoothing
     <output>.estimated		temporarily estiamted LM
     <output>.lm		LM with more efficient indexing
     <output>.lm.quantize	8-bit quantized 

SEE ALSO
--------

`expgram_vocab(1)`, `expgram_vocab_mpi(1)`,
`expgram_counts_extract(1)`, `expgram_counts_extract_mpi(1)`,
`expgram_counts_index(1)`, `expgram_counts_index_mpi(1)`,
`expgram_counts_modify(1)`, `expgram_counts_modify_mpi(1)`,
`expgram_counts_estimate(1)`, `expgram_counts_estimate_mpi(1)`,
`expgram_backward(1)`, `expgram_backward_mpi(1)`,
`expgram_quantize(1)`, `expgram_quantize_mpi(1)`
