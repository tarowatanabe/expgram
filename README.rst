=========================================
expgram: EXPonential-order n-GRAM toolkit
=========================================

This is an ngram package with efficient handling of large data in mind, based on a succinct storage [1]_.
The target is to index Google's ngrams into 10GB.
Among ngram compression mentioned in [1]_, we do not implement block-wise compression (or gzip every 8k-byte)
for efficiency reason. 
Also, this toolkit implements large scale ngram language model proposed by Goolge [2]_.
In order to support better rest cost estimation, particulary used in SMT systems, this toolkit also implements better
upper bound estimates by correctly estimating lower-order ngram language models [3]_.
We also supports transducer-like interface motivated by [4]_ for a faster lm score computation.

Quick Start
-----------

Compile
```````

Get the source code from `expgram <...>`_.

::

   ./autogen.sh
   ./configure --prefix <prefix where you want to install. detault = /usr/local>
   make
   make install (optional)

Dependencies
````````````

expgram is dependent on `boost library <http://boost.org>`_.
Optionally, following libraries are recommended:

  - MPI impllementation
    We strongly recommends `open mpi <http://www.open-mpi.org>`_
    which is regularly tested by the author.
    
  - Fast malloc replacement (recommended for Linux)
    In general, Linux is very slow for malloc and it is recommended
    to link with a faster malloc implementation. Some of the
    recommended mallocs are:

     - `jemalloc <http://www.canonware.com/jemalloc/>`_
     - `gperftools (tcmalloc) <http://code.google.com/p/gperftools/>`_

Run
```

Basically, you have only to use expgram.py which encapsulate all the routimes to estimate LM.
For instance, you can run:

::

  ./expgram.py
	   --expgram-dir <installed expgram>
       	   --corpus <corpus> or --corpus-list <list of corpus> or --counts-list <list of counts>
	   --output <prefix of lm name>
	   --order  <order of ngram lm>
	   --temporary-dir <temporary disk space>

This will dump 4 models:

::

     <prefix>.counts		indexed counts
     <prefix>.modified		indexed counts with modified counts for modified-KN smoothing
     <prefix>.lm		estiamted LM
     <prefix>.lm.quantize	8-bit quantized LM

or, if you already have count data organized into Google format, simply run

::

  ./expgram.py
	   --expgram-dir <installed expgram>
	   --counts <counts in Google format>
	   --output <prefix of lm name>
	   --order  <order of ngram lm>
	   --temporary-dir <temporary disk space>

This will dump 3 models:

::

     <prefix>.modified		indexed counts with modified counts for modified-KN smoothing
     <prefix>.lm		estiamted LM
     <prefix>.lm.quantize	8-bit quantized LM

Internals
---------

Brief descriptions of binaries:

      expgram_vocab
        Compute vocabulary (unigram with count). From this file, you can create count thresholded word-list, for instance, by
	 
        cat [output of expgram_vocab] | gawk '{ if ($12 >= 20) {print $1;}}' > vocab.list

      expgram_counts_extract
	compute ngram counts given  sentence data or ngram counts collection. The output is almost compatible with
	Google ngram data specification, but differ in that counts are not reduced.
	If you want to restrict vocabulary, supply word-list via --vocab option.

      expgram_counts_index
	ngram counts indexer from Googles ngram data format.
	--shard control # of threads, which implies # of shards (or # of data splitting)

      expgram_counts_modify
	perform counts modification in ngram data for KN smoothing. If not indexed, perform indexing.

      expgram_counts_estimate
	perform ngram probabilities estimation. If not indexed, perfomr indexing. If not modified, perform counts modification.

      expgram_index
	ngram indexer from arpa format. For instance, you can use srilm to estimate your LM, and index it as an expgram format.

      expgram_bound
	ngram upper bound estimator. Compute upper bounds for ngrams. You do not have to run this, since ngram_index will
	compute upper bounds simultaneously. It remains here for compatibility with mpi version (see below).
	This is not required any more if you estimate your LM using this toolkit.

      expgram_quantize
	perform quantization for ngram probabilities, backoffs and upper bounds. If not indexed, perform indexing.

      expgram_stat/expgram_counts_stat
	dump statistics on storage size

      expgram_clean
	If you found spurious data on your temporary directory, run this to clean up the data. 
	REMARK: make sure you are not runnning other expgram tools!

     The set of tools provided here extensively use temporary disk space specified either by the environgment
     variable, TMPDIR or TMPDIR_SPEC, or program option, --temporary-dir.
     TMPDIR is usually set by default, and usually specified /tmp or /var/tmp
     Alternatively, you can specify via TMPDIR_SPEC: 
     
     export TMPDIR_SPEC=/export/%host/users/${USER}
     
     The %host key word is simply replaced by the host of running machine (so that each binary may abuse only local-storage).
     (This %host replace format can be specified via --temporary-dir option.)

For larger data, it is recommended to use mpi-version for scalability.

::

      expgram_vocab_mpi
      expgram_counts_extract_mpi
      expgram_counts_index_mpi
      expgram_counts_modify_mpi
      expgram_counts_estimate_mpi
      expgram_bound_mpi
      expgram_quantize_mpi

They performed similar to threaded version, but differ in that you have to explicitly run from index though quantize in order.

APIs
----

API: Sample codes exists at sample directory, ngram.cc and ngram_counts.cc

	NGram:
		:code:`operator()(first, last)`: return backoff log-probabilities for ngram. Iterator must supports random-access
				  	  concepts, such as vector's iterator.

		:code:`logprob(first, last)` : synonym to operator()(first, last)
		
		:code:`logbound(first, last)` : Return upper-bound log-probability for ngram. Specifically,
				        :math:`P_{bound}(w_n | w_i ... w_{n-1}) = max_{w_{i-1}} P(w_n | w_{i-1}, ... w_{n-1})`.
				      	It is useful for a task, such as decoding, when we want to pre-compute heuristic
					score in advance.
		
		:code:`exists(first, last)` : check whether a particular ngram exists or not.

		:code:`index.order()`: returns ngram's maximum order
		
	NGramCounts:
		:code:`operator()(first, last)` : return ngram count. if an ngram [first, last) does not exist, return zero.
		
		:code:`count(first, last)` : synonym to operator()(first, last)
		:code:`modified(first, last)` : returnn "modified" ngram count for KN smoothing (when estimated...)
		
		:code:`exists(first, last)` : check whether a particular ngram exists or not.
		
		:code:`index.order()`: returns ngram's maximum order
	
	Internally, words (assuming std::string) are autoamtically converted into word_type (expgram::Word), then word_id
	(expgram::Word::id_type). If you want to avoid such conversion on-the-fly, you can pre convert them by
		
	.. code:: c++

	   expgram::Word::id_type word_id = {ngram or ngram_counts}.index.vocab()[word];

	or, if you don't want to waste extra memory for expgram::Word type, use: (assuming "word" is std::string)

	.. code:: c++
	  
	  expgram::Word::id_type word_id = {ngram or ngram_counts}.index.vocab()[expgram::Vocab::UNK];
	  
	  if ({ngram or ngram_counts}.index.vocab().exists(word))
	    word_id = {ngram or ngram_counts}.index.vocab()[word];
	
	All the operations are thread-safe, meaning that concurrent
	programs may call any API any time without locking! (except for ngram loading...)

References
----------

.. [1]
.. code:: latex

 @InProceedings{watanabe-tsukada-isozaki:2009:Short,
   author    = {Watanabe, Taro  and  Tsukada, Hajime  and  Isozaki, Hideki},
   title     = {A Succinct N-gram Language Model},
   booktitle = {Proceedings of the ACL-IJCNLP 2009 Conference Short Papers},
   month     = {August},
   year      = {2009},
   address   = {Suntec, Singapore},
   publisher = {Association for Computational Linguistics},
   pages     = {341--344},
   url       = {http://www.aclweb.org/anthology/P/P09/P09-2086}
 }

.. [2]
.. code:: latex

 @InProceedings{brants-EtAl:2007:EMNLP-CoNLL2007,
   author    = {Brants, Thorsten  and  Popat, Ashok C.  and  Xu, Peng  and  Och, Franz J.  and  Dean, Jeffrey},
   title     = {Large Language Models in Machine Translation},
   booktitle = {Proceedings of the 2007 Joint Conference on Empirical Methods in Natural Language Processing and Computational Natural Language Learning (EMNLP-CoNLL)},
   month     = {June},
   year      = {2007},
   address   = {Prague, Czech Republic},
   publisher = {Association for Computational Linguistics},
   pages     = {858--867},
   url       = {http://www.aclweb.org/anthology/D/D07/D07-1090}
 }

.. [3]
.. code:: latex

 @InProceedings{heafield-koehn-lavie:2012:EMNLP-CoNLL,
   author    = {Heafield, Kenneth  and  Koehn, Philipp  and  Lavie, Alon},
   title     = {Language Model Rest Costs and Space-Efficient Storage},
   booktitle = {Proceedings of the 2012 Joint Conference on Empirical Methods in Natural Language Processing and Computational Natural Language Learning},
   month     = {July},
   year      = {2012},
   address   = {Jeju Island, Korea},
   publisher = {Association for Computational Linguistics},
   pages     = {1169--1178},
   url       = {http://www.aclweb.org/anthology/D12-1107}
 }

.. [4]
.. code:: latex

 @inproceedings{37218,
   title = {Unary Data Structures for Language Models},
   author  = {Jeffrey Sorensen and Cyril Allauzen},
   year  = 2011,
   booktitle = {Interspeech 2011},
   pages = {1425-1428}
 }
