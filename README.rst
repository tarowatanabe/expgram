=========================================
expgram: EXPonential-order n-GRAM toolkit
=========================================

This is an ngram package with efficient handling of large data in
mind, based on a succinct storage [1]_.
The target is to index Google's ngrams into 10GB.
Among ngram compression mentioned in [1]_, we do not implement
block-wise compression (or gzip every 8k-byte) for efficiency reason. 
Also, this toolkit implements large scale ngram language model
estimation proposed by Goolge [2]_.
In order to support better rest cost estimation, particulary used in
SMT systems, this toolkit also implements better bound scores
by correctly estimating lower-order ngram language models [3]_.
We also supports transducer-like interface motivated by [4]_ for a
faster lm score computation.
Note this toolkit is primarily developed to handle large ngram count
data, thus it is not called like xxxlm.

Quick Start
-----------

Compile
```````
Get the source code from `expgram <...>`_ by git and simply follow GNU
the standard pipiline. For details, see BUILD.rst.

::

   ./autogen.sh
   ./configure --prefix <prefix where you want to install. detault = /usr/local>
   make
   make install (optional)

Run
```

Basically, you have only to use expgram.py (found at
`<builddir>/scripts` or `<install prefix>/bin`) which encapsulate all
the routimes to estimate LM. For instance, you can run:

::

  ./expgram.py
	   --expgram-dir <installed expgram>
       	   --corpus <corpus> or --corpus-list <list of corpus> or --counts-list <list of counts>
	   --output <prefix of lm name>
	   --order  <order of ngram lm>
	   --temporary-dir <temporary disk space>

Here, we assume a corpus, newline delimited set of sentences,
indicated by `--corpus <corpus>` or a list of corpus, newline
delimited set of corpora specified by `--corpus-list <list of corpus>`
or `--counts-list <list of counts>` a list of count data.
This will dump 4 data:

::

     <prefix>.counts		indexed counts
     <prefix>.modified		indexed counts with modified counts for modified-KN smoothing
     <prefix>.lm		estiamted LM
     <prefix>.lm.quantize	8-bit quantized LM

or, if you already have count data organized into a Google format, simply run

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
