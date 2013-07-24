=========================================
expgram: EXPonential-order n-GRAM toolkit
=========================================

This is an ngram toolkit with efficient handling of large data in
mind, based on a succinct storage [1]_. Among ngram compression
methods mentioned in [1]_, we do not implement block-wise compression
(or gzip every 8k-byte) for computational efficiency reason.  Also,
this toolkit implements a MapReduce based large scale ngram language
model estimation method proposed by Goolge [2]_.
In order to support better rest cost estimation, particulary used in
SMT systems, this toolkit also implements better bound scores
by correctly estimating lower-order ngram language models [3]_.
We also supports transducer-like interface motivated by [4]_ and an
efficient prefix/suffix ngram context computation [3]_.
Note this toolkit is primarily developed to handle large ngram count
data, thus it is not called like xxxlm.

The expgram toolkit is developed at Multilingual Translation
Laboratory, Universal Communication Institute, National Institute of
Information and Communications Technology (NICT).

Quick Start
-----------

Compile
```````
Get the source code from `expgram <http://www2.nict.go.jp/univ-com/multi_trans/expgram>`_,
or from `github.com <http://github.com/tarowatanabe/expgram>`_, and
simply follow the GNU standard pipiline. For details, see `BUILD.rst`.

.. code:: bash

   ./autogen.sh (required when you get the code by git clone)
   ./configure
   make
   make install (optional)

Run
```

Basically, you have only to use `expgram.py` (found at
`<build dir>/scripts` or `<install prefix>/bin`) which encapsulate all
the routimes to estimate LM. For instance, you can run:

.. code:: bash

  expgram.py \
	   --expgram-dir <installed expgram> \
       	   --corpus <corpus>
	      or --corpus-list <list of corpus>
	      or --counts-list <list of counts> \
	   --output <prefix of lm name> \
	   --order  <order of ngram lm> \
	   --temporary-dir <temporary disk space>

Here, we assume a corpus, newline delimited set of sentences,
indicated by `--corpus <corpus>` or a list of corpus, newline
delimited set of corpora specified by `--corpus-list <list of corpus>`
or `--counts-list <list of counts>` a list of count data.
This will dump 6 data:

::

     <prefix>.counts		extracted ngram counts
     <prefix>.index		indexed ngram counts
     <prefix>.modified		indexed modified counts for modified-KN smoothing
     <prefix>.estimated		temporarily estiamted LM
     <prefix>.lm		LM with more efficient indexing
     <prefix>.lm.quantize	8-bit quantized LM

or, if you already have count data organized into a Google format, simply run

.. code:: bash

  expgram.py \
	   --expgram-dir <installed expgram> \
	   --counts <counts in Google format> \
	   --output <prefix of lm name> \
	   --order  <order of ngram lm> \
	   --temporary-dir <temporary disk space>

This will dump 5 models:

::

     <prefix>.index		indexed ngram counts
     <prefix>.modified		indexed modified counts for modified-KN smoothing
     <prefix>.estimated		temporarily estiamted LM
     <prefix>.lm		LM with more efficient indexing
     <prefix>.lm.quantize	8-bit quantized LM

The language model probabilities are stored by the natural logarithm
(with e as a base), not by the logarithm with base 10. If you want to
see the LM, use (found at `<build dir>/progs` or `<install prefix>/bin`):

.. code:: bash

   expgram_dump --ngram <prefix>.lm (or <prefix>.lm.quantize)

which write LM in ARPA format, using the common logarithm with base 10.

References
----------

.. [1]	 Taro Watanabe, Hajime Tsukada, and Hideki Isozaki. A succinct
	 n-gram language model. In Proceedings of the ACL-IJCNLP 2009
	 Conference Short Papers, pages 341-344, Suntec, Singapore,
	 August 2009. Association for Computational Linguistics.

.. [2]	 Thorsten Brants, Ashok C. Popat, Peng Xu, Franz J. Och, and
	 Jeffrey Dean. Large language models in machine
	 translation. In Proceedings of the 2007 Joint Conference on
	 Empirical Methods in Natural Language Processing and
	 Computational Natural Language Learning (EMNLP-CoNLL), pages
	 858-867, Prague, Czech Republic, June 2007. Association for
	 Computational Linguistics.

.. [3]	 Kenneth Heafield, Philipp Koehn, and Alon Lavie. Language
	 model rest costs and space-efficient storage. In Proceedings
	 of the 2012 Joint Conference on Empirical Methods in Natural
	 Language Processing and Computational Natural Language
	 Learning, pages 1169-1178, Jeju Island, Korea,
	 July 2012. Association for Computational Linguistics.

.. [4]	 Jeffrey Sorensen and Cyril Allauzen. Unary data structures
	 for language models. In Interspeech 2011, pages
	 1425-1428, 2011.

