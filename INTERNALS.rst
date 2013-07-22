=========
Internals
=========

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

