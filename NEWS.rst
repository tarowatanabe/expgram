2013-10-26
        * Bugfix for MPI libraries linking (thanks to the problem
	  reported by Hiroshi Umemoto)

2013-10-25
        * Update to compile for OS X 10.9 (with libc++ and libstdc++).

2013-10-2
        * Minor bug fix for two dimensional arrays, which do not
	  affect anything.

2013-8-2
        * INCOMPATIBLE: We employ backward sorted ngram language
	  model, which is basically identical to KenLM, but with more
	  succinct storage.
        * INCOMPATIBLE: The state reprensentation is exactly the same
	  as KenLM, but differ in that we do not have suffix checking
	  via backoff parameter.
	* INCOMPATIBLE: Use of murmur3 for hash functions for strings/sentence etc., but we preserve the old
	  murmur hash function for indexed, binary files.
	* Support bzip2 compressed ngram counts (which was found in LDC's web data in European languages)

2013-1-11
	* Faster MPI-based ngram LM estimation by tweaking buffer size
	* Added --first-step and --last-step in exmpgra.py to start from intermediate steps

2013-1-10
	* Potential bugfix for deadlock in MPI-based ngram LM estimation

2013-1-9
	* Added --temporary-dir option for exgram.py to specify temporary directory
	* Added --temporary option to specify temporary directory

2012-12-31
	* Added --prefix when running via mpirun for locating binaries/libraries
	* Bugfix for pbs: set up threads parameter for MPI-based applications

2012-11-27
	* Removed the --expgram-dir requirement for expgram.py by checking the direcotries of installed binaries
	* Added options for expgram.py
	     --kbest for limit the vocabulary to kbest
	     --vocab for limit the vocabulary specified in a file

2012-9-6
	* INCOMPATIBLE: ngram-counts now separately store modified counts (and data structure has changed!)
	* INCOMPATIBLE: Estimate lower-order ngram model at the same time.
	* INCOMPATIBLE: renamed binaries and scripts from ngram* to expgram*
	* Removed unused data structure
	* Support the latest sparsehash and gperftools (actually, they are renamed)

2012-1-15
	* Revert back to the upper-bound estimates to use immediately bigger order.
	
2011-10-29
	* Serious bugfix: very rare case may happen to query a node which does not exists
	* Added an efficient ngram state representation

2011-10-25
	* Added ngram_prefix and ngram_suffix for ngram state computation

2011-9-20
	* Serious bugfix for mpi-based programs: use of zlib_{compressor,decompressor} when sending data

2011-8-22
	* Serious bugfix for mpi-based ngram counts accumulation when readling from single files: check for terminatin condition
	
2011-5-9
	* Quantization fix for unigram probability for <s>

2011-4-18
	* unigram probability <s> is assigned -99.0, taken from SRILM

2011-2-17
	* Added ngram.py a wrapper script to compute a ngram language model either from
	  a corpus, a list of corpora, or counts (in Google format).

2011-2-14
	* Added ngram_vocab{,_mpi} for computing vocabulary, then
	  use it to restrict vocabulary in ngram_counts_extract{,_mpi}
	* Internal change:
	  - Faster vocabulary lookup by eliminating temporary buffer
	  - (Potentially) faster integer conversion by boost.spirit

2010-5-7
	* Better caching for child-node traversal
	* Better caching for backoff traversal
	* Removed dependence on libcodec. (deprecated?)

2009-10-14
	* Ignore unindexed ngrams when computing ngram upper-bound score
	* Upper-bound estimate will use immediately longer ngrams. For instance, upper
	  bound for 3-gram will be computed by 4-grams, but not 5,6,7-grams etc.
	* Bugfix for memory management at ngram_counts_extract

2009-7-1
	* ngram_counts_estimate can starts from ngram counts in Google format :)
	    - For larger data, it is recommended to run separately, but you can, for instance, run
	      ngram_counts_estimate directly after ngram_counts_extract

2009-6-22
	* The expgram will concentrate on ngrams, not other stuff:
	    - Completely removed ICU and boost.regex
	    - Completely removed stemmer/speller

	* Added smooth_smallest to logprob(first, last) and logbound(first, last) when querying logprobabilities

2009-6-1
	* Renamed ngram_counts/ngram_counts_mpi to ngram_counts_extract/ngram_counts_extract_mpi, respectively.
	
	* Faster ngram_counts_extract/ngram_counts_extract_mpi
	     - Use non-compact hash-table at root level.
	     - Vocab-mapping tweaking when dumping ngram-counts.

	* Added --filter to ngram_counts_extract/ngram_counts_extract_mpi in order to perform text filtering, such as tokenization.

	* Added ngram_stat/ngram_counts_stat to show some statiscs on indexed size.

	* Added ngram_perplexiy
	
	* Implemented "mpish", a tool to perform mapping of set of shell-command.
	  For instance, you can run:
	      for ((i=0;i<1024;++i)) do; echo "uname -a" done | mpirun --host <list-of-host-delimited-by-,> --np <#-of-processes> mpish 
	  This will run exact 1024 "uname -a" split on machines (indicated by --host), with  --np processees. The tasks are not evently split, but
	  perform simple scheduling.
	  
2009-5-25
	* Initial release.
