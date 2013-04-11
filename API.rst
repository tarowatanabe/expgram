====
APIs
====

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
