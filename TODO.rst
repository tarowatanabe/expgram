* Implement vocabulary mapper which simply transform text into
  vocabulary-id given ngram language model or counts

* Implement --min-ngram to perform filtering duing indexing (for bigrams and upward)

* Implement mini-expgram, which supports only loading.

* Support for ngram-suffix traversal in the backward ngram. Use
  an additional bit to indicate the end of context?

* Reduce memory consumption, especially when backward computation.
