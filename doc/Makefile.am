
if WITH_RST2MAN

if WITH_MPI
man_mpi = \
expgram_bound_mpi.man \
expgram_counts_estimate_mpi.man \
expgram_counts_extract_mpi.man \
expgram_counts_modify_mpi.man \
expgram_quantize_mpi.man \
expgram_vocab_mpi.man
endif WITH_MPI

man1_MANS = \
expgram.py.man \
expgram.man \
expgram_bound.man \
expgram_diff.man \
expgram_clean.man \
expgram_counts_diff.man \
expgram_counts_dump.man \
expgram_counts_estimate.man \
expgram_counts_extract.man \
expgram_counts_modify.man \
expgram_counts_stat.man \
expgram_dump.man \
expgram_index.man \
expgram_perplexity.man \
expgram_quantize.man \
expgram_stat.man \
expgram_vocab.man \
$(man_mpi)


.rst.man: $*.rst
	$(RST2MAN) < $*.rst > $*.man

endif WITH_RST2MAN
