
if WITH_RST2MAN

if WITH_MPI
man_mpi = \
expgram_backward_mpi.man \
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
expgram_backward.man \
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

EXTRA_DIST = \
expgram.py.rst \
expgram.rst \
expgram_backward.rst \
expgram_backward_mpi.rst \
expgram_bound.rst \
expgram_bound_mpi.rst \
expgram_clean.rst \
expgram_counts_diff.rst \
expgram_counts_dump.rst \
expgram_counts_estimate.rst \
expgram_counts_estimate_mpi.rst \
expgram_counts_extract.rst \
expgram_counts_extract_mpi.rst \
expgram_counts_modify.rst \
expgram_counts_modify_mpi.rst \
expgram_counts_stat.rst \
expgram_diff.rst \
expgram_dump.rst \
expgram_index.rst \
expgram_perplexity.rst \
expgram_quantize.rst \
expgram_quantize_mpi.rst \
expgram_stat.rst \
expgram_vocab.rst \
expgram_vocab_mpi.rst
