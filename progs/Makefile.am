AM_CPPFLAGS = -I.. $(BOOST_CPPFLAGS) $(PTHREAD_FLAGS) $(TCMALLOC_CPPFLAGS) $(PROFILER_CPPFLAGS)

perftools_LDADD = $(JEMALLOC_LDFLAGS) $(TCMALLOC_LDFLAGS) $(PROFILER_LDFLAGS)

boost_LDADD = \
	$(BOOST_THREAD_LDFLAGS) $(BOOST_THREAD_LIBS) \
	$(BOOST_PROGRAM_OPTIONS_LDFLAGS) $(BOOST_PROGRAM_OPTIONS_LIBS) \
	$(BOOST_FILESYSTEM_LDFLAGS) $(BOOST_FILESYSTEM_LIBS) \
	$(BOOST_IOSTREAMS_LDFLAGS) $(BOOST_IOSTREAMS_LIBS) 

LIBEXPGRAM = $(top_builddir)/expgram/libexpgram.la
LIBUTILS   = $(top_builddir)/utils/libutils.la

if WITH_MPI
bin_mpi = \
	expgram_quantize_mpi \
	expgram_backward_mpi \
	expgram_bound_mpi \
	expgram_counts_extract_mpi \
	expgram_counts_index_mpi \
	expgram_counts_modify_mpi \
	expgram_counts_estimate_mpi \
	expgram_vocab_mpi
endif WITH_MPI

bin_PROGRAMS = \
	expgram \
	expgram_index \
	expgram_dump \
	expgram_quantize \
	expgram_backward \
	expgram_bound \
	expgram_diff \
	expgram_stat \
	expgram_perplexity \
	expgram_counts_extract \
	expgram_counts_index \
	expgram_counts_modify \
	expgram_counts_dump \
	expgram_counts_estimate \
	expgram_counts_stat \
	expgram_vocab \
	\
	expgram_clean \
	$(bin_mpi)


noinst_HEADERS = \
	expgram_counts_extract_impl.hpp \
	expgram_vocab_impl.hpp

noinst_DATA = .gitignore

.gitignore: Makefile.am
	cat $(top_builddir)/.gitignore > .gitignore
	for bin in $(PROGRAMS); do \
	  echo $$bin >> .gitignore; \
	done; \
	for bin in $(SCRIPTS); do \
	  echo $$bin >> .gitignore; \
	done

expgram_SOURCES = expgram.cpp
expgram_LDADD   = $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

expgram_backward_SOURCES = expgram_backward.cpp
expgram_backward_LDADD   = $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

expgram_clean_SOURCES = expgram_clean.cpp
expgram_clean_LDADD   = $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

expgram_bound_SOURCES = expgram_bound.cpp
expgram_bound_LDADD   = $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

expgram_counts_dump_SOURCES = expgram_counts_dump.cpp
expgram_counts_dump_LDADD   = $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

expgram_counts_estimate_SOURCES = expgram_counts_estimate.cpp
expgram_counts_estimate_LDADD   = $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

expgram_counts_extract_SOURCES = expgram_counts_extract.cpp expgram_counts_extract_impl.hpp
expgram_counts_extract_LDADD   = $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

expgram_counts_index_SOURCES = expgram_counts_index.cpp
expgram_counts_index_LDADD   = $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

expgram_counts_modify_SOURCES = expgram_counts_modify.cpp
expgram_counts_modify_LDADD   = $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

expgram_counts_stat_SOURCES = expgram_counts_stat.cpp
expgram_counts_stat_LDADD   = $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

expgram_diff_SOURCES = expgram_diff.cpp
expgram_diff_LDADD   = $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

expgram_index_SOURCES = expgram_index.cpp
expgram_index_LDADD   = $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

expgram_dump_SOURCES = expgram_dump.cpp
expgram_dump_LDADD   = $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

expgram_stat_SOURCES = expgram_stat.cpp
expgram_stat_LDADD   = $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

expgram_perplexity_SOURCES = expgram_perplexity.cpp
expgram_perplexity_LDADD   = $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

expgram_quantize_SOURCES = expgram_quantize.cpp
expgram_quantize_LDADD   = $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

expgram_vocab_SOURCES = expgram_vocab.cpp expgram_vocab_impl.hpp
expgram_vocab_LDADD   = $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

if WITH_MPI

expgram_backward_mpi_SOURCES = expgram_backward_mpi.cpp
expgram_backward_mpi_CPPFLAGS = $(MPI_CPPFLAGS) $(AM_CPPFLAGS)
expgram_backward_mpi_LDADD   = $(MPI_LDFLAGS) $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

expgram_bound_mpi_SOURCES = expgram_bound_mpi.cpp
expgram_bound_mpi_CPPFLAGS = $(MPI_CPPFLAGS) $(AM_CPPFLAGS)
expgram_bound_mpi_LDADD   = $(MPI_LDFLAGS) $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

expgram_counts_estimate_mpi_SOURCES = expgram_counts_estimate_mpi.cpp
expgram_counts_estimate_mpi_CPPFLAGS = $(MPI_CPPFLAGS) $(AM_CPPFLAGS)
expgram_counts_estimate_mpi_LDADD   = $(MPI_LDFLAGS) $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

expgram_counts_extract_mpi_SOURCES = expgram_counts_extract_mpi.cpp expgram_counts_extract_impl.hpp
expgram_counts_extract_mpi_CPPFLAGS = $(MPI_CPPFLAGS) $(AM_CPPFLAGS)
expgram_counts_extract_mpi_LDADD   = $(MPI_LDFLAGS) $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

expgram_counts_index_mpi_SOURCES = expgram_counts_index_mpi.cpp
expgram_counts_index_mpi_CPPFLAGS = $(MPI_CPPFLAGS) $(AM_CPPFLAGS)
expgram_counts_index_mpi_LDADD   = $(MPI_LDFLAGS) $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

expgram_counts_modify_mpi_SOURCES = expgram_counts_modify_mpi.cpp
expgram_counts_modify_mpi_CPPFLAGS = $(MPI_CPPFLAGS) $(AM_CPPFLAGS)
expgram_counts_modify_mpi_LDADD   = $(MPI_LDFLAGS) $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

expgram_quantize_mpi_SOURCES = expgram_quantize_mpi.cpp
expgram_quantize_mpi_CPPFLAGS = $(MPI_CPPFLAGS) $(AM_CPPFLAGS)
expgram_quantize_mpi_LDADD   = $(MPI_LDFLAGS) $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

expgram_vocab_mpi_SOURCES = expgram_vocab_mpi.cpp expgram_vocab_impl.hpp
expgram_vocab_mpi_CPPFLAGS = $(MPI_CPPFLAGS) $(AM_CPPFLAGS)
expgram_vocab_mpi_LDADD   = $(MPI_LDFLAGS) $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

endif WITH_MPI
