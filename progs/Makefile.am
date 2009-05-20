AM_CPPFLAGS = -I.. $(ICU_CPPFLAGS) $(BOOST_CPPFLAGS) $(PTHREAD_FLAGS) $(TCMALLOC_CPPFLAGS) $(PROFILER_CPPFLAGS) $(SPARSEHASH_CPPFLAGS)

perftools_LDADD = $(TCMALLOC_LDFLAGS) $(PROFILER_LDFLAGS)

boost_LDADD = \
	$(BOOST_THREAD_LDFLAGS) $(BOOST_THREAD_LIBS) \
	$(BOOST_PROGRAM_OPTIONS_LDFLAGS) $(BOOST_PROGRAM_OPTIONS_LIBS) \
	$(BOOST_FILESYSTEM_LDFLAGS) $(BOOST_FILESYSTEM_LIBS) \
	$(BOOST_IOSTREAMS_LDFLAGS) $(BOOST_IOSTREAMS_LIBS) \
	$(BOOST_SYSTEM_LDFLAGS) $(BOOST_SYSTEM_LIBS)

LIBEXPGRAM = $(top_builddir)/expgram/libexpgram.la
LIBCODEC   = $(top_builddir)/codec/libcodec.la
LIBUTILS   = $(top_builddir)/utils/libutils.la

bin_PROGRAMS = \
	ngram_counts \
	ngram_counts_mpi \
	\
	ngram_index \
	ngram_dump \
	ngram_quantize \
	ngram_quantize_mpi \
	ngram_bound \
	ngram_bound_mpi \
	\
	ngram_counts_index \
	ngram_counts_index_mpi \
	ngram_counts_modify \
	ngram_counts_modify_mpi \
	ngram_counts_dump \
	ngram_counts_estimate \
	\
	tokenize_en

noinst_HEADERS = \
	ngram_counts_impl.hpp

### ngram counts accumulation
ngram_counts_SOURCES = ngram_counts.cpp
ngram_counts_LDADD   = $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

ngram_counts_mpi_SOURCES = ngram_counts_mpi.cpp
ngram_counts_mpi_CPPFLAGS = $(MPI_CPPFLAGS) $(AM_CPPFLAGS)
ngram_counts_mpi_LDADD   = $(MPI_LDFLAGS) $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

### ngram language model
ngram_index_SOURCES = ngram_index.cpp
ngram_index_LDADD   = $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

ngram_dump_SOURCES = ngram_dump.cpp
ngram_dump_LDADD   = $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

ngram_quantize_SOURCES = ngram_quantize.cpp
ngram_quantize_LDADD   = $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

ngram_quantize_mpi_SOURCES = ngram_quantize_mpi.cpp
ngram_quantize_mpi_CPPFLAGS = $(MPI_CPPFLAGS) $(AM_CPPFLAGS)
ngram_quantize_mpi_LDADD   = $(MPI_LDFLAGS) $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

ngram_bound_SOURCES = ngram_bound.cpp
ngram_bound_LDADD   = $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

ngram_bound_mpi_SOURCES = ngram_bound_mpi.cpp
ngram_bound_mpi_CPPFLAGS = $(MPI_CPPFLAGS) $(AM_CPPFLAGS)
ngram_bound_mpi_LDADD   = $(MPI_LDFLAGS) $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

### ngram counts
ngram_counts_index_SOURCES = ngram_counts_index.cpp
ngram_counts_index_LDADD   = $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

ngram_counts_index_mpi_SOURCES = ngram_counts_index_mpi.cpp
ngram_counts_index_mpi_CPPFLAGS = $(MPI_CPPFLAGS) $(AM_CPPFLAGS)
ngram_counts_index_mpi_LDADD   = $(MPI_LDFLAGS) $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

ngram_counts_modify_SOURCES = ngram_counts_modify.cpp
ngram_counts_modify_LDADD   = $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

ngram_counts_modify_mpi_SOURCES = ngram_counts_modify_mpi.cpp
ngram_counts_modify_mpi_CPPFLAGS = $(MPI_CPPFLAGS) $(AM_CPPFLAGS)
ngram_counts_modify_mpi_LDADD   = $(MPI_LDFLAGS) $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

ngram_counts_dump_SOURCES = ngram_counts_dump.cpp
ngram_counts_dump_LDADD   = $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

ngram_counts_estimate_SOURCES = ngram_counts_estimate.cpp
ngram_counts_estimate_LDADD   = $(LIBEXPGRAM) $(LIBUTILS) $(boost_LDADD) $(perftools_LDADD)

### tokenizer
tokenize_en_SOURCES = tokenize_en.cpp
tokenize_en_LDADD   = $(LIBUTILS) $(ICU_LDFLAGS) $(boost_LDADD) $(perftools_LDADD)
