AM_CPPFLAGS = -I.. $(BOOST_CPPFLAGS) $(ICU_CPPFLAGS) $(PTHREAD_FLAGS) $(TCMALLOC_CPPFLAGS) $(PROFILER_CPPFLAGS) $(SPARSEHASH_CPPFLAGS)

LIBUTILS=$(top_builddir)/utils/libutils.la
LIBCODEC=$(top_builddir)/codec/libcodec.la
LIBSTEMMER=$(top_builddir)/stemmer/libstemmer.la

lib_LTLIBRARIES = libexpgram.la

libexpgramincludedir= $(pkgincludedir)/expgram

libexpgraminclude_HEADERS = \
NGram.hpp \
NGramCounts.hpp \
NGramIndex.hpp \
Sentence.hpp \
Stemmer.hpp \
Vocab.hpp \
Word.hpp

noinst_HEADERS = \
Discount.hpp \
Quantizer.hpp \
NGramCountsIndexer.hpp

libexpgram_la_SOURCES = \
NGram.cpp \
NGramCounts.cpp \
NGramIndex.cpp \
Sentence.cpp \
Stemmer.cpp \
Vocab.cpp \
Word.cpp

libexpgram_la_LDFLAGS = \
	$(PTHREAD_FLAGS) \
	$(BOOST_THREAD_LDFLAGS) \
	$(BOOST_FILESYSTEM_LDFLAGS) \
	$(BOOST_IOSTREAMS_LDFLAGS) \
	$(BOOST_SYSTEM_LDFLAGS)

libexpgram_la_LIBADD = \
	$(LIBUTILS) \
	$(LIBCODEC) \
	$(LIBSTEMMER) \
	$(ICU_LDFLAGS) \
	$(BOOST_THREAD_LIBS) \
	$(BOOST_FILESYSTEM_LIBS) \
	$(BOOST_IOSTREAMS_LIBS) \
	$(BOOST_SYSTEM_LIBS)
