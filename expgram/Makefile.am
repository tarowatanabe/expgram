AM_CPPFLAGS = -I.. $(BOOST_CPPFLAGS) $(PTHREAD_FLAGS) $(TCMALLOC_CFLAGS) $(PROFILER_CFLAGS) $(SPARSEHASH_CPPFLAGS)

LIBUTILS=$(top_builddir)/utils/libutils.la
LIBCODEC=$(top_builddir)/codec/libcodec.la

lib_LTLIBRARIES = libexpgram.la

libexpgramincludedir= $(pkgincludedir)/expgram

libexpgraminclude_HEADERS = \
NGram.hpp \
NGramIndex.hpp \
Quantizer.hpp \
Sentence.hpp \
Vocab.hpp \
Word.hpp

libexpgram_la_SOURCES = \
NGram.cpp \
NGramIndex.cpp \
Sentence.cpp \
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
	$(BOOST_THREAD_LIBS) \
	$(BOOST_FILESYSTEM_LIBS) \
	$(BOOST_IOSTREAMS_LIBS) \
	$(BOOST_SYSTEM_LIBS)
