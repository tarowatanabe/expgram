AM_CPPFLAGS = -I.. $(ICU_CPPFLAGS) $(BOOST_CPPFLAGS) $(PTHREAD_FLAGS) $(TCMALLOC_CPPFLAGS) $(PROFILER_CPPFLAGS)

perftools_LDADD = $(TCMALLOC_LDFLAGS) $(PROFILER_LDFLAGS)

boost_LDADD = \
	$(BOOST_THREAD_LDFLAGS) $(BOOST_THREAD_LIBS) \
	$(BOOST_PROGRAM_OPTIONS_LDFLAGS) $(BOOST_PROGRAM_OPTIONS_LIBS) \
	$(BOOST_FILESYSTEM_LDFLAGS) $(BOOST_FILESYSTEM_LIBS) \
	$(BOOST_IOSTREAMS_LDFLAGS) $(BOOST_IOSTREAMS_LIBS) \
	$(BOOST_SYSTEM_LDFLAGS) $(BOOST_SYSTEM_LIBS)

LIBEXPGRAM = $(top_builddir)/expgram/libexpgram.la
LIBUTILS   = $(top_builddir)/utils/libutils.la

noinst_PROGRAMS = \
	ngram \
	ngram_counts

noinst_SCRIPTS = \
	web1t-en.sh \
	web1t-ja.sh \
	web1t-zh.sh

noinst_DATA = .gitignore

EXTRA_DIST = \
	$(noinst_SCRIPTS)

.gitignore: Makefile.am
	cat $(top_builddir)/.gitignore > .gitignore
	for bin in $(PROGRAMS); do \
	  echo $$bin >> .gitignore; \
	done; \
	for bin in $(SCRIPTS); do \
	  echo $$bin >> .gitignore; \
	done

ngram_SOURCES = ngram.cpp
ngram_LDADD   = $(LIBEXPGRAM) $(boost_LDADD) $(perftools_LDADD)

ngram_counts_SOURCES = ngram_counts.cpp
ngram_counts_LDADD   = $(LIBEXPGRAM) $(boost_LDADD) $(perftools_LDADD)
