AM_CPPFLAGS = -I.. $(BOOST_CPPFLAGS)

LIBCODEC = $(top_builddir)/codec/libcodec.la

lib_LTLIBRARIES = libcodec.la

libcodec_la_SOURCES = \
	fastlz_codec.cpp \
	fastlz.c \
	quicklz_codec.cpp

libcodecincludedir = $(pkgincludedir)/codec
libcodecinclude_HEADERS = \
	codec.hpp \
	fastlz_codec.hpp \
	quicklz_codec.hpp \
	zlib_codec.hpp \
	bzip2_codec.hpp \
	pfor_codec.hpp \
	\
	block_device.hpp \
	block_file.hpp

noinst_PROGRAMS = \
sample_block_sink \
sample_block_file

sample_block_sink_SOURCES = sample_block_sink.cpp
sample_block_sink_LDFLAGS = $(BOOST_FILESYSTEM_LDFLAGS)
sample_block_sink_LDADD = $(BOOST_FILESYSTEM_LIBS) $(BOOST_IOSTREAMS_LIBS) $(BOOST_SYSTEM_LIBS) $(LIBCODEC)

sample_block_file_SOURCES = sample_block_file.cpp
sample_block_file_LDFLAGS = $(BOOST_FILESYSTEM_LDFLAGS) $(BOOST_THREAD_LDFLAGS)
sample_block_file_LDADD = $(BOOST_FILESYSTEM_LIBS) $(BOOST_IOSTREAMS_LIBS) $(BOOST_SYSTEM_LIBS) $(BOOST_THREAD_LIBS) $(LIBCODEC)
