include mkinc_utf8.mak

lib_LTLIBRARIES = libstemmer.la

INCLUDES =-I$(top_builddir)/stemmer/include

libstemmerincludedir = $(pkgincludedir)/libstemmer

libstemmerinclude_HEADERS = include/libstemmer.h
noinst_HEADERS = $(snowball_headers)

libstemmer_la_SOURCES = $(snowball_sources)

noinst_PROGRAMS =  stemwords

stemwords_SOURCES = examples/stemwords.c
stemwords_LDADD = libstemmer.la
