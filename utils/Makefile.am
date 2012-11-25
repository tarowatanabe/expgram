
AM_CPPFLAGS = -I$(top_builddir) $(BOOST_CPPFLAGS) $(TCMALLOC_CPPFLAGS) $(PTHREAD_FLAGS) $(JEMALLOC_CPPFLAGS)

perftools_LDADD = $(JEMALLOC_LDFLAGS) $(TCMALLOC_LDFLAGS) $(PROFILER_LDFLAGS)

LIBUTILS=libutils.la

lib_LTLIBRARIES = libutils.la

libutils_la_SOURCES = \
	malloc_stats.cpp \
	map_file_allocator.cpp \
	tempfile.cpp

libutils_la_LDFLAGS = -version-info $(EXPGRAM_LTVERSION)

libutils_la_LIBADD = \
	$(BOOST_THREAD_LDFLAGS) $(BOOST_THREAD_LIBS) \
	$(BOOST_IOSTREAMS_LDFLAGS) $(BOOST_IOSTREAMS_LIBS) \
	$(BOOST_FILESYSTEM_LDFLAGS) $(BOOST_FILESYSTEM_LIBS) \
	$(JEMALLOC_LDFLAGS) \
	$(TCMALLOC_LDFLAGS)

libutilsincludedir = $(pkgincludedir)/utils
libutilsinclude_HEADERS = \
allocinfo_allocator.hpp \
array_power2.hpp \
async_device.hpp \
atomicop.hpp \
base64.hpp \
bithack.hpp \
bounded_queue.hpp \
byte_aligned_code.hpp \
chunk_vector.hpp \
compact_func.hpp \
compact_hashtable.hpp \
compact_map.hpp \
compact_set.hpp \
compress_stream.hpp \
config.hpp \
config.h \
filesystem.hpp \
group_aligned_code.hpp \
group_aligned_code_offsets.hpp \
hashmurmur.hpp \
indexed_hashtable.hpp \
indexed_map.hpp \
indexed_set.hpp \
istream_line_iterator.hpp \
lexical_cast.hpp \
lockfree_list_queue.hpp \
lockfree_queue.hpp \
malloc_stats.hpp \
map_file.hpp \
map_file_allocator.hpp \
mathop.hpp \
memory.hpp \
mpi.hpp \
mpi_allocator.hpp \
mpi_device.hpp \
mpi_device_bcast.hpp \
mpi_stream.hpp \
mpi_stream_simple.hpp \
mpi_traits.hpp \
packed_device.hpp \
packed_vector.hpp \
piece.hpp \
program_options.hpp \
repository.hpp \
resource.hpp \
rwticket.hpp \
simple_vector.hpp \
space_separator.hpp \
spinlock.hpp \
subprocess.hpp \
succinct_vector.hpp \
tempfile.hpp \
thread_specific_ptr.hpp \
trie.hpp \
trie_compact.hpp \
unordered_map.hpp \
unordered_set.hpp \
vector2.hpp \
vector3.hpp \
vector2_aligned.hpp \
vector3_aligned.hpp \
vertical_coded_device.hpp \
vertical_coded_vector.hpp

noinst_PROGRAMS = \
bithack_main \
byte_aligned_code_main \
chunk_vector_main \
compress_stream_main \
group_aligned_code_main \
indexed_set_main \
istream_line_iterator_main \
lockfree_list_queue_main \
lockfree_queue_main \
map_file_allocator_main \
packed_vector_main \
simple_vector_main \
subprocess_main \
succinct_vector_main \
tempfile_main \
vector2_main \
vertical_coded_vector_main

dist_noinst_SCRIPTS = \
group_aligned_code_offsets.py

IGNORES = \
config.h \
config.h.in \
group_aligned_code_offsets.hpp \
stamp-h1

noinst_DATA = .gitignore

.gitignore: Makefile.am
	cat $(top_builddir)/.gitignore > .gitignore
	for bin in $(PROGRAMS); do \
	  echo $$bin >> .gitignore; \
	done; \
	for bin in $(SCRIPTS); do \
	  echo $$bin >> .gitignore; \
	done; \
	for bin in $(IGNORES); do \
	  echo $$bin >> .gitignore; \
	done

group_aligned_code_main.cpp : group_aligned_code.hpp group_aligned_code_offsets.hpp

group_aligned_code.hpp : group_aligned_code_offsets.hpp

group_aligned_code_offsets.hpp : group_aligned_code_offsets.py
	./group_aligned_code_offsets.py > group_aligned_code_offsets.hpp

bithack_main_SOURCES = bithack_main.cpp

byte_aligned_code_main_SOURCES = byte_aligned_code_main.cpp

chunk_vector_main_SOURCES = chunk_vector_main.cpp
chunk_vector_main_LDFLAGS = $(BOOST_THREAD_LDFLAGS)
chunk_vector_main_LDADD = $(BOOST_THREAD_LIBS)

compress_stream_main_SOURCES = compress_stream_main.cpp
compress_stream_main_LDADD = $(LIBUTILS)

group_aligned_code_main_SOURCES = group_aligned_code_main.cpp

indexed_set_main_SOURCES = indexed_set_main.cpp
indexed_set_main_LDFLAGS = $(BOOST_THREAD_LDFLAGS) $(BOOST_THREAD_LIBS)

istream_line_iterator_main_SOURCES = istream_line_iterator_main.cpp

lockfree_list_queue_main_SOURCES = lockfree_list_queue_main.cpp
lockfree_list_queue_main_LDFLAGS = $(BOOST_THREAD_LDFLAGS)
lockfree_list_queue_main_LDADD = $(BOOST_THREAD_LIBS)

lockfree_queue_main_SOURCES = lockfree_queue_main.cpp
lockfree_queue_main_LDFLAGS = $(BOOST_THREAD_LDFLAGS)
lockfree_queue_main_LDADD = $(BOOST_THREAD_LIBS)

map_file_allocator_main_SOURCES = map_file_allocator_main.cpp
map_file_allocator_main_LDFLAGS = $(BOOST_THREAD_LDFLAGS)
map_file_allocator_main_LDADD = $(BOOST_FILESYSTEM_LIBS) $(BOOST_IOSTREAMS_LIBS) $(BOOST_THREAD_LIBS) $(LIBUTILS) $(perftools_LDADD)

packed_vector_main_SOURCES = packed_vector_main.cpp
packed_vector_main_LDFLAGS = $(BOOST_FILESYSTEM_LDFLAGS) $(BOOST_THREAD_LDFLAGS)
packed_vector_main_LDADD = $(BOOST_FILESYSTEM_LIBS) $(BOOST_IOSTREAMS_LIBS) $(BOOST_THREAD_LIBS) $(LIBUTILS)

simple_vector_main_SOURCES = simple_vector_main.cpp
simple_vector_main_LDFLAGS = $(BOOST_THREAD_LDFLAGS)
simple_vector_main_LDADD = $(BOOST_THREAD_LIBS)

subprocess_main_SOURCES = subprocess_main.cpp
subprocess_main_LDFLAGS = $(BOOST_IOSTREAMS_LDFLAGS) $(BOOST_THREAD_LDFLAGS) $(BOOST_FILESYSTEM_LDFLAGS)
subprocess_main_LDADD = $(BOOST_IOSTREAMS_LIBS) $(BOOST_FILESYSTEM_LIBS) $(BOOST_THREAD_LIBS)

succinct_vector_main_SOURCES = succinct_vector_main.cpp
succinct_vector_main_LDFLAGS = $(BOOST_FILESYSTEM_LDFLAGS) $(BOOST_THREAD_LDFLAGS)
succinct_vector_main_LDADD = $(BOOST_FILESYSTEM_LIBS) $(BOOST_IOSTREAMS_LIBS) $(BOOST_THREAD_LIBS)

tempfile_main_SOURCES = tempfile_main.cpp
tempfile_main_LDFLAGS = $(BOOST_THREAD_LDFLAGS)
tempfile_main_LDADD = $(BOOST_FILESYSTEM_LIBS) $(BOOST_THREAD_LIBS) $(LIBUTILS)

vector2_main_SOURCES = vector2_main.cpp

vertical_coded_vector_main_SOURCES = vertical_coded_vector_main.cpp
vertical_coded_vector_main_LDFLAGS = $(BOOST_FILESYSTEM_LDFLAGS) $(BOOST_THREAD_LDFLAGS)
vertical_coded_vector_main_LDADD = $(BOOST_FILESYSTEM_LIBS) $(BOOST_IOSTREAMS_LIBS) $(BOOST_THREAD_LIBS)


