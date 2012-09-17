
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
alloc_vector.hpp \
allocinfo_allocator.hpp \
arc_list.hpp \
array_power2.hpp \
async_device.hpp \
atomicop.hpp \
base64.hpp \
bithack.hpp \
bounded_queue.hpp \
byte_aligned_code.hpp \
chunk_vector.hpp \
compact_trie.hpp \
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
unordered_map.hpp \
unordered_set.hpp \
vector2.hpp \
vector3.hpp \
vector2_aligned.hpp \
vector3_aligned.hpp \
vertical_coded_device.hpp \
vertical_coded_vector.hpp

noinst_PROGRAMS = \
sample_arc_list \
sample_bithack \
sample_byte_aligned_code \
sample_chunk_vector \
sample_compress_stream \
sample_group_aligned_code \
sample_indexed_set \
sample_istream_line_iterator \
sample_lockfree_list_queue \
sample_lockfree_queue \
sample_map_file_allocator \
sample_packed_vector \
sample_simple_vector \
sample_subprocess \
sample_succinct_vector \
sample_tempfile \
sample_vector2 \
sample_vertical_coded_vector

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

sample_group_aligned_code.cpp : group_aligned_code.hpp group_aligned_code_offsets.hpp

group_aligned_code.hpp : group_aligned_code_offsets.hpp

group_aligned_code_offsets.hpp : group_aligned_code_offsets.py
	./group_aligned_code_offsets.py > group_aligned_code_offsets.hpp

sample_arc_list_SOURCES = sample_arc_list.cpp
sample_arc_list_LDFLAGS = $(BOOST_THREAD_LDFLAGS) 
sample_arc_list_LDADD = $(BOOST_THREAD_LIBS)

sample_bithack_SOURCES = sample_bithack.cpp

sample_byte_aligned_code_SOURCES = sample_byte_aligned_code.cpp

sample_chunk_vector_SOURCES = sample_chunk_vector.cpp
sample_chunk_vector_LDFLAGS = $(BOOST_THREAD_LDFLAGS)
sample_chunk_vector_LDADD = $(BOOST_THREAD_LIBS)

sample_compress_stream_SOURCES = sample_compress_stream.cpp
sample_compress_stream_LDADD = $(LIBUTILS)

sample_group_aligned_code_SOURCES = sample_group_aligned_code.cpp

sample_indexed_set_SOURCES = sample_indexed_set.cpp
sample_indexed_set_LDFLAGS = $(BOOST_THREAD_LDFLAGS) $(BOOST_THREAD_LIBS)

sample_istream_line_iterator_SOURCES = sample_istream_line_iterator.cpp

sample_lockfree_list_queue_SOURCES = sample_lockfree_list_queue.cpp
sample_lockfree_list_queue_LDFLAGS = $(BOOST_THREAD_LDFLAGS)
sample_lockfree_list_queue_LDADD = $(BOOST_THREAD_LIBS) 

sample_lockfree_queue_SOURCES = sample_lockfree_queue.cpp
sample_lockfree_queue_LDFLAGS = $(BOOST_THREAD_LDFLAGS)
sample_lockfree_queue_LDADD = $(BOOST_THREAD_LIBS) 

sample_map_file_allocator_SOURCES = sample_map_file_allocator.cpp
sample_map_file_allocator_LDFLAGS = $(BOOST_THREAD_LDFLAGS)
sample_map_file_allocator_LDADD = $(BOOST_FILESYSTEM_LIBS) $(BOOST_IOSTREAMS_LIBS) $(BOOST_THREAD_LIBS) $(LIBUTILS) $(perftools_LDADD)

sample_packed_vector_SOURCES = sample_packed_vector.cpp
sample_packed_vector_LDFLAGS = $(BOOST_FILESYSTEM_LDFLAGS) $(BOOST_THREAD_LDFLAGS)
sample_packed_vector_LDADD = $(BOOST_FILESYSTEM_LIBS) $(BOOST_IOSTREAMS_LIBS) $(BOOST_THREAD_LIBS) $(LIBUTILS)

sample_simple_vector_SOURCES = sample_simple_vector.cpp
sample_simple_vector_LDFLAGS = $(BOOST_THREAD_LDFLAGS)
sample_simple_vector_LDADD = $(BOOST_THREAD_LIBS)

sample_subprocess_SOURCES = sample_subprocess.cpp
sample_subprocess_LDFLAGS = $(BOOST_IOSTREAMS_LDFLAGS) $(BOOST_THREAD_LDFLAGS) $(BOOST_FILESYSTEM_LDFLAGS)
sample_subprocess_LDADD = $(BOOST_IOSTREAMS_LIBS)  $(BOOST_THREAD_LIBS) $(BOOST_FILESYSTEM_LIBS)

sample_succinct_vector_SOURCES = sample_succinct_vector.cpp
sample_succinct_vector_LDFLAGS = $(BOOST_FILESYSTEM_LDFLAGS) $(BOOST_THREAD_LDFLAGS)
sample_succinct_vector_LDADD = $(BOOST_FILESYSTEM_LIBS) $(BOOST_IOSTREAMS_LIBS) $(BOOST_THREAD_LIBS)

sample_tempfile_SOURCES = sample_tempfile.cpp
sample_tempfile_LDFLAGS = $(BOOST_THREAD_LDFLAGS)
sample_tempfile_LDADD = $(BOOST_FILESYSTEM_LIBS) $(BOOST_THREAD_LIBS)  $(LIBUTILS)

sample_vector2_SOURCES = sample_vector2.cpp

sample_vertical_coded_vector_SOURCES = sample_vertical_coded_vector.cpp
sample_vertical_coded_vector_LDFLAGS = $(BOOST_FILESYSTEM_LDFLAGS) $(BOOST_THREAD_LDFLAGS)
sample_vertical_coded_vector_LDADD = $(BOOST_FILESYSTEM_LIBS) $(BOOST_IOSTREAMS_LIBS) $(BOOST_THREAD_LIBS)

