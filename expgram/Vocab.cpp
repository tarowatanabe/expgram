
#include "Word.hpp"
#include "Vocab.hpp"

#include "utils/filesystem.hpp"
#include "utils/sgi_hash_map.hpp"
#include "utils/atomicop.hpp"
#include "utils/spinlock.hpp"

#include <unicode/uchar.h>
#include <unicode/unistr.h>
#include <unicode/schriter.h>
#include <unicode/bytestream.h>

namespace expgram
{
  // constants...
  const Vocab::word_type Vocab::EMPTY = Vocab::word_type("");
  const Vocab::word_type Vocab::NONE = Vocab::word_type("<none>");
  const Vocab::word_type Vocab::UNK  = Vocab::word_type("<unk>");
  const Vocab::word_type Vocab::BOS  = Vocab::word_type("<s>");
  const Vocab::word_type Vocab::EOS  = Vocab::word_type("</s>");
  
  struct WordCache
  {
    typedef Vocab::word_type   word_type;
    typedef word_type::id_type id_type;
    
    struct Cache
    {
      typedef uint64_t value_type;
      Cache() : value(value_type(-1)) {}
      Cache(const value_type& __value)  : value(__value) {}
      
      id_type id()   const { return (value) & 0xffffffff; }
      id_type word() const { return (value >> 32) & 0xffffffff; }
      
      volatile value_type value;
    };
    typedef Cache cache_type;
    typedef std::vector<cache_type, std::allocator<cache_type> > cache_set_type;
    
    WordCache() : caches(1024 * 16) {}
    
    cache_type operator[](id_type id)
    {
      return cache_type(utils::atomicop::fetch_and_add(caches[id & (caches.size() - 1)].value, uint64_t(0)));
    }
    
    void set(cache_type& cache_prev, id_type id, id_type word)
    {
      cache_type cache((uint64_t(word) << 32) || uint64_t(id));
      utils::atomicop::compare_and_swap(caches[id & (caches.size() - 1)].value, cache_prev.value, cache.value);
    }
    
    cache_set_type caches;
  };
  
  typedef WordCache word_cache_type;
  
#ifdef HAVE_TR1_UNORDERED_MAP
  typedef std::tr1::unordered_map<size_t, word_cache_type, boost::hash<size_t>, std::equal_to<size_t>,
				  std::allocator<std::pair<const size_t, word_cache_type> > > word_cache_set_type;
#else
  typedef sgi::hash_map<size_t, word_cache_type, boost::hash<size_t>, std::equal_to<size_t>,
			std::allocator<std::pair<const size_t, word_cache_type> > > word_cache_set_type;

#endif
  
  inline
  bool __is_tag(const std::string& word)
  {
    return (word.empty() || (word[0] == '<' && word[word.size() - 1] == '>'));
  }
  
  inline
  bool __is_tag(const Vocab::word_type& word)
  {
    if (word == Vocab::EMPTY || word == Vocab::NONE || word == Vocab::UNK || word == Vocab::BOS || word == Vocab::EOS)
      return true;
    else
      return __is_tag(static_cast<const std::string&>(word));
  }
  

  
  Vocab::word_type Vocab::prefix(const word_type& word, size_type size)
  {
    typedef word_cache_type::cache_type cache_type;
    typedef utils::spinlock spinlock_type;
    typedef spinlock_type::scoped_lock lock_type;
    
    static spinlock_type       __spinlock;
    static word_cache_set_type __caches;

    if (__is_tag(word)) return word;
    
    word_cache_type* caches;
    {
      lock_type lock(__spinlock);
      caches = &(__caches[size]);
    }
    
    cache_type cache = caches->operator[](word.id());
    
    if (cache.id() == word.id())
      return word_type(cache.word());
    
    const word_type word_prefix = prefix(static_cast<const std::string&>(word), size);
    
    caches->set(cache, word.id(), word_prefix.id());
    
    return word_prefix;
  }
  
  Vocab::word_type Vocab::suffix(const word_type& word, size_type size)
  {
    typedef word_cache_type::cache_type cache_type;
    typedef utils::spinlock spinlock_type;
    typedef spinlock_type::scoped_lock lock_type;
    
    static spinlock_type       __spinlock;
    static word_cache_set_type __caches;
    
    if (__is_tag(word)) return word;

    word_cache_type* caches;
    {
      lock_type lock(__spinlock);
      caches = &(__caches[size]);
    }
    
    cache_type cache = caches->operator[](word.id());
    
    if (cache.id() == word.id())
      return word_type(cache.word());
    
    const word_type word_suffix = suffix(static_cast<const std::string&>(word), size);
    
    caches->set(cache, word.id(), word_suffix.id());
    
    return word_suffix;
  }
  
  Vocab::word_type Vocab::digits(const word_type& word)
  {
    typedef word_cache_type::cache_type cache_type;
    
    static word_cache_type caches;
    
    if (__is_tag(word)) return word;

    cache_type cache = caches[word.id()];
    
    if (cache.id() == word.id())
      return word_type(cache.word());
    
    const word_type word_digits = digits(static_cast<const std::string&>(word));
    
    caches.set(cache, word.id(), word_digits.id());
    
    return word_digits;
  }
  
  std::string Vocab::prefix(const std::string& word, size_type size)
  {
    if (__is_tag(word)) return word;

    UnicodeString uword = UnicodeString::fromUTF8(word);
    
    const size_t index = uword.moveIndex32(0, int(size));
    
    UnicodeString uword_prefix;
    uword.extractBetween(0, index, uword_prefix);
    
    if (uword_prefix.length() < uword.length()) {
      uword_prefix.append('+');
      
      std::string word_prefix;
      StringByteSink<std::string> __sink(&word_prefix);
      uword_prefix.toUTF8(__sink);
      
      return word_prefix;
    } else
      return word;
  }
  
  std::string Vocab::suffix(const std::string& word, size_type size)
  {
    if (__is_tag(word)) return word;

    UnicodeString uword = UnicodeString::fromUTF8(word);
    
    const size_t index = uword.moveIndex32(uword.length(), - int(size));
    UnicodeString uword_suffix;
    uword.extractBetween(index, uword.length(), uword_suffix);
    
    if (uword_suffix.length () < uword.length()) {
      uword_suffix.insert(0, '+');
      
      std::string word_suffix;
      StringByteSink<std::string> __sink(&word_suffix);
      uword_suffix.toUTF8(__sink);
      
      return word_suffix;
    } else
      return word;
  }
  
  std::string Vocab::digits(const std::string& word)
  {
    if (__is_tag(word)) return word;

    UnicodeString uword = UnicodeString::fromUTF8(word);
    
    // at least front or back must have numeric property...
    if (u_getIntPropertyValue(uword.char32At(0), UCHAR_NUMERIC_TYPE) == U_NT_NONE
	&& u_getIntPropertyValue(uword.char32At(uword.length() - 1), UCHAR_NUMERIC_TYPE) == U_NT_NONE)
      return word;
    
    bool found = false;
    UnicodeString uword_digits("<digit-");
    StringCharacterIterator iter(uword);
    for (iter.setToStart(); iter.hasNext(); /**/) {
      UChar32 c = iter.next32PostInc();
      
      //const int32_t numeric_type = u_getIntPropertyValue(c, UCHAR_NUMERIC_TYPE);
      const double numeric_value = u_getNumericValue(c);
      const int32_t numeric_int = int(numeric_value);
      
      const bool replace =(numeric_value != U_NO_NUMERIC_VALUE
			   && double(numeric_int) == numeric_value
			   && 0 <= numeric_int
			   && numeric_int <= 9);
      
      found |= replace;
      uword_digits.append(replace ? '@' : c);
    }
    
    if (found) {
      uword_digits.append('>');
      
      std::string word_digits;
      StringByteSink<std::string> __sink(&word_digits);
      uword_digits.toUTF8(__sink);
      
      return word_digits;
    } else
      return word;
  }
  
  Vocab::stemmer_type Vocab::stemmer(const std::string& algorithm)
  {
    return stemmer_type(algorithm);
  }
  
  Word::id_type Vocab::insert(const std::string& word)
  {
    if (__succinct_hash_stream)
      return __succinct_hash_stream->insert(word.c_str(), word.size(), __hasher(word.begin(), word.end(), 0));

    if (! __succinct_hash)
      __succinct_hash.reset(new succinct_hash_type(1024 * 1024));

    if (__succinct_hash_mapped) {
      const hash_value_type hash_value = __hasher(word.begin(), word.end(), 0);
      
      word_type::id_type word_id_mapped = __succinct_hash_mapped->find(word.c_str(), word.size(), hash_value);
      if (word_id_mapped != succinct_hash_mapped_type::npos())
	return word_id_mapped;
      else
	return __succinct_hash->insert(word.c_str(), word.size(), hash_value) + __succinct_hash_mapped->size();
      
    } else
      return __succinct_hash->insert(word.c_str(), word.size(), __hasher(word.begin(), word.end(), 0));
  }
  
  void Vocab::write(const path_type& path) const
  {
    //if both of dynamic/static hash are open,

    if (__succinct_hash_mapped) {
      
      if (__succinct_hash && ! __succinct_hash->empty()) {
	succinct_hash_stream_type succinct_hash(path, (__succinct_hash_mapped->size() + __succinct_hash->size()) / 2);
	
	{
	  // insert data from mapped file
	  succinct_hash_mapped_type::const_iterator iter_end = __succinct_hash_mapped->end();
	  for (succinct_hash_mapped_type::const_iterator iter = __succinct_hash_mapped->begin(); iter != iter_end; ++ iter) {
	    const std::string word(iter.begin(), iter.end());
	    succinct_hash.insert(word.c_str(), word.size(), __hasher(word.begin(), word.end(), 0));
	  }
	}
	
	{
	  // insert data from raw storage...
	  succinct_hash_type::const_iterator iter_end = __succinct_hash->end();
	  for (succinct_hash_type::const_iterator iter = __succinct_hash->begin(); iter != iter_end; ++ iter)
	    succinct_hash.insert(&(*iter.begin()), iter.size(), __hasher(iter.begin(), iter.end(), 0));
	}
	
	// finally, dump!
	succinct_hash.close();
	
      } else
	__succinct_hash_mapped->write(path);
      
    } else if (__succinct_hash) {
      // we have only dynamic db... dump!
      __succinct_hash->write(path);
    } 
  }
  
};
