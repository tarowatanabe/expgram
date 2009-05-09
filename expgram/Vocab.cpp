
#include "Word.hpp"
#include "Vocab.hpp"

#include "utils/filesystem.hpp"

namespace expgram
{
  // constants...
  const Vocab::word_type Vocab::EMPTY = Vocab::word_type("");
  const Vocab::word_type Vocab::NONE = Vocab::word_type("<none>");
  const Vocab::word_type Vocab::UNK  = Vocab::word_type("<unk>");
  const Vocab::word_type Vocab::BOS  = Vocab::word_type("<s>");
  const Vocab::word_type Vocab::EOS  = Vocab::word_type("</s>");
  
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
