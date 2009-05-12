
#include "Stemmer.hpp"

#include "stemmer/include/libstemmer.h"

#include <vector>

namespace expgram
{
  
  Stemmer::Stemmer()
    : __algorithm(), __handle() {}
  
  Stemmer::Stemmer(const std::string& algorithm)
    : __algorithm(algorithm), __handle(sb_stemmer_new(algorithm.c_str(), 0)) {}
  
  Stemmer::Stemmer(const Stemmer& x)
    : __algorithm(x.__algorithm), __handle(sb_stemmer_new(x.__algorithm.c_str(), 0)) {}
  
  Stemmer::~Stemmer()
  { 
    if (__handle)
      sb_stemmer_delete((struct sb_stemmer*) __handle);
  }
  
  void Stemmer::assign(const Stemmer& x)
  {
    if (__handle)
      sb_stemmer_delete((struct sb_stemmer*) __handle);
    
    __handle = 0;
    if (x.__handle)
      __handle = sb_stemmer_new(x.__algorithm.c_str(), 0);
    
    __algorithm = x.__algorithm;
  }

  inline
  bool __is_tag(const std::string& word)
  {
    return (word.empty() || (word[0] == '<' && word[word.size() - 1] == '>'));
  }
  
  std::string Stemmer::operator()(const std::string& word) const
  {
    if (! __handle || __is_tag(word))
      return word;
    
    std::vector<sb_symbol, std::allocator<sb_symbol> > buffer(word.begin(), word.end());
    
    const sb_symbol* stemmed = sb_stemmer_stem((struct sb_stemmer*) __handle, (const sb_symbol*) &(*buffer.begin()), buffer.size());
    
    return std::string((const char*) stemmed);
  }

};
