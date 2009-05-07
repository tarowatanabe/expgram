
#include "Word.hpp"

#include "Vocab.hpp"


namespace expgram
{
  void Word::write(const path_type& path)
  {
    lock_type lock(__mutex);
    Vocab vocab(path, std::max(size_type(__words().size() / 2), size_type(1024)));
    word_set_type::const_iterator witer_end = __words().end();
    for (word_set_type::const_iterator witer = __words().begin(); witer != witer_end; ++ witer)
      vocab.insert(*witer);
  }
  
  Word::mutex_type    Word::__mutex;
  
};
