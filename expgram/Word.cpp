#include <utils/config.hpp>

#include "Word.hpp"

#include "Vocab.hpp"


namespace expgram
{
  Word::word_map_type& Word::__word_maps()
  {
#ifdef HAVE_TLS
    static __thread word_map_type* __maps_tls = 0;
    static boost::thread_specific_ptr<word_map_type> __maps;
    
    if (! __maps_tls) {
      __maps.reset(new word_map_type());
      __maps->reserve(allocated());
	
      __maps_tls = __maps.get();
    }
      
    return *__maps_tls;
#else
    static boost::thread_specific_ptr<word_map_type> __maps;
      
    if (! __maps.get()) {
      __maps.reset(new word_map_type());
      __maps->reserve(allocated());
    }
      
    return *__maps;
#endif
  }

  void Word::write(const path_type& path)
  {
    lock_type lock(__mutex_data);
    Vocab vocab(path, std::max(size_type(__words().size() / 2), size_type(1024)));
    word_set_type::const_iterator witer_end = __words().end();
    for (word_set_type::const_iterator witer = __words().begin(); witer != witer_end; ++ witer)
      vocab.insert(*witer);
  }
  
  Word::mutex_type    Word::__mutex_index;
  Word::mutex_type    Word::__mutex_data;
  
};
