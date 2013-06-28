// -*- mode: c++ -*-
//
//  Copyright(C) 2009-2013 Taro Watanabe <taro.watanabe@nict.go.jp>
//

#ifndef __EXPGRAM__NGRAM_STATE_CHART__HPP__
#define __EXPGRAM__NGRAM_STATE_CHART__HPP__ 1

//
// ngram state manager for chart
//

#include <algorithm>

#include <expgram/NGramIndex.hpp>
#include <expgram/NGramState.hpp>

namespace expgram
{
  struct NGramStateChart
  {
    typedef size_t             size_type;
    typedef ptrdiff_t          difference_type;
    
    typedef Word               word_type;
    typedef float              logprob_type;

    typedef NGramIndex::state_type state_type;
    typedef NGramState ngram_state_type;
    
    NGramStateChart(const size_type order) : ngram_state(order) {}
    
    size_type buffer_size() const
    {
      return offset_suffix() + ngram_state.buffer_size();
    }

    size_type offset_prefix() const
    {
      return 0;
    }

    size_type offset_suffix() const
    {
      return sizeof(size_type) * 2 + sizeof(state_type) * (ngram_state.order_ - 1);
    }
    
    size_type& length_suffix(void* buffer) const
    {
      return ngram_state.length((char*) buffer + offset_suffix());
    }
    
    const size_type& length_suffix(const void* buffer) const
    {
      return ngram_state.length((const char*) buffer + offset_suffix());
    }

    size_type& length_prefix(void* buffer) const
    {
      return *reinterpret_cast<size_type*>(buffer);
    }
    
    const size_type& length_prefix(const void* buffer) const
    {
      return *reinterpret_cast<const size_type*>(buffer);
    }
    
    size_type& complete(void* buffer) const
    {
      return *reinterpret_cast<size_type*>((char*) buffer + sizeof(size_type));
    }

    const size_type& complete(const void* buffer) const
    {
      return *reinterpret_cast<const size_type*>((const char*) buffer + sizeof(size_type));
    }

    state_type* state(void* buffer) const
    {
      return reinterpret_cast<state_type*>((char*) buffer + sizeof(size_type) * 2);
    }

    const state_type* state(const void* buffer) const
    {
      return reinterpret_cast<const state_type*>((const char*) buffer + sizeof(size_type) * 2);
    }
    
    word_type::id_type* context(void* buffer) const
    {
      return ngram_state.context((char*) buffer + offset_suffix());
    }
    
    const word_type::id_type* context(const void* buffer) const
    {
      return ngram_state.context((const char*) buffer + offset_suffix());
    }

    logprob_type* backoff(void* buffer) const
    {
      return ngram_state.backoff((char*) buffer + offset_suffix());
    }

    const logprob_type* backoff(const void* buffer) const
    {
      return ngram_state.backoff((const char*) buffer + offset_suffix());
    }
    
    
    ngram_state_type ngram_state;
  };

};

#endif
