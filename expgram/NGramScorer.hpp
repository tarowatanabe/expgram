// -*- mode: c++ -*-
//
//  Copyright(C) 2013 Taro Watanabe <taro.watanabe@nict.go.jp>
//

#ifndef __EXPGRAM__NGRAM_SCORER__HPP__
#define __EXPGRAM__NGRAM_SCORER__HPP__ 1

#include <expgram/Word.hpp>
#include <expgram/Vocab.hpp>

#include <expgram/NGram.hpp>
#include <expgram/NGramState.hpp>
#include <expgram/NGramStateChart.hpp>

namespace expgram
{

  struct NGramScorer
  {
    typedef size_t             size_type;
    typedef ptrdiff_t          difference_type;
    
    typedef Word               word_type;
    typedef Vocab              vocab_type;
    typedef float              logprob_type;

    typedef NGram           ngram_type;
    typedef NGramStateChart ngram_state_type;

    typedef std::vector<char, std::allocator<char> > buffer_type;
    
    NGramScore(const ngram_type& ngram, void* state)
      : ngram_state_(ngram.index.order()), ngram_(ngram), state_(state), prob_(0.0), complete_(false)
    {
      ngram_state_.length_prefix(state_) = 0;
      ngram_state_.length_suffix(state_) = 0;
      ngram_state_.complete(state_) = false;

      buffer1_.reserve(ngram_state_.suffix_state_.buffer_size());
      buffer2_.reserve(ngram_state_.suffix_state_.buffer_size());
      
      buffer1_.resize(ngram_state_.suffix_state_.buffer_size());
      buffer2_.resize(ngram_state_.suffix_state_.buffer_size());
    }
    
    void initial_bos()
    {
      const word_type::id_type bos_id = ngram_.index.vocab()[vocab_type::BOS];
      
      ngram_.lookup_context(&bos_id, (&bos_id) + 1, ngram_state_.suffix(state_));
      
      complete_ = true;
    }

    void initial_bos(const void* buffer)
    {
      // copy suffix state...
      ngram_state_.suffix_state_.copy(buffer, ngram_state_.suffix(state_));
      complete_ = true;
    }
    
    template <typename Word_>
    void terminal(const Word_& word)
    {
      void* state_prev = &(*buffer1.begin());
      
      ngram_state_.suffix_state_.copy(nram_state_.suffix(state_), state_prev);
      
      const ngram_type::result_type result = ngram_.ngram_score(state_prev, word, nram_state_.suffix(state_));
      
      if (complete_ || result.complete) {
	prob_ += result.prob;
	
	complete_ = true;
      } else {
	prob_ += result.bound;
	
	ngram_state_.state(state_)[ngram_state_.length_prefix(state_)] = result.state;
	++ ngram_state_.length_prefix(state_);
	
	// if not incremental, complete it.
	if (ngram_state_.length_suffix(state_) != ngram_state_.suffix_state_.length(state_prev) + 1)
	  complete_ = true;
      }
    }

    // special handling for left-most non-terminal
    void initial_non_terminal(const void* antecedent)
    {
      ngram_state_.copy(antecedent, state_);
      
      complete_ = ngram_state_.complete(antecedent);
    }
    
    void non_terminal(const void* antecedent)
    {
      // antecedent has no pending scoring...
      if (ngram_state_.length_prefix(antecedent) == 0) {
	
	// if this antecedent is complete, we will copy suffix from antecedent to state_.
	// then, all the pending backoff in current will be scored...!
	if (ngram_state_.complete(antecedent)) {
	  // score all the backoff
	  const logprob_type* biter     = ngram_state_.backoff(state_);
	  const logprob_type* biter_end = biter + ngram_state_.length_suffix(state_);
	  for (/**/; biter != biter_end; ++ biter)
	    prob_ += *biter;
	  
	  // copy suffix state
	  ngram_state_.copy_suffix(antecedent, state_);
	  
	  // this is a complete scoring
	  complete_ = true;
	}
	
	return;
      }
      
      // we have no context as a suffix
      if (ngram_state_.length_suffix(state_) == 0) {
	// copy suffix state from antecedent
	ngram_state_.copy_suffix(antecedent, state_);
	
	if (complete_) {
	  // adjust all the bound scoring
	  
	} else if (ngram_state_.length_prefix(state_) == 0) {
	  ngram_state_.copy_prefix(antecedent, state_);
	  
	  complete_ = ngram_state_.complete(antecedent);
	} else
	  complete_ = true;
	
	return;
      }
      
      // temporary storage...
      void* state_curr = &(*buffer1_.begin());
      void* state_next = &(*buffer2_.begin());

      const ngram_type::state_type* states = ngram_state_.state(antecedent);
      const size_type               states_length = ngram_state_.length_prefix(antecedent);
      
      // first, try the first state
      {
	const ngram_type::result_type result = ngram_.ngram_partial_score(ngram_state_.suffix(state_), states[0], 1, state_curr);
	
	if (complete_ || result.complete) {
	  prob_ += result.prob;
	  
	  complete_ = true;
	} else {
	  prob_ += result.bound;
	  
	  ngram_state.state(state_)[ngram_state_.length_prefix(state_)] = result.state;
	  ++ ngram_state_.length_prefix(state_);
	}
	
	if (ngram_state_.length_suffix(state_curr) != ngram_state_.length_suffix(state_)) {
	  complete_ = true;
	  
	  // we have finished all the scoring... and we do not have to score for the rest of the states
	  if (ngram_state_.length_suffix(state_curr) == 0) {
	    ngram_state_.copy_suffix(antecedent, state_);
	    
	    // adjust rest-cost from antecedent.pointers + 1, antecedent.pointers + length!
	    
	    return;
	  }
	}
      }

      // second, try the second and the up...
      for (size_type order = 2; order <= states_length; ++ order) {
	const ngram_type::result_type result = ngram_.ngram_partial_score(state, states[order - 1], order, state_next);
	
	if (complete_ || result.complete) {
	  prob_ += result.prob;
	  
	  complete_ = true;
	} else {
	  prob_ += result.bound;
	  
	  ngram_state.state(state_)[ngram_state_.length_prefix(state_)] = result.state;
	  ++ ngram_state_.length_prefix(state_);
	}

	// do swap!
	std::swap(state_curr, state_next);
	
	if (ngram_state_.length_suffix(state_curr) != ngram_state_.length_suffix(state_)) {
	  complete_ = true;
	  
	  // we have finished all the scoring... and we do not have to score for the rest of the states
	  if (ngram_state_.length_suffix(state_curr) == 0) {
	    ngram_state_.copy_suffix(antecedent, state_);
	    
	    // adjust rest-cost from antecedent.pointers + order, antecedent.pointers + length!
	    
	    return;
	  }
	}
      }
      
      // antecedent is a complete state
      if (ngram_state_.complete(antecedent)) {
	// score all the backoff..
	const logprob_type* biter     = ngram_state_.backoff(state_curr);
	const logprob_type* biter_end = biter + ngram_state_.length_suffix(state_curr);
	for (/**/; biter < biter_end; ++ biter)
	  prob_ += *biter;
	
	// copy suffix state
	ngram_state_.copy_suffix(antecedent, state_);
	
	// this is a complete scoring
	complete_ = true;
	
	return;
      }
      
      // minimum suffix: thus, independent of the words to the left...
      if (ngram_state_.length_suffx(antecedent) < ngram_state_.length_prefix(antecedent)) {
	ngram_state_.copy_suffix(antecedent, state_);
	return;
      }
      
      // copy and merge states...
      std::copy(ngram_state_.context(antecedent),
		ngram_state_.context(antecedent) + ngram_state_.length_suffix(antecedent),
		ngram_state_.context(state_));
      std::copy(ngram_state_.suffix_state.context(state_curr),
		ngram_state_.suffix_state.context(state_curr) + ngram_state_.suffix_state.length(state_curr),
		ngram_state_.context(state_) + ngram_state_.length_suffix(antecedent))
      
      std::copy(ngram_state_.backoff(antecedent),
		ngram_state_.backoff(antecedent) + ngram_state_.length_suffix(antecedent),
		ngram_state_.backoff(state_));
      std::copy(ngram_state_.suffix_state.backoff(state_curr),
		ngram_state_.suffix_state.backoff(state_curr) + ngram_state_.suffix_state.length(state_curr),
		ngram_state_.backoff(state_) + ngram_state_.length_suffix(antecedent))
    }
    
    ngram_state_type  ngram_state_;
    
    const ngram_type& ngram_;
    void*             state_;
    double            prob_;
    bool              complete_;
    
    buffer_type buffer1_;
    buffer_type buffer2_;
  };
};

#endif
