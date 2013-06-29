//
//  Copyright(C) 2009-2013 Taro Watanabe <taro.watanabe@nict.go.jp>
//

#include <cmath>
#include <iostream>
#include <vector>
#include <string>
#include <iterator>
#include <algorithm>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <boost/tokenizer.hpp>

#include <utils/space_separator.hpp>
#include <utils/program_options.hpp>

#include <expgram/NGram.hpp>
#include <expgram/NGramState.hpp>
#include <expgram/Word.hpp>
#include <expgram/Vocab.hpp>
#include <expgram/Sentence.hpp>

typedef boost::filesystem::path path_type;

typedef boost::tokenizer<utils::space_separator>               tokenizer_type;
typedef std::vector<std::string, std::allocator<std::string> > tokens_type;

typedef expgram::Vocab    vocab_type;
typedef expgram::Word     word_type;
typedef expgram::Sentence sentence_type;

typedef expgram::NGramState ngram_state_type;

path_type ngram_file = "-";

bool quantize = false;
int shards = 4;

int debug = 0;

int getoptions(int argc, char** argv);

int main(int argc, char** argv)
{
  try {
    if (getoptions(argc, argv) != 0) 
      return 1;
    
    expgram::NGram ngram(ngram_file, shards, debug);
    if (quantize)
      ngram.quantize();
    
    std::string   line;
    tokens_type   tokens;
    sentence_type sentence;

    const int order = ngram.index.order();
    
    ngram_state_type ngram_state(order);
    
    // We need to allocate buffer for state representation!
    // Here, buffer_size() is the # of bytes required for state representation,
    // and we use std::vector to allocate buffer (you can use new char[ngram_state.buffer_size()])
    typedef std::vector<char, std::allocator<char> > buffer_type;
    
    buffer_type buffer(ngram_state.buffer_size());
    buffer_type buffer_bos(ngram_state.buffer_size());
    buffer_type buffer_next(ngram_state.buffer_size());
    
    void* state      = &(*buffer.begin());
    void* state_bos  = &(*buffer_bos.begin());
    void* state_next = &(*buffer_next.begin());
    
    // for adnaved scoring... see something wild in the loop below
    buffer_type buffer_curr(ngram_state.buffer_size());
    buffer_type buffer_suffix(ngram_state.buffer_size());
    buffer_type buffer_suffix_next(ngram_state.buffer_size());
    
    void* state_curr  = &(*buffer_curr.begin());
    void* suffix      = &(*buffer_suffix.begin());
    void* suffix_next = &(*buffer_suffix_next.begin());

    // fill-in BOS state
    ngram.lookup_context(&vocab_type::BOS, (&vocab_type::BOS) + 1, state_bos);

    const double log_10 = M_LN10;
    
    while (std::getline(std::cin, line)) {
      tokenizer_type tokenizer(line);
      
      // Here, we store in vector<string>.
      
      tokens.clear();
      tokens.push_back(vocab_type::BOS);
      tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
     tokens.push_back(vocab_type::EOS);

      //
      // Alternatively, you can try: (Remark: sentence is simply vector<word_type>)
      // 
      // sentence.clear();
      // sentence.push_back(vocab_type::BOS);
      // sentence.insert(sentence.end(), tokenizer.begin(), tokenizer.end());
      // sentence.push_back(vocab_type::EOS);
      //
      // and iterate over sentence type, not tokens.
      //
      
      //
      // The above example still automatically convert word_type into word_type::id_type on the fly.
      // An alternative faster approach is: (Remark: we assume id_set is vector<word_type::id_type> )
      //
      // const word_type::id_type bos_id = ngram.index.vocab()[vocab_type::BOS]
      // const word_type::id_type eos_id = ngram.index.vocab()[vocab_type::EOS]
      //
      // id_set.clear();
      // id_set.push_back(bos_id);
      // for (tokenizer_type::iterator titer = tokenizer.begin(); titer != tokenizer.end(); ++ titer)
      //   id_set.push_back(ngram.index.vocab()[*titer]);
      // id_set.push_back(eos_id);
      //
      // then, you can iterate over id_set, not tokens.
      // 
      
      // 
      // Note that the word_type will automatically assign id which may not
      // match with the word-id assigned by the indexed ngram language model.
      // This means that even OOV by the ngram language model may be assigned word-id.
      // If you want to avoid this, here is a solution:
      //
      // const word_type::id_type bos_id = ngram.index.vocab()[vocab_type::BOS]
      // const word_type::id_type eos_id = ngram.index.vocab()[vocab_type::EOS]
      // const word_type::id_type unk_id = ngram.index.vocab()[vocab_type::UNK]
      //
      // id_set.clear();
      // id_set.push_back(bos_id);
      // for (tokenizer_type::iterator titer = tokenizer.begin(); titer != tokenizer.end(); ++ titer)
      //   id_set.push_back(ngram.index.vocab().exists(*titer) ? ngram.index.vocab()[*titer] : unk_id);
      // id_set.push_back(eos_id);
      //
      
      // A state-based example...
      // Since will not score P(BOS), state should starts from BOS!

      // copy bos-state to state
      ngram_state.copy(state_bos, state);
            
      tokens_type::const_iterator titer_begin = tokens.begin();
      tokens_type::const_iterator titer_end = tokens.end();
      for (tokens_type::const_iterator titer = titer_begin + 1; titer != titer_end; ++ titer) {
	
	const expgram::NGram::result_type result = ngram.ngram_score(state, *titer, state_next);
	
	// some debug information...
	std::cerr << "state: shard: " <<  result.state.shard()
		  << " node: " << result.state.node()
		  << " logprob: " << result.prob
		  << " base10: " << (result.prob / log_10)
		  << " logbound: " << result.bound
		  << " base10: " << (result.bound / log_10)
		  << " length: " << result.length
		  << " completed: " << (result.complete ? "true" : "false")
		  << std::endl;
	
	// next state!
	std::swap(state, state_next);
	
	// A slow, non-state version
        tokens_type::const_iterator titer_first = std::max(titer_begin, titer + 1 - order);
        tokens_type::const_iterator titer_last  = titer + 1;
	
	const double logprob = ngram.logprob(titer_first, titer_last);
	
	std::cerr << " non-state: ";
	std::copy(titer_first, titer_last, std::ostream_iterator<std::string>(std::cerr, " "));
	std::cerr << "logprob: " << logprob
		  << " base10: " << (logprob / log_10)
		  << std::endl;
	
	// we will do something more wild here....
	
	// starting from titer_last, untile titer_last + order - 1, perform scoring without correct-state.
	// then, combine with the suffix state in the "state"

	// initialize suffix state and current state
	ngram_state.copy(state, suffix);
	ngram_state.length(state_curr) = 0;
	
	tokens_type::const_iterator niter_end = std::min(titer_last + order - 1, titer_end);
	for (tokens_type::const_iterator niter = titer_last; niter != niter_end; ++ niter) {
	  // this is a special case, and we do not treat in this example...
	  if (ngram_state.length(suffix) == 0) break;
	  
	  const expgram::NGram::result_type result = ngram.ngram_score(state_curr, *niter, state_next);
	  
	  // this is a complete result, so we will stop this demonstration...
	  if (result.complete) break;
	  
	  const int order = (niter - titer_last) + 1;

	  //
	  // The difference of this ordering comes from the backoff in the previous ngram_score call...
	  //
	  std::cerr << '\t' << "ngram order: " << order << " state order: " << ngram.index.order(result.state) << std::endl;
	  
	  std::cerr << '\t' << "state: curr: " << ngram_state.length(state_curr)
		    << " next: " << ngram_state.length(state_next) << std::endl;

	  const expgram::NGram::result_type result_partial = ngram.ngram_partial_score(suffix,
										       result.state,
										       order,
										       suffix_next);

	  std::cerr << '\t' << "suffix: curr: " << ngram_state.length(suffix)
		    << " next: " << ngram_state.length(suffix_next) << std::endl;
	  
	  std::cerr << '\t' << "partial ngram length: " << result_partial.length
		    << " rescore length: " << result.length
		    << std::endl;
	  
	  double logprob_partial = result.bound + result_partial.prob;
	  	  
	  std::cerr << '\t' << "ngram: ";
	  std::copy(titer_last, niter + 1, std::ostream_iterator<std::string>(std::cerr, " "));
	  std::cerr << std::endl;
	  std::cerr << '\t' << "partial " << "logprob: " << logprob_partial
		    << " base10: " << (logprob_partial / log_10)
		    << " complete: " << (result_partial.complete ? "true" : "false")
		    << std::endl;
	  
	  const double logprob = ngram.logprob(titer_first, niter + 1);
	  std::cerr << '\t' << "   full " << "logprob: " << logprob
		    << " base10: " << (logprob / log_10)
		    << std::endl;

	  // next state...
	  std::swap(state_curr, state_next);
	  std::swap(suffix, suffix_next);
	}
      }
    }
  }
  catch (std::exception& err) {
    std::cerr << "error: " << err.what() << std::endl;
    return 1;
  }
  return 0;
}

int getoptions(int argc, char** argv)
{
  namespace po = boost::program_options;
  
  po::options_description desc("options");
  desc.add_options()
    ("ngram",  po::value<path_type>(&ngram_file)->default_value(ngram_file),   "ngram in ARPA or binary format")
    
    ("quantize", po::bool_switch(&quantize), "perform quantization")
    
    ("shard",  po::value<int>(&shards)->default_value(shards),                 "# of shards (or # of threads)")
    
    ("debug", po::value<int>(&debug)->implicit_value(1), "debug level")
    ("help", "help message");
  
  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc, po::command_line_style::unix_style & (~po::command_line_style::allow_guessing)), vm);
  po::notify(vm);
  
  if (vm.count("help")) {
    std::cout << argv[0] << " [options]" << '\n' << desc << '\n';
    return 1;
  }
  
  return 0;
}
