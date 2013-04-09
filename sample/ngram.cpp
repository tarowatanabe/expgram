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
#include <expgram/Word.hpp>
#include <expgram/Vocab.hpp>
#include <expgram/Sentence.hpp>

typedef boost::filesystem::path path_type;

typedef boost::tokenizer<utils::space_separator>               tokenizer_type;
typedef std::vector<std::string, std::allocator<std::string> > tokens_type;

typedef expgram::Vocab    vocab_type;
typedef expgram::Word     word_type;
typedef expgram::Sentence sentence_type;

typedef expgram::NGram::state_type state_type;

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
      state_type state = ngram.index.next(state_type(), vocab_type::BOS);
      
      // debug message...
      std::cerr << "bos state: shard: " <<  state.shard()
		<< " node: " << state.node()
		<< " is-root-shard: " << state.is_root_shard()
		<< " is-root-node: " << state.is_root_node()
		<< " order: " << ngram.index.order(state)
		<< std::endl;
      
      tokens_type::const_iterator titer_begin = tokens.begin();
      tokens_type::const_iterator titer_end = tokens.end();
      for (tokens_type::const_iterator titer = titer_begin + 1; titer != titer_end; ++ titer) {
	
	std::pair<state_type, float> result_logprob  = ngram.logprob(state, *titer);
	
	// some debug information...
	std::cerr << "state: shard: " <<  result_logprob.first.shard()
		  << " node: " << result_logprob.first.node()
		  << " is-root-shard: " << result_logprob.first.is_root_shard()
		  << " is-root-node: " << result_logprob.first.is_root_node()
		  << " logprob: " << result_logprob.second
		  << " base10: " << (result_logprob.second / log_10)
		  << " order: " << ngram.index.order(result_logprob.first)
		  << std::endl;
	
	// next state!
	state = result_logprob.first;
	
	// non-state version for scoring ngrams
	// ngram access must use container that supports random-iterator concepts.
	// If not sure, use vector!
	
	tokens_type::const_iterator titer_first = std::max(titer_begin, titer + 1 - order);
	tokens_type::const_iterator titer_last  = titer + 1;
	
	const float score_logprob  = ngram(titer_first, titer_last);
	
	std::cout << score_logprob << ' ';
	std::copy(titer_first, titer_last, std::ostream_iterator<std::string>(std::cout, " "));
	std::cout << std::endl;
	
	if (score_logprob != result_logprob.second)
	  std::cerr << "logprob differ: " << score_logprob << ' ' << result_logprob.second << std::endl;
	
	// we compute prefix for decoding with CKY-style bottom up algorithm.
	// The prefix is the left context which should be scored when "correct" history is available...
	std::pair<tokens_type::const_iterator, tokens_type::const_iterator> prefix = ngram.prefix(titer_first, titer_last);
	
	std::cout << "prefix: ";
	std::copy(prefix.first, prefix.second, std::ostream_iterator<std::string>(std::cout, " "));
	std::cout << std::endl;
	
	// An example for upper bound estimates, which is used to compute heursitic estimates.
	// Usually, we will score the ngram "prefix" computed by ngram.prefix(...)
	state_type state_ngram;
	
	for (tokens_type::const_iterator titer = titer_first; titer != titer_last; ++ titer) {
	  // state-based access...
	  const bool backoffed = ngram.index.order(state_ngram) != std::distance(titer_first, titer);
	  
	  const std::pair<state_type, float> result_logprob  = ngram.logprob(state_ngram, *titer, backoffed, order);
	  const std::pair<state_type, float> result_logbound = ngram.logbound(state_ngram, *titer, backoffed, order);
	  
	  // next state...
	  state_ngram = result_logprob.first;
	  
	  // non-state-based access...
	  const float score_logprob  = ngram.logprob(titer_first, titer + 1);
	  const float score_logbound = ngram.logbound(titer_first, titer + 1);
	  
	  std::cout << result_logprob.second
		    << ' ' << result_logbound.second
		    << '\t';
	  std::copy(titer_first, titer + 1, std::ostream_iterator<std::string>(std::cout, " "));
	  std::cout << std::endl;
	  
	  // they should be equal!
	  if (result_logprob.first != result_logbound.first)
	    std::cerr << "\tstate differ:" << std::endl
		      << "\tlogprob:  shard: " << result_logprob.first.shard() << " node: " << result_logprob.first.node() << std::endl
		      << "\tlogbound: shard: " << result_logbound.first.shard() << " node: " << result_logbound.first.node() << std::endl;
	  
	  if (score_logprob != result_logprob.second)
	    std::cerr << "\tlogprob differ: " << score_logprob << ' ' << result_logprob.second << std::endl;
	  if (score_logbound != result_logbound.second)
	    std::cerr << "\tlogbound differ: " << score_logbound << ' ' << result_logbound.second << std::endl;
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
