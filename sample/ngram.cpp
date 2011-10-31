
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
    
    while (std::getline(std::cin, line)) {
      tokenizer_type tokenizer(line);
      
      // here, we store in vector<string>.
      // alternatively you can try: (Remark: sentence_type is simply vector<word_type>)
      // 
      // sentence.clear();
      // sentence.push_back(vocab_type::BOS);
      // sentence.insert(sentence.end(), tokenizer.begin(), tokenizer.end());
      // sentence.push_back(vocab_type::EOS);
      //
      // and iterate over sentence type...
      //
      // or even more: (Remark: we assume id_set is vector<word_type::id_type> )
      //
      // id_set.clear();
      // id_set.push_back(ngram.index.vocab()[vocab_type::BOS]);
      // for (tokenizer_type::iterator titer = tokenizer.begin(); titer != tokenizer.end(); ++ titer)
      //   id_set.push_back(ngram.index.vocab()[*titer]);
      // id_set.push_back(ngram.index.vocab()[vocab_type::EOS]);
      //
      // then, you can iterate over id_set
      
      tokens.clear();
      tokens.push_back(vocab_type::BOS);
      tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
      tokens.push_back(vocab_type::EOS);

      state_type state = ngram.index.next(state_type(), vocab_type::BOS);

      std::cerr << "bos state: shard: " <<  state.shard()
		<< " node: " << state.node()
		<< " is-root-shard: " << state.is_root_shard()
		<< " is-root-node: " << state.is_root_node()
		<< " order: " << ngram.index.order(state)
		<< std::endl;
      
      tokens_type::const_iterator titer_begin = tokens.begin();
      tokens_type::const_iterator titer_end = tokens.end();
      for (tokens_type::const_iterator titer = titer_begin + 1; titer != titer_end; ++ titer) {
	
	// ngram access must use containser that supports random-iterator concepts.
	// If not sure, use vector!

	std::pair<state_type, float> result_logprob  = ngram.logprob(state, *titer);	
	
	std::cerr << "state: shard: " <<  result_logprob.first.shard()
		  << " node: " << result_logprob.first.node()
		  << " is-root-shard: " << result_logprob.first.is_root_shard()
		  << " is-root-node: " << result_logprob.first.is_root_node()
		  << " logprob: " << result_logprob.second
		  << " order: " << ngram.index.order(result_logprob.first)
		  << std::endl;
		
	tokens_type::const_iterator titer_first = std::max(titer_begin, titer + 1 - order);
	tokens_type::const_iterator titer_last  = titer + 1;
	
	const float score_logprob  = ngram(titer_first, titer_last);
	
	std::cout << score_logprob
		  << ' ' << (ngram.exists(titer_first, titer_last) ? "true" : "false");
	std::cout << ' ';
	std::copy(titer_first, titer_last, std::ostream_iterator<std::string>(std::cout, " "));
	std::cout << std::endl;
	
	if (score_logprob != result_logprob.second)
	  std::cerr << "logprob differ: " << score_logprob << ' ' << result_logprob.second << std::endl;
	
	std::pair<tokens_type::const_iterator, tokens_type::const_iterator> prefix_old = ngram.ngram_prefix(titer_first, titer_last + 1);
	std::pair<tokens_type::const_iterator, tokens_type::const_iterator> prefix_new = ngram.prefix(titer_first, titer_last + 1);

	if (prefix_old != prefix_new)
	  std::cerr << "prefix differ" << std::endl;

	
	state = result_logprob.first;
	
	{
	  state_type state;
	  
	  for (tokens_type::const_iterator titer = titer_first; titer != titer_last; ++ titer) {
	    const float score_logprob  = ngram.logprob(titer_first, titer + 1);
	    const float score_logbound = ngram.logbound(titer_first, titer + 1);
	    
	    const bool backoffed = ngram.index.order(state) != std::distance(titer_first, titer);
	    
	    const std::pair<state_type, float> result_logprob  = ngram.logprob(state, *titer, backoffed, order);
	    const std::pair<state_type, float> result_logbound = ngram.logbound(state, *titer, backoffed, order);
	    
	    std::cout << "\t";
	    std::copy(titer_first, titer + 1, std::ostream_iterator<std::string>(std::cout, " "));
	    std::cout << std::endl;
	    
	    std::cerr << "\tstate: shard: " <<  result_logprob.first.shard()
		      << " node: " << result_logprob.first.node()
		      << " is-root-shard: " << result_logprob.first.is_root_shard()
		      << " is-root-node: " << result_logprob.first.is_root_node()
		      << " logprob: " << result_logprob.second
		      << " order: " << ngram.index.order(result_logprob.first)
	      
		      << std::endl;

	    
	    if (result_logprob.first != result_logbound.first)
	      std::cerr << "\tstate differ:" << std::endl
			<< "\tlogprob:  shard: " << result_logprob.first.shard() << " node: " << result_logprob.first.node() << std::endl
			<< "\tlogbound: shard: " << result_logbound.first.shard() << " node: " << result_logbound.first.node() << std::endl;
	    
	    
	    if (score_logprob != result_logprob.second)
	      std::cerr << "\tlogprob differ: " << score_logprob << ' ' << result_logprob.second << std::endl;
	    if (score_logbound != result_logbound.second)
	      std::cerr << "\tlogbound differ: " << score_logbound << ' ' << result_logbound.second << std::endl;
	    
	    state = result_logprob.first;
	  }
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
