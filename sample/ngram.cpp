
#include <iostream>
#include <vector>
#include <string>
#include <iterator>
#include <algorithm>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <boost/tokenizer.hpp>

#include <utils/space_separator.hpp>

#include <expgram/NGram.hpp>
#include <expgram/Vocab.hpp>

typedef boost::filesystem::path path_type;

typedef boost::tokenizer<utils::space_separator>               tokenizer_type;
typedef std::vector<std::string, std::allocator<std::string> > tokens_type;

typedef expgram::Vocab vocab_type;


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
    
    std::string  line;
    tokens_type  tokens;

    const int order = ngram.index.order();
    
    while (std::getline(std::cin, line)) {
      tokenizer_type tokenizer(line);
      tokens.clear();
      tokens.push_back(vocab_type::BOS);
      tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
      tokens.push_back(vocab_type::EOS);

      tokens_type::const_iterator titer_begin = tokens.begin();
      tokens_type::const_iterator titer_end = tokens.end();
      for (tokens_type::const_iterator titer = titer_begin + 1; titer != titer_end; ++ titer) {
	
	// ngram access must use containser that supports forward-iterator concepts.
	// If not sure, use vector!

	tokens_type::const_iterator titer_first = std::max(titer + 1 - order, titer_begin);
	std::copy(titer_first, titer + 1, std::ostream_iterator<std::string>(std::cout, " "));
	std::cout << ngram(titer_first, titer + 1) << std::endl;
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
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);
  
  if (vm.count("help")) {
    std::cout << argv[0] << " [options]" << '\n' << desc << '\n';
    return 1;
  }
  
  return 0;
}
