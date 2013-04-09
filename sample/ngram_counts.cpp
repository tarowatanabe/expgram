//
//  Copyright(C) 2009-2013 Taro Watanabe <taro.watanabe@nict.go.jp>
//

#include <iostream>
#include <vector>
#include <string>
#include <iterator>
#include <algorithm>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <boost/tokenizer.hpp>

#include <utils/space_separator.hpp>

#include <expgram/NGramCounts.hpp>
#include <expgram/Vocab.hpp>

typedef boost::filesystem::path path_type;

typedef boost::tokenizer<utils::space_separator>               tokenizer_type;
typedef std::vector<std::string, std::allocator<std::string> > tokens_type;

typedef expgram::Vocab vocab_type;


path_type ngram_file = "-";

int shards = 4;

int debug = 0;

int getoptions(int argc, char** argv);

int main(int argc, char** argv)
{
  try {
    if (getoptions(argc, argv) != 0) 
      return 1;
    
    expgram::NGramCounts ngram(ngram_file, shards, debug);
    
    std::string  line;
    tokens_type  tokens;

    const int order = ngram.index.order();
    
    while (std::getline(std::cin, line)) {
      tokenizer_type tokenizer(line);
      
      // Here, we store in vector<string>.
      
      tokens.clear();
      tokens.insert(tokens.end(), tokenizer.begin(), tokenizer.end());
      
      
      //
      // Alternatively, you can try: (Remark: sentence is simply vector<word_type>)
      // 
      // sentence.clear();
      // sentence.insert(sentence.end(), tokenizer.begin(), tokenizer.end());
      //
      // and iterate over sentence type, not tokens.
      //
      
      //
      // The above example still automatically convert word_type into word_type::id_type on the fly.
      // An alternative faster approach is: (Remark: we assume id_set is vector<word_type::id_type> )
      //
      // id_set.clear();
      // for (tokenizer_type::iterator titer = tokenizer.begin(); titer != tokenizer.end(); ++ titer)
      //   id_set.push_back(ngram.index.vocab()[*titer]);
      //
      // then, you can iterate over id_set, not tokens.
      //
      
      // 
      // Note that the word_type will automatically assign id which may not
      // match with the word-id assigned by the indexed ngram language model.
      // This means that even OOV by the ngram language model may be assigned word-id.
      // If you want to avoid this, here is a solution:
      //
      // const word_type::id_type unk_id = ngram.index.vocab()[vocab_type::UNK]
      //
      // id_set.clear();
      // for (tokenizer_type::iterator titer = tokenizer.begin(); titer != tokenizer.end(); ++ titer)
      //   id_set.push_back(ngram.index.vocab().exists(*titer) ? ngram.index.vocab()[*titer] : unk_id);
      //

      // ngram access must use containser that supports forward-iterator concepts.
      // If not sure, use vector!

      std::copy(tokens.begin(), tokens.end(), std::ostream_iterator<std::string>(std::cout, " "));
      std::cout << ngram(tokens.begin(), tokens.end()) << std::endl;
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
    ("ngram",  po::value<path_type>(&ngram_file)->default_value(ngram_file),   "ngram counts in Google format or indexed binary")
        
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
