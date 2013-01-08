//
//  Copyright(C) 2009-2012 Taro Watanabe <taro.watanabe@nict.go.jp>
//

#include <iostream>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

#include <utils/compress_stream.hpp>
#include <utils/program_options.hpp>
#include <utils/mathop.hpp>

#include <expgram/NGram.hpp>
#include <expgram/Sentence.hpp>
#include <expgram/Vocab.hpp>

typedef boost::filesystem::path path_type;

path_type ngram_file;
path_type input_file = "-";
path_type output_file = "-";
path_type temporary_dir = "";

int order = 0;

int shards = 4;
int debug = 0;

int getoptions(int argc, char** argv);

int main(int argc, char** argv)
{
  try {
    if (getoptions(argc, argv) != 0) 
      return 1;

    if (! temporary_dir.empty())
      ::setenv("TMPDIR_SPEC", temporary_dir.string().data(), 1);

    typedef expgram::NGram    ngram_type;
    typedef expgram::Word     word_type;
    typedef expgram::Vocab    vocab_type;
    typedef expgram::Sentence sentence_type;

    typedef ngram_type::state_type state_type;

    ngram_type ngram(ngram_file, shards, debug);
    
    double logprob_total = 0.0;
    double logprob_total_oov = 0.0;
    size_t num_word = 0;
    size_t num_oov = 0;
    size_t num_sentence = 0;
    
    sentence_type sentence;
    
    utils::compress_istream is(input_file);

    order = (order <= 0 ? ngram.index.order() : order);

    const word_type::id_type bos_id = ngram.index.vocab()[vocab_type::BOS];
    const word_type::id_type eos_id = ngram.index.vocab()[vocab_type::EOS];
    const word_type::id_type unk_id = ngram.index.vocab()[vocab_type::UNK];
    const word_type::id_type none_id = word_type::id_type(-1);
    
    while (is >> sentence) {
      // add BOS and EOS
      
      if (sentence.empty()) continue;
      
      state_type state = ngram.index.next(state_type(), bos_id);
      
      sentence_type::const_iterator siter_end = sentence.end();
      for (sentence_type::const_iterator siter = sentence.begin(); siter != siter_end; ++ siter) {
	const word_type::id_type word_id = ngram.index.vocab()[*siter];
	const bool is_oov = (word_id == unk_id) || (word_id == none_id);
	
	const std::pair<state_type, float> result = ngram.logprob(state, word_id);
	
	state = result.first;
	
	if (! is_oov)
	  logprob_total += result.second;
	logprob_total_oov += result.second;
	
	num_oov += is_oov;
      }
      
      const float logprob = ngram.logprob(state, eos_id).second;
      
      logprob_total     += logprob;
      logprob_total_oov += logprob;
      
      num_word += sentence.size();
      ++ num_sentence;
    }
    
    utils::compress_ostream os(output_file);
    os << "# of sentences: " << num_sentence
       << " # of words: " << num_word
       << " # of OOV: " << num_oov
       << " order: " << order
       << std::endl;
    
    os << "logprob = " << logprob_total << " base10 = " << (logprob_total / M_LN10) << std::endl;
    os << "ppl     = " << utils::mathop::exp(- logprob_total / (num_word - num_oov + num_sentence)) << std::endl;
    os << "ppl1    = " << utils::mathop::exp(- logprob_total / (num_word - num_oov)) << std::endl;
    os << "logprob(+oov) = " << logprob_total_oov << " base10 = " << (logprob_total_oov / M_LN10) << std::endl;
    os << "ppl(+oov)     = " << utils::mathop::exp(- logprob_total_oov / (num_word + num_sentence)) << std::endl;
    os << "ppl1(+oov)    = " << utils::mathop::exp(- logprob_total_oov / (num_word)) << std::endl;
    
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
    ("ngram",     po::value<path_type>(&ngram_file)->default_value(ngram_file),   "ngram in ARPA or binary format")
    ("input",     po::value<path_type>(&input_file)->default_value(input_file),   "input")
    ("output",    po::value<path_type>(&output_file)->default_value(output_file), "output")
    ("temporary", po::value<path_type>(&temporary_dir),                           "temporary directory")    
    
    ("order",       po::value<int>(&order)->default_value(order),              "ngram order")
    
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
