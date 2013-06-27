//
//  Copyright(C) 2009-2013 Taro Watanabe <taro.watanabe@nict.go.jp>
//

#define BOOST_SPIRIT_THREADSAFE

#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/karma.hpp>

#include <iostream>
#include <iterator>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

#include <utils/compress_stream.hpp>
#include <utils/program_options.hpp>
#include <utils/mathop.hpp>
#include <utils/resource.hpp>
#include <utils/hashmurmur3.hpp>
#include <utils/bithack.hpp>

#include <expgram/NGram.hpp>
#include <expgram/NGramState.hpp>
#include <expgram/Sentence.hpp>
#include <expgram/Vocab.hpp>

typedef boost::filesystem::path path_type;

typedef expgram::NGram    ngram_type;
typedef expgram::Word     word_type;
typedef expgram::Vocab    vocab_type;
typedef expgram::Sentence sentence_type;
typedef expgram::NGramState ngram_state_type;

path_type ngram_file;
path_type input_file = "-";
path_type output_file = "-";

int shards = 4;
int verbose = 0;
int debug = 0;

int getoptions(int argc, char** argv);

int main(int argc, char** argv)
{
  try {
    if (getoptions(argc, argv) != 0) 
      return 1;

    namespace karma = boost::spirit::karma;
    namespace standard = boost::spirit::standard;
        
    ngram_type ngram(ngram_file, shards, debug);
    
    sentence_type sentence;
    
    const bool flush_output = (output_file == "-" || (boost::filesystem::exists(output_file) && ! boost::filesystem::is_regular_file(output_file)));
    
    utils::compress_istream is(input_file);
    utils::compress_ostream os(output_file, 1024 * 1024 * (! flush_output));
    
    const int order = ngram.index.order();
    
    const word_type::id_type bos_id = ngram.index.vocab()[vocab_type::BOS];
    const word_type::id_type eos_id = ngram.index.vocab()[vocab_type::EOS];
    const word_type::id_type unk_id = ngram.index.vocab()[vocab_type::UNK];
    const word_type::id_type none_id = word_type::id_type(-1);

    typedef std::vector<char, std::allocator<char> > buffer_type;
    
    ngram_state_type ngram_state(order);
    
    // buffer!
    buffer_type __buffer(ngram_state.buffer_size());
    buffer_type __buffer_bos(ngram_state.buffer_size());
    buffer_type __buffer_next(ngram_state.buffer_size());

    void* buffer      = &(*__buffer.begin());
    void* buffer_bos  = &(*__buffer_bos.begin());
    void* buffer_next = &(*__buffer_next.begin());
    
    ngram.lookup_context(&bos_id, (&bos_id) + 1, buffer_bos);
    
    size_t num_word = 0;
    size_t num_sentence = 0;

    utils::resource start;
    
    while (is >> sentence) {
      // add BOS and EOS
      
      if (sentence.empty()) continue;
      
      int oov = 0;
      double logprob = 0.0;
      
      ngram_state.copy(buffer_bos, buffer);
      
      sentence_type::const_iterator siter_end = sentence.end();
      for (sentence_type::const_iterator siter = sentence.begin(); siter != siter_end; ++ siter) {
	const word_type::id_type id = ngram.index.vocab()[*siter];

	const ngram_type::result_type result = ngram.ngram_score(buffer, id, buffer_next);
	
	if (verbose)
	  if (! karma::generate(std::ostream_iterator<char>(os),
				standard::string << '=' << karma::uint_ << ' ' << karma::int_ << ' ' << karma::double_ << '\n',
				*siter, id, result.length, result.prob))
	    throw std::runtime_error("generation failed");

	oov += (id == unk_id) || (id == none_id);
	
	logprob += result.prob;
	std::swap(buffer, buffer_next);
      }
      
      const ngram_type::result_type result = ngram.ngram_score(buffer, eos_id, buffer_next);
      
      if (verbose)
	if (! karma::generate(std::ostream_iterator<char>(os),
			      standard::string << '=' << karma::uint_ << ' ' << karma::int_ << ' ' << karma::double_ << '\n',
			      vocab_type::EOS, eos_id, result.length, result.prob))
	  throw std::runtime_error("generation failed");
      
      logprob += result.prob;
      
      if (! karma::generate(std::ostream_iterator<char>(os),
			    karma::double_ << ' ' << karma::int_ << '\n',
			    logprob, oov))
	throw std::runtime_error("generation failed");

      ++ num_sentence;
      num_word += sentence.size();
    }
    
    utils::resource end;
    
    if (debug)
      std::cerr << "queries: " << (num_word + num_sentence) << std::endl
		<< "cpu:    " << 1e-3 * (num_word + num_sentence) / (end.cpu_time() - start.cpu_time()) << " queries/ms" << std::endl
		<< "user:   " << 1e-3 * (num_word + num_sentence) / (end.user_time() - start.user_time()) << " queries/ms" << std::endl
		<< "thread: " << 1e-3 * (num_word + num_sentence) / (end.thread_time() - start.thread_time()) << " queries/ms" << std::endl;
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
    ("ngram",  po::value<path_type>(&ngram_file)->default_value(ngram_file),   "ngram in ARPA or expgram format")
    ("input",  po::value<path_type>(&input_file)->default_value(input_file),   "input")
    ("output", po::value<path_type>(&output_file)->default_value(output_file), "output")
    
    ("shard",   po::value<int>(&shards)->default_value(shards),                 "# of shards (or # of threads)")
    ("verbose", po::value<int>(&verbose)->implicit_value(1), "verbose level")
    ("debug",   po::value<int>(&debug)->implicit_value(1),   "debug level")
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
