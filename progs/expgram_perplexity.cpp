//
//  Copyright(C) 2009-2013 Taro Watanabe <taro.watanabe@nict.go.jp>
//

#define BOOST_DISABLE_ASSERTS
#define BOOST_SPIRIT_THREADSAFE
#define PHOENIX_THREADSAFE

#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/karma.hpp>

#include <iostream>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

#include <utils/compress_stream.hpp>
#include <utils/program_options.hpp>
#include <utils/resource.hpp>
#include <utils/mathop.hpp>

#include <expgram/NGram.hpp>
#include <expgram/Sentence.hpp>
#include <expgram/Vocab.hpp>

typedef boost::filesystem::path path_type;

path_type ngram_file;
path_type input_file = "-";
path_type output_file = "-";
path_type temporary_dir = "";

int shards = 4;
bool populate = false;
bool precompute = false;
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
    typedef expgram::NGramState ngram_state_type;

    typedef word_type::id_type id_type;
    typedef std::vector<id_type, std::allocator<id_type> > id_set_type;

    ngram_type ngram(ngram_file, shards, debug);

    if (populate)
      ngram.populate();
    
    double logprob_total = 0.0;
    double logprob_total_oov = 0.0;
    size_t num_word = 0;
    size_t num_oov = 0;
    size_t num_sentence = 0;
    
    sentence_type sentence;
    id_set_type   ids;
    
    utils::compress_istream is(input_file);

    const int order = ngram.index.order();

    const word_type::id_type bos_id = ngram.index.vocab()[vocab_type::BOS];
    const word_type::id_type eos_id = ngram.index.vocab()[vocab_type::EOS];
    const word_type::id_type unk_id = ngram.index.vocab()[vocab_type::UNK];
    const word_type::id_type none_id = word_type::id_type(-1);

    typedef std::vector<char, std::allocator<char> > buffer_type;
    
    ngram_state_type ngram_state(order);
    
    // buffer!
    buffer_type buffer(ngram_state.buffer_size());
    buffer_type buffer_bos(ngram_state.buffer_size());
    buffer_type buffer_next(ngram_state.buffer_size());

    void* state      = &(*buffer.begin());
    void* state_bos  = &(*buffer_bos.begin());
    void* state_next = &(*buffer_next.begin());
    
    ngram.lookup_context(&bos_id, (&bos_id) + 1, state_bos);
    
    utils::resource start;
    
    if (precompute) {
      namespace qi = boost::spirit::qi;
      namespace standard = boost::spirit::standard;
      
      typedef boost::spirit::istream_iterator iter_type;
      
      is.unsetf(std::ios::skipws);
      
      iter_type iter(is);
      iter_type iter_end;
      
      while (iter != iter_end) {
	
	ids.clear();
	if (! qi::phrase_parse(iter, iter_end, *qi::uint_ >> (qi::eol | qi::eoi), standard::blank, ids))
	  throw std::runtime_error("parsing failed");

	if (ids.empty()) continue;

	ngram_state.copy(state_bos, state);
	
	id_set_type::const_iterator siter_end = ids.end();
	for (id_set_type::const_iterator siter = ids.begin(); siter != siter_end; ++ siter) {
	  const word_type::id_type& word_id = *siter;
	  const bool is_oov = (word_id == unk_id) || (word_id == none_id);
	  
	  const float logprob = ngram.ngram_score(state, word_id, state_next).prob;
	  
	  std::swap(state, state_next);
	  
	  if (! is_oov)
	    logprob_total += logprob;
	  logprob_total_oov += logprob;
	  
	  num_oov += is_oov;
	}
	
	const float logprob = ngram.ngram_score(state, eos_id, state_next).prob;
	
	logprob_total     += logprob;
	logprob_total_oov += logprob;
	
	num_word += sentence.size();
	++ num_sentence;	
      }
      
    } else {
      while (is >> sentence) {
	// add BOS and EOS
	
	if (sentence.empty()) continue;
	
	ngram_state.copy(state_bos, state);
	
	sentence_type::const_iterator siter_end = sentence.end();
	for (sentence_type::const_iterator siter = sentence.begin(); siter != siter_end; ++ siter) {
	  const word_type::id_type word_id = ngram.index.vocab()[*siter];
	  const bool is_oov = (word_id == unk_id) || (word_id == none_id);
	  
	  const float logprob = ngram.ngram_score(state, word_id, state_next).prob;
	  
	  std::swap(state, state_next);
	  
	  if (! is_oov)
	    logprob_total += logprob;
	  logprob_total_oov += logprob;
	  
	  num_oov += is_oov;
	}
	
	const float logprob = ngram.ngram_score(state, eos_id, state_next).prob;
	
	logprob_total     += logprob;
	logprob_total_oov += logprob;
	
	num_word += sentence.size();
	++ num_sentence;
      }
    }

    utils::resource end;
    
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
    
    os << "cpu:    " << 1e-3 * (num_word + num_sentence) / (end.cpu_time() - start.cpu_time()) << " queries/ms" << std::endl
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
    ("ngram",     po::value<path_type>(&ngram_file)->default_value(ngram_file),   "ngram in ARPA or expgram format")
    ("input",     po::value<path_type>(&input_file)->default_value(input_file),   "input")
    ("output",    po::value<path_type>(&output_file)->default_value(output_file), "output")
    ("temporary", po::value<path_type>(&temporary_dir),                           "temporary directory")    
    
    ("shard",  po::value<int>(&shards)->default_value(shards),                 "# of shards (or # of threads)")
    
    ("populate",   po::bool_switch(&populate),   "perform memory population")
    ("precompute", po::bool_switch(&precompute), "assume precomputed word-id input")
    
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
