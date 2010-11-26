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

int order = 0;

int shards = 4;
int debug = 0;

int getoptions(int argc, char** argv);

int main(int argc, char** argv)
{
  try {
    if (getoptions(argc, argv) != 0) 
      return 1;

    typedef expgram::NGram    ngram_type;
    typedef expgram::Word     word_type;
    typedef expgram::Vocab    vocab_type;
    typedef expgram::Sentence sentence_type;

    typedef std::vector<word_type::id_type, std::allocator<word_type::id_type> > id_set_type;
    
    ngram_type ngram(ngram_file, shards, debug);
    
    sentence_type sentence;
    id_set_type ids;
    
    const bool flush_output = (output_file == "-" || (boost::filesystem::exists(output_file) && ! boost::filesystem::is_regular_file(output_file)));
    
    utils::compress_istream is(input_file);
    utils::compress_ostream os(output_file, 1024 * 1024 * (! flush_output));
    
    order = (order <= 0 ? ngram.index.order() : order);
    
    const word_type::id_type bos_id = ngram.index.vocab()[vocab_type::BOS];
    const word_type::id_type eos_id = ngram.index.vocab()[vocab_type::EOS];
    
    while (is >> sentence) {
      // add BOS and EOS
      
      if (sentence.empty()) continue;

      ids.clear();
      ids.push_back(bos_id);
      sentence_type::const_iterator siter_end = sentence.end();
      for (sentence_type::const_iterator siter = sentence.begin(); siter != siter_end; ++ siter)
	ids.push_back(ngram.index.vocab()[*siter]);
      ids.push_back(eos_id);
      
      double logprob = 0.0;
      id_set_type::const_iterator iter_begin = ids.begin();
      id_set_type::const_iterator iter_end   = ids.end();
      for (id_set_type::const_iterator iter = iter_begin + 1; iter != iter_end; ++ iter)
	logprob += ngram(std::max(iter_begin, iter + 1 - order), iter + 1);
      
      os << logprob << '\n';
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
    ("input",  po::value<path_type>(&input_file)->default_value(input_file),   "input")
    ("output", po::value<path_type>(&output_file)->default_value(output_file), "output")
    
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