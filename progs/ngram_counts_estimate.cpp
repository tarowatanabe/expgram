
#include <iostream>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

#include <utils/resource.hpp>

#include <expgram/NGram.hpp>
#include <expgram/NGramCounts.hpp>

typedef boost::filesystem::path path_type;

path_type ngram_file;
path_type output_file;

bool remove_unk = false;

int shards = 4;
bool unique = false;
int debug = 0;

int getoptions(int argc, char** argv);

int main(int argc, char** argv)
{
  try {
    if (getoptions(argc, argv) != 0) 
      return 1;
    
    expgram::NGramCounts ngram_counts(ngram_file, shards, unique, debug);
    
    expgram::NGram ngram(debug);
    ngram_counts.estimate(ngram, remove_unk);
    
    if (! output_file.empty())
      ngram.write(output_file);
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
    ("ngram",  po::value<path_type>(&ngram_file),  "ngram counts")
    ("output", po::value<path_type>(&output_file), "output in binary format")

    ("remove-unk", po::bool_switch(&remove_unk),   "remove UNK when estimating language model")
    
    ("shard",  po::value<int>(&shards),            "# of shards (or # of threads)")
    ("unique", po::bool_switch(&unique),           "unique counts (i.e. ngram counts from LDC/GSK)")
    
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
