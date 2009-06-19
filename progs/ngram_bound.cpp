
#include <iostream>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

#include <utils/resource.hpp>
#include <utils/program_options.hpp>

#include <expgram/NGram.hpp>

typedef boost::filesystem::path path_type;

path_type ngram_file = "-";
path_type output_file;

int shards = 4;
bool smooth_smallest = false;
int debug = 0;

int getoptions(int argc, char** argv);

int main(int argc, char** argv)
{
  try {
    if (getoptions(argc, argv) != 0) 
      return 1;

    if (output_file.empty())
      throw std::runtime_error("no output file?");
    
    expgram::NGram ngram(ngram_file, shards, smooth_smallest, debug);
    
    ngram.bounds();
    
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
    ("ngram",  po::value<path_type>(&ngram_file)->default_value(ngram_file),   "ngram in ARPA or expgram format")
    ("output", po::value<path_type>(&output_file)->default_value(output_file), "output in expgram format with ngram bound estimation")
    
    ("shard",  po::value<int>(&shards)->default_value(shards),                 "# of shards (or # of threads)")
    ("smooth-smallest", utils::true_false_switch(&smooth_smallest),            "use of smallest value for UNK...")
    
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
