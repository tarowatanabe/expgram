//
//  Copyright(C) 2009-2012 Taro Watanabe <taro.watanabe@nict.go.jp>
//

#include <iostream>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

#include <utils/resource.hpp>

#include <expgram/NGramCounts.hpp>

typedef boost::filesystem::path path_type;
typedef expgram::NGramCounts::count_type count_type;

path_type ngram_file;
path_type output_file = "-";

int shards = 4;

int debug = 0;

int getoptions(int argc, char** argv);

int main(int argc, char** argv)
{
  try {
    if (getoptions(argc, argv) != 0) 
      return 1;
    
    expgram::NGramCounts ngram(ngram_file, shards, debug);
    ngram.dump(output_file);
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
    ("ngram",  po::value<path_type>(&ngram_file),                              "ngram counts in Google of expgram")
    ("output", po::value<path_type>(&output_file)->default_value(output_file), "output in text")
    
    ("shard",  po::value<int>(&shards),            "# of shards (or # of threads)")

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
