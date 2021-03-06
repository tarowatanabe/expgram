//
//  Copyright(C) 2009-2012 Taro Watanabe <taro.watanabe@nict.go.jp>
//

#include <iostream>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

#include <utils/resource.hpp>
#include <utils/program_options.hpp>

#include <expgram/NGram.hpp>

typedef boost::filesystem::path path_type;

path_type ngram_file = "-";
path_type output_file;
path_type temporary_dir = "";

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

    if (output_file.empty())
      throw std::runtime_error("no output file?");
    
    expgram::NGram ngram(ngram_file, shards, debug);

    ngram.logbounds.clear();

    utils::resource start;
    
    ngram.bounds();

    utils::resource end;
    
    if (debug)
      std::cerr << "upper bound"
		<< " cpu time:  " << end.cpu_time() - start.cpu_time() 
		<< " user time: " << end.user_time() - start.user_time()
		<< std::endl;
    
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
    ("ngram",     po::value<path_type>(&ngram_file)->default_value(ngram_file),   "ngram in ARPA or expgram format")
    ("output",    po::value<path_type>(&output_file)->default_value(output_file), "output in expgram format with ngram bound estimation")
    ("temporary", po::value<path_type>(&temporary_dir),                           "temporary directory")

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
