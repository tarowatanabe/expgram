//
//  Copyright(C) 2013 Taro Watanabe <taro.watanabe@nict.go.jp>
//

#include <iostream>
#include <vector>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

#include <utils/resource.hpp>
#include <utils/program_options.hpp>

#include <expgram/NGram.hpp>

typedef boost::filesystem::path path_type;
typedef std::vector<path_type, std::allocator<path_type> > path_set_type;

path_set_type ngram_files;
path_type output_file = "-";
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
    
    if (ngram_files.size() <= 1) return 0;
    
    typedef std::vector<boost::shared_ptr<expgram::NGram>, std::allocator<boost::shared_ptr<expgram::NGram> > > ngram_set_type;
    
    ngram_set_type ngrams(ngram_files.size());
    
    for (size_t i = 0; i != ngram_files.size(); ++ i) {
      ngrams[i].reset(new expgram::NGram(ngram_files[i], shards, debug));
      
      if (i && *ngrams[0] != *ngrams[i])
	std::cerr << i << "th ngram is different from the first ngram!" << std::endl;
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
    ("output",    po::value<path_type>(&output_file)->default_value(output_file), "output result")
    ("temporary", po::value<path_type>(&temporary_dir),                           "temporary directory")

    ("shard",  po::value<int>(&shards)->default_value(shards), "# of shards (or # of threads)")
    
    ("debug", po::value<int>(&debug)->implicit_value(1), "debug level")
    ("help", "help message");
  
  po::options_description hidden;
  hidden.add_options()
    ("ngram", po::value<path_set_type>(&ngram_files), "input file");

  po::options_description cmdline_options;
  cmdline_options.add(desc).add(hidden);

  po::positional_options_description pos;
  pos.add("ngram", -1); // all the files

  po::variables_map vm;
  po::store(po::command_line_parser(argc, argv).style(po::command_line_style::unix_style & (~po::command_line_style::allow_guessing)).options(cmdline_options).positional(pos).run(), vm);
  po::notify(vm);
  
  if (vm.count("help")) {
    std::cout << argv[0] << " [options] ngram(s)" << '\n' << desc << '\n';
    return 1;
  }
  
  return 0;
}
