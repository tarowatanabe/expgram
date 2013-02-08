//
//  Copyright(C) 2009-2012 Taro Watanabe <taro.watanabe@nict.go.jp>
//

#include <iostream>
#include <sstream>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

#include <utils/resource.hpp>
#include <utils/compress_stream.hpp>
#include <utils/program_options.hpp>

#include <expgram/NGram.hpp>

typedef boost::filesystem::path path_type;

template <typename Tp>
std::string dump_size(const Tp& size)
{
  const double size_float(size);
  const double size_giga = size_float / (1024 * 1024 * 1024);
  const double size_mega = size_float / (1024 * 1024);
  const double size_kilo = size_float / (1024);
  
  std::ostringstream os;
  os << std::setw(8) << std::setiosflags(std::ios::fixed) << std::setprecision(3);
  
  if (size_giga >= 1.0)
    os << size_giga << "G";
  else if (size_mega >= 1.0)
    os << size_mega << "M";
  else if (size_kilo >= 1.0)
    os << size_kilo << "K";
  else 
    os << size;
  
  return os.str();
}

template <typename Stat>
std::ostream& dump(std::ostream& os, const std::string& name, const Stat& stat)
{
  os << std::setw(9) << std::setiosflags(std::ios::left) << name
     << " raw: " << dump_size(stat.bytes())
     << " compressed: " << dump_size(stat.compressed())
     << " cache: " << dump_size(stat.cache());
  return os;
}

path_type ngram_file = "-";
path_type output_file = "-";

int shards = 4;

int debug = 0;

int getoptions(int argc, char** argv);

int main(int argc, char** argv)
{
  try {
    if (getoptions(argc, argv) != 0) 
      return 1;
    
    expgram::NGram ngram(ngram_file, shards, debug);
    
    utils::compress_ostream os(output_file);
    os << "ngram order: " << ngram.index.order() << '\n';
    for (int order = 1; order <= ngram.index.order(); ++ order)
      os << order << "-gram: " << std::setw(16) << ngram.index.ngram_size(order) << '\n';
    
    dump(os, "index",    ngram.stat_index()) << '\n';
    dump(os, "pointer",  ngram.stat_pointer()) << '\n';
    dump(os, "vocab",    ngram.stat_vocab()) << '\n';
    dump(os, "logprob",  ngram.stat_logprob()) << '\n';
    dump(os, "backoff",  ngram.stat_backoff()) << '\n';
    dump(os, "logbound", ngram.stat_logbound()) << '\n';
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
    ("output", po::value<path_type>(&output_file)->default_value(output_file), "output")
    
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
