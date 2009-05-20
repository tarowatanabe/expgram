// -*- encoding: utf-8 -*-

#include <iostream>

#include <string>
#include <vector>
#include <stdexcept>
#include <iterator>
#include <algorithm>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>

#include "utils/compress_stream.hpp"
#include "utils/regex_group.hpp"

#include <unicode/uchar.h>
#include <unicode/unistr.h>
#include <unicode/bytestream.h>

typedef utils::regex_group      regex_group_type;
typedef boost::filesystem::path path_type;

path_type input_file = "-";
path_type output_file = "-";

int debug = 0;

int getoptions(int argc, char** argv);
void initialize(regex_group_type& regex_group);

int main(int argc, char** argv)
{
  try {
    if (getoptions(argc, argv) != 0) 
      return 1;
        
    regex_group_type regex_group;
    initialize(regex_group);
    
    std::string line;
    
    utils::compress_istream is(input_file, 1024 * 1024);
    utils::compress_ostream os(output_file, 1024 * 1024);
  
    while (std::getline(is, line)) {
    
      UnicodeString uline = UnicodeString::fromUTF8(line);
    
      // apply a seriese of regex substitution...
      regex_group(uline);
    
      line.clear();
      StringByteSink<std::string> __sink(&line);
      uline.toUTF8(__sink);
    
      os << line << '\n';
    }
  }
  catch (std::exception& err) {
    std::cerr << "error: " << err.what() << std::endl;
    return 1;
  }
  return 0;
}

/*
  split all:
  !"$%~{}[]<>\
  
  split all, but sgml entity:
  #&;

  &(#|)[a-z0-9A-Z];

  split all but smiley
  ()|
  
  8-(
  :-(
  ;-(
  8-)
  :-)
  ;-)
  8-|
  :-|
  ;-|

  ??:
  '

  grop: ``

  data like:
  
 */

void initialize(regex_group_type& regex_group)
{
  regex_group.insert()
    // normalize spaces
    ("[[:C:]-[:White_Space:]]+", "")
    ("[[:C:][:White_Space:]]+", " ")
    
    // split by punctuations...
    ("[;@$&#%`~|\\/\"^]", "\t$0\t")
    
    // brackets
    ("[\\[\\](){}<>]", "\t$0\t")
    
    // fix-up sgml entities...
    ("\t&\t#\t([a-zA-Z0-9]+)\t;\t", "&#$1;")
    ("\t&\t([a-zA-Z]+)\t;\t", "&$1;")
    // smiley
    ("([;:8])\t-\t[|()]\t", "\t$1-$2\t")
    
    // sentence is surrounded by spaces
    ("^(.*)$", " $1 ")
    // it's, I'm, we'd
    ("(?<=[^[:White_Space:]])'([smd])(?=[[:White_Space:]])", "\t'$1", true)
    // I'll, You're, You've
    ("(?<=[^[:White_Space:]])'(ll|re|ve)(?=[[:White_Space:]])", "\t'$1", true)
    // n't
    ("(?<=[^[:White_Space:]])(n't)(?=[[:White_Space:]])", "\t$1", true)
    
    // final concatenation
    ("[[:White_Space:]]+", " ")
    ("^[[:White_Space:]]+", "")
    ("[[:White_Space:]]+$", "");
    
}

int getoptions(int argc, char** argv)
{
  namespace po = boost::program_options;
  
  po::options_description desc("options");
  desc.add_options()
    ("input",  po::value<path_type>(&input_file),  "input")
    ("output", po::value<path_type>(&output_file), "output")
    
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
