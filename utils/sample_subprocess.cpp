
#include <iostream>
#include <string>

#include "utils/subprocess.hpp"

#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/device/file_descriptor.hpp>

int main(int argc, char** argv)
{
  utils::subprocess run("uptime");
  
  boost::iostreams::filtering_istream is;
  is.push(boost::iostreams::file_descriptor_source(run.desc_read(), true));
  
  std::string line;
  while (std::getline(is, line))
    std::cout << line << std::endl;
}
