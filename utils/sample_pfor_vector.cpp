
#include <iostream>
#include <vector>

#include "pfor_vector.hpp"
#include "pfor_device.hpp"

int main(int argc, char** argv)
{
  srandom(time(0) * getpid());
  
  for (int i = 0; i < 16; ++ i) {
    std::cerr << "iteration: " << i << std::endl;
    
    std::vector<int> integers;
    for (int j = 0; j < 256 * 256 + 10; ++ j)
      integers.push_back(random() % (1024));
    
    utils::pfor_vector<int> pfor_ints;
    boost::iostreams::filtering_ostream os;
    os.push(utils::pfor_sink<int>("tmptmp.pfor.iostream"));

    for (int j = 0; j < 256 * 256 + 10; ++ j) {
      pfor_ints.push_back(integers[j]);
      os.write((char*) &integers[j], sizeof(int));
    }
    os.pop();
    
    pfor_ints.build();
    pfor_ints.write("tmptmp.pfor");
    
    utils::pfor_vector_mapped<int> pfor_ints_mapped("tmptmp.pfor");
    utils::pfor_vector_mapped<int> pfor_ints_mapped_stream("tmptmp.pfor.iostream");
    
    for (int j = 0; j < 256 * 256 + 10; ++ j) {
      
      const int value = pfor_ints[j];
      const int value_mapped = pfor_ints_mapped[j];
      const int value_mapped_stream = pfor_ints_mapped_stream[j];
      
      if (value != integers[j])
	std::cerr << "DIFFER(raw   ): i = " << j << " " << value << " " << integers[j] << std::endl;
      
      if (value_mapped != integers[j])
	std::cerr << "DIFFER(mapped): i = " << j << " " << value_mapped << " " << integers[j] << std::endl;
      
      if (value_mapped_stream != integers[j])
	std::cerr << "DIFFER(stream): i = " << j << " " << value_mapped_stream << " " << integers[j] << std::endl;
    }
  }
}
