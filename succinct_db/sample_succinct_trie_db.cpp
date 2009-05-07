
#include <iostream>
#include <sstream>
#include <string>

#include <map>

#include "succinct_trie_db.hpp"

int main(int argc, char** argv)
{
  typedef succinctdb::succinct_trie_db<char, double> succinct_db_type;
  typedef std::multimap<std::string, double> map_db_type;

  srandom(time(0) * getpid());
  
  for (int iter = 0; iter < 4; ++ iter) {
    
    succinct_db_type succinct_db("tmptmp.db.fixed", succinct_db_type::write);
    map_db_type      map_db;
    
    for (int i = 0; i < 1024 * 16; ++ i) {
      const int value = random();
      const double value_double = double(random()) /  random();
      
      std::ostringstream stream;
      stream << value;
      const std::string value_str = stream.str();
      
      if (! value_str.empty()) {
	map_db.insert(std::make_pair(value_str, value_double));
	const size_t size = succinct_db.insert(value_str.c_str(), value_str.size(), &value_double);
	if (size + 1 != map_db.size())
	  std::cerr << "different size...?" << std::endl;
      }
    }
    succinct_db.close();
    succinct_db.open("tmptmp.db.fixed", succinct_db_type::read);

    std::cerr << "db size: " << succinct_db.size() << std::endl;
    
    for (map_db_type::const_iterator iter = map_db.begin(); iter != map_db.end(); ++ iter) {
      
      const succinct_db_type::size_type node_pos = succinct_db.find(iter->first.c_str(), iter->first.size());

      if (! succinct_db.is_valid(node_pos))
	std::cerr << "out of range..?" << std::endl;
      
      if (! succinct_db.exists(node_pos))
	std::cerr << "NO KEY FOUND?" << std::endl;
      else {
	
	bool found = false;
	for (succinct_db_type::const_cursor citer = succinct_db.cbegin(node_pos); citer != succinct_db.cend(); ++ citer)
	  if (*citer == iter->second)
	    found = true;
	if (! found)
	  std::cerr << "no data?" << std::endl;
      }
	
    }
  }
}
