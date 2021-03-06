//
//  Copyright(C) 2010-2011 Taro Watanabe <taro.watanabe@nict.go.jp>
//

#include <iostream>

#include <utils/tempfile.hpp>

int main(int argc, char** argv)
{
  for (int i = 0; i < 16; ++ i) {
    const boost::filesystem::path temp = utils::tempfile::file_name(utils::tempfile::tmp_dir() / "testing.XXXXXX");
    std::cout << temp.string() << std::endl;
    utils::tempfile::insert(temp);
  }
  
  for (int i = 0; i < 16; ++ i) {
    const boost::filesystem::path temp = utils::tempfile::directory_name(utils::tempfile::tmp_dir() / "testing.XXXXXX");
    std::cout << temp.string() << std::endl;
    utils::tempfile::insert(temp);
  }
  
  sleep(100);
}
