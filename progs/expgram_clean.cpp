//
//  Copyright(C) 2009-2012 Taro Watanabe <taro.watanabe@nict.go.jp>
//

// clear temporary files

#include <boost/filesystem.hpp>
#include <boost/algorithm/string/find.hpp>

#include <utils/tempfile.hpp>
#include <utils/filesystem.hpp>

typedef boost::filesystem::path path_type;

int main(int argc, char** argv)
{
  try {
    const path_type tmp_dir = utils::tempfile::tmp_dir();

    static const char* expgram_suffix[9] = {
      "logprob",
      "backoff",
      "logbound",
      "index",
      "position",
      "vocab",
      "modified",
      "count",
      "accumulated",
    };
    
    static const char* succinct_suffix[2] = {
      "size",
      "key-data",
    };
    
    boost::filesystem::directory_iterator iter_end;
    for (boost::filesystem::directory_iterator iter(tmp_dir); iter != iter_end; ++ iter) {
      const path_type path = *iter;

#if BOOST_FILESYSTEM_VERSION == 2
      for (int i = 0; i < 9; ++ i)
	if (path_type(path.filename()).file_string().find(std::string("expgram.") + expgram_suffix[i]) != std::string::npos)
	  utils::filesystem::remove_all(path);
      for (int i = 0; i < 2; ++ i)
	if (path_type(path.filename()).file_string().find(std::string("succinct-db.") + succinct_suffix[i]) != std::string::npos)
	  utils::filesystem::remove_all(path);
#else
      for (int i = 0; i < 9; ++ i)
	if (path_type(path.filename()).string().find(std::string("expgram.") + expgram_suffix[i]) != std::string::npos)
	  utils::filesystem::remove_all(path);
      for (int i = 0; i < 2; ++ i)
	if (path_type(path.filename()).string().find(std::string("succinct-db.") + succinct_suffix[i]) != std::string::npos)
	  utils::filesystem::remove_all(path);
#endif
    }
  }
  catch (const std::exception& err) {
    std::cerr << "error: " << err.what() << std::endl;
    return -1;
  }
  return 0;
}
