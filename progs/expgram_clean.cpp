// clear temporary files

#include <boost/filesystem.hpp>
#include <boost/regex.hpp>

#include <utils/tempfile.hpp>
#include <utils/filesystem.hpp>

typedef boost::filesystem::path path_type;

int main(int argc, char** argv)
{
  try {
    const path_type tmp_dir = utils::tempfile::tmp_dir();

    boost::regex pattern_expgram("^expgram\\.(logprob|backoff|logbound|index|position|vocab|modified|count|accumulated)\\..+$");
    boost::regex pattern_succinct("^succinct-db\\.(size|key-data)\\..+$");
    
    boost::filesystem::directory_iterator iter_end;
    for (boost::filesystem::directory_iterator iter(tmp_dir); iter != iter_end; ++ iter) {
      const path_type path = *iter;
      
      if (boost::regex_search(path.filename(), pattern_expgram))
	utils::filesystem::remove_all(path);
      else if (boost::regex_search(path.filename(), pattern_succinct))
	utils::filesystem::remove_all(path);
    }
  }
  catch (const std::exception& err) {
    std::cerr << "error: " << err.what() << std::endl;
    return -1;
  }
  return 0;
}
