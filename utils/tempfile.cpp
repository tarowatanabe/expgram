
#include <unistd.h>

#include "tempfile.hpp"

#include <string>

#include <boost/regex.hpp>

namespace utils
{

  boost::mutex tempfile::mutex;
  tempfile::__files_type   tempfile::__files = tempfile::__files_type();
  tempfile::__signals_type tempfile::__signals = tempfile::__signals_type();

  inline 
  std::string get_hostname()
  {
    char name[HOST_NAME_MAX];
    int result = ::gethostname(name, sizeof(name));
    if (result < 0)
      throw std::runtime_error("gethostname()");
    
    return std::string(name);
  }
  
  inline
  std::string get_hostname_short()
  {
    std::string hostname = get_hostname();
    
    return boost::regex_replace(hostname, boost::regex("[.].+$"), "");
  }
  
  
  tempfile::path_type tempfile::tmp_dir()
  {
    // wired to /tmp... is this safe?
    std::string tmpdir("/tmp");
    
    const char* tmpdir_spec_env = getenv("TMPDIR_SPEC");
    if (tmpdir_spec_env) {
      std::string tmpdir_spec(tmpdir_spec_env);
      
      const std::string tmpdir_spec_short = boost::regex_replace(tmpdir_spec, boost::regex("%host"), get_hostname_short());
      const std::string tmpdir_spec_long = boost::regex_replace(tmpdir_spec, boost::regex("%host"), get_hostname());

      if (boost::filesystem::exists(tmpdir_spec_short) && boost::filesystem::is_directory(tmpdir_spec_short))
	return tmpdir_spec_short;
      
      if (boost::filesystem::exists(tmpdir_spec_long) && boost::filesystem::is_directory(tmpdir_spec_long))
	return tmpdir_spec_long;
    }
      
    const char* tmpdir_env = getenv("TMPDIR");
    if (! tmpdir_env)
      return path_type(tmpdir);
      
    const path_type tmpdir_env_path(tmpdir_env);
    if (boost::filesystem::exists(tmpdir_env_path) && boost::filesystem::is_directory(tmpdir_env_path))
      return tmpdir_env_path;
    else
      return path_type(tmpdir);
    
    
  }
  
};
