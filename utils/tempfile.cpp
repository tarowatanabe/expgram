
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
    const char* tmpdir_spec_env = getenv("TMPDIR_SPEC");
    if (tmpdir_spec_env) {
      std::string tmpdir_spec(tmpdir_spec_env);
      
      const std::string tmpdir_spec_short = boost::regex_replace(tmpdir_spec, boost::regex("%host"), get_hostname_short());
      const std::string tmpdir_spec_long = boost::regex_replace(tmpdir_spec, boost::regex("%host"), get_hostname());
#if 0
      // do we check for ':' and perform splitting?
      {
	boost::sregex_token_iterator iter(tmpdir_spec_short.begin(), tmpdir_spec_short.end(), boost::regex(":"), -1);
	boost::sregex_token_iterator iter_end;
	
	for (/**/; iter != iter_end; ++ iter) {
	  const std::string __tmpdir = *iter;
	  
	  if (! __tmpdir.empty() && boost::filesystem::exists(__tmpdir) && boost::filesystem::is_directory(__tmpdir))
	    return __tmpdir;
	}
      }
      
      
      {
	boost::sregex_token_iterator iter(tmpdir_spec_long.begin(), tmpdir_spec_long.end(), boost::regex(":"), -1);
	boost::sregex_token_iterator iter_end;
	
	for (/**/; iter != iter_end; ++ iter) {
	  const std::string __tmpdir = *iter;
	  
	  if (! __tmpdir.empty() && boost::filesystem::exists(__tmpdir) && boost::filesystem::is_directory(__tmpdir))
	    return __tmpdir;
	}
      }
#endif
#if 1
      if (boost::filesystem::exists(tmpdir_spec_short) && boost::filesystem::is_directory(tmpdir_spec_short))
	return tmpdir_spec_short;
      
      if (boost::filesystem::exists(tmpdir_spec_long) && boost::filesystem::is_directory(tmpdir_spec_long))
	return tmpdir_spec_long;
#endif
    }
    
    // wired to /tmp... is this safe?
    std::string tmpdir("/tmp");
    
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
