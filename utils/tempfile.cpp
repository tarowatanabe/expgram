
#include "tempfile.hpp"

namespace utils
{

  boost::mutex tempfile::mutex;
  tempfile::__files_type   tempfile::__files = tempfile::__files_type();
  tempfile::__signals_type tempfile::__signals = tempfile::__signals_type();
  
};
