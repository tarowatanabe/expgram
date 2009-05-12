// -*- mode: c++ -*-

#ifndef __EXPGRAM__STEMMER__HPP__
#define __EXPGRAM__STEMMER__HPP__ 1

#include <string>

namespace expgram
{
  
  class Stemmer
  {
  public:
    Stemmer();
    Stemmer(const std::string& algorithm);
    Stemmer(const Stemmer& x);
    ~Stemmer();
    Stemmer& operator=(const Stemmer& x) { assign(x); return *this; }
    
  public:
    operator bool() const { return __handle; }
    std::string operator()(const std::string& word) const;
    
    void assign(const Stemmer& x);
      
  private:
    std::string __algorithm;
    void*       __handle;
  };
  
};

#endif
