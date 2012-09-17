//
//  Copyright(C) 2009-2012 Taro Watanabe <taro.watanabe@nict.go.jp>
//

#include <iterator>

#include "Sentence.hpp"

#include "utils/space_separator.hpp"
#include "utils/piece.hpp"

#include <boost/tokenizer.hpp>

namespace expgram
{
  
  std::ostream& operator<<(std::ostream& os, const Sentence& x)
  {
    if (! x.empty()) {
      std::copy(x.__sent.begin(), x.__sent.end() - 1, std::ostream_iterator<Sentence::word_type>(os, " "));
      os << x.__sent.back();
    }
    return os;
  }
  
  std::istream& operator>>(std::istream& is, Sentence& x)
  {
    typedef std::vector<utils::piece, std::allocator<utils::piece> > tokens_type;
    typedef boost::tokenizer<utils::space_separator, utils::piece::const_iterator, utils::piece> tokenizer_type;
    
    std::string line;
    x.clear();
    if (std::getline(is, line)) {
      utils::piece line_piece(line);
      tokenizer_type tokenizer(line_piece);
      x.__sent.assign(tokenizer.begin(), tokenizer.end());
    }
    
    return is;
  }
  
};
