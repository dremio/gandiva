// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef GANDIVA_REGEX_UTIL_H
#define GANDIVA_REGEX_UTIL_H

#include <set>
#include "gandiva/status.h"

namespace gandiva {

#ifdef GDV_HELPERS
namespace helpers {
#endif

/// \brief Utility class for converting sql patterns to pcre patterns.
class RegexUtil {
 public:
  // Convert an sql pattern to a pcre pattern
  static Status SqlLikePatternToPcre(const std::string &like_pattern, char escape_char,
                                     std::string &pcre_pattern);

  static Status SqlLikePatternToPcre(const std::string &like_pattern,
                                     std::string &pcre_pattern) {
    return SqlLikePatternToPcre(like_pattern, 0 /*escape_char*/, pcre_pattern);
  }

 private:
  static const std::set<char> pcre_regex_specials_;
};

#ifdef GDV_HELPERS
}  // namespace helpers
#endif

}  // namespace gandiva

#endif  // GANDIVA_REGEX_UTIL_H
