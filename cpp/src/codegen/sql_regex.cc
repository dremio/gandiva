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

#include "codegen/sql_regex.h"

namespace gandiva {

const std::set<char> SqlRegex::posix_regex_specials = {
    '[', ']', '(', ')', '|', '^', '-', '+', '*', '?', '{', '}', '$', '\\'};

SqlRegex::SqlRegex(const std::string &pattern) : regex_(pattern) {}

Status SqlRegex::Make(const std::string &sql_pattern, char escape_char,
                      std::shared_ptr<SqlRegex> *regex) {
  std::string posix_pattern;
  auto status = SqlPatternToPosixPattern(sql_pattern, escape_char, posix_pattern);
  GANDIVA_RETURN_ARROW_NOT_OK(status);

  *regex = std::shared_ptr<SqlRegex>(new SqlRegex(posix_pattern));
  return Status::OK();
}

Status SqlRegex::SqlPatternToPosixPattern(const std::string &sql_pattern,
                                          char escape_char, std::string &posix_pattern) {
  posix_pattern.clear();
  for (int idx = 0; idx < sql_pattern.size(); ++idx) {
    auto cur = sql_pattern.at(idx);

    // Escape any char that is special for posix regex
    if (posix_regex_specials.find(cur) != posix_regex_specials.end()) {
      posix_pattern += "\\";
    }

    if (cur == escape_char) {
      // escape char must be followed by '_', '%' or the escape char itself.
      ++idx;
      if (idx == sql_pattern.size()) {
        std::stringstream msg;
        msg << "unexpected escape char at the end of pattern " << sql_pattern;
        return Status::Invalid(msg.str());
      }

      cur = sql_pattern.at(idx);
      if (cur == '_' || cur == '%' || cur == escape_char) {
        posix_pattern += cur;
      } else {
        std::stringstream msg;
        msg << "invalid escape sequence in pattern " << sql_pattern << " at offset "
            << idx;
        return Status::Invalid(msg.str());
      }
    } else if (cur == '_') {
      posix_pattern += '.';
    } else if (cur == '%') {
      posix_pattern += ".*";
    } else {
      posix_pattern += cur;
    }
  }
  return Status::OK();
}

}  // namespace gandiva
