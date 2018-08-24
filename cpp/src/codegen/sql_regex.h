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

#ifndef GANDIVA_SQL_REGEX_H
#define GANDIVA_SQL_REGEX_H

#include <regex>
#include <set>
#include "gandiva/status.h"

namespace gandiva {

class Operator {
 public:
  virtual ~Operator() = default;
};

/// \brief Build a regex matcher for the specified 'sql_pattern'
class SqlRegex : public Operator {
 public:
  ~SqlRegex() = default;

  /// Build a regex using the given sql pattern.
  static Status Make(const std::string &sql_pattern, std::shared_ptr<SqlRegex> *regex) {
    return Make(sql_pattern, 0, regex);
  }

  /// Build a regex using the given sql pattern. Treat the characters in escape_list as
  /// escape characters.
  static Status Make(const std::string &sql_pattern, char escape_char,
                     std::shared_ptr<SqlRegex> *regex);

  /// Returns true if 'data' matches the sql_pattern.
  bool Like(std::string data) { return std::regex_match(data, regex_); }

 private:
  SqlRegex(const std::string &pattern);

  // set of characters that std::regex treats as special.
  static const std::set<char> posix_regex_specials;

  // Convert an sql pattern to an std::regex pattern
  static Status SqlPatternToPosixPattern(const std::string &sql_pattern, char escape_char,
                                         std::string &output);

  std::regex regex_;
};

}  // namespace gandiva

#endif  // GANDIVA_SQL_REGEX_H
