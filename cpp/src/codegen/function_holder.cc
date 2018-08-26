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

#include "codegen/function_holder.h"

#include "codegen/node.h"
#include "codegen/sql_regex.h"

namespace gandiva {

Status FunctionHolder::Make(const std::string &name, const FunctionNode &node,
                            std::shared_ptr<FunctionHolder> *holder) {
  if (name.compare("like")) {
    return Status::Invalid("unknown function " + name);
  }
  if (node.children().size() != 2) {
    return Status::Invalid("expected two parameters");
  }

  auto literal = dynamic_cast<LiteralNode *>(node.children().at(0).get());
  if (literal == nullptr) {
    return Status::Invalid("expected literal as the first parameter");
  }

  if (literal->return_type()->id() != arrow::Type::STRING &&
      literal->return_type()->id() != arrow::Type::BINARY) {
    return Status::Invalid("expected string or binary literal");
  }
  auto pattern = boost::get<std::string>(literal->holder());

  std::shared_ptr<SqlRegex> regex;
  auto status = SqlRegex::Make(pattern, &regex);
  GANDIVA_RETURN_NOT_OK(status);

  *holder = regex;
  return Status::OK();
}

extern "C" bool like_utf8_utf8(int64_t holder, const char *pattern, int pattern_len,
                               const char *data, int data_len) {
  SqlRegex *regex = reinterpret_cast<SqlRegex *>(holder);
  return regex->Like(std::string(data, data_len));
}

}  // namespace gandiva
