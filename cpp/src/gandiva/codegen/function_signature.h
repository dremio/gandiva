/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef GANDIVA_FUNCTION_SIGNATURE_H
#define GANDIVA_FUNCTION_SIGNATURE_H

#include <unordered_map>
#include "Types.pb.h"

namespace gandiva {

/*
 * Signature for a function : includes the base name, input params and output param.
 */
class FunctionSignature {
 public:
  FunctionSignature(std::string base_name,
                    std::vector<common::MinorType> param_types,
                    common::MinorType ret_type)
      : base_name_(base_name),
        param_types_(param_types),
        ret_type_(ret_type)
  {}

  FunctionSignature(std::string base_name,
                    std::vector<const common::MajorType *> major_param_types,
                    const common::MajorType *major_ret_type)
      : base_name_(base_name),
        ret_type_(major_ret_type->minor_type())
  {
    for (auto it = major_param_types.begin(); it != major_param_types.end(); ++it) {
      param_types_.push_back((*it)->minor_type());
    }
  }

  bool operator == (const FunctionSignature &other) const {
    return param_types_.size() == other.param_types_.size() &&
        ret_type_ == other.ret_type_ &&
        base_name_ == other.base_name_ &&
        param_types_ == other.param_types_;
  }

  std::size_t Hash() const {
    size_t result = 17;
    result = result * 31 + std::hash<std::string>()(base_name_);
    result = result * 31 + std::hash<int>()(ret_type_);
    for (auto it = param_types_.begin(); it != param_types_.end(); it++) {
      result = result * 31 + std::hash<int>()(*it);
    }
    return result;
  }

  common::MinorType ret_type() const { return ret_type_; }

 private:
  std::string base_name_;
  std::vector<common::MinorType> param_types_;
  common::MinorType ret_type_;
};

} // namespace gandiva

#endif //GANDIVA_FUNCTION_SIGNATURE_H

