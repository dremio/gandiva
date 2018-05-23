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

#include <string>
#include <sstream>
#include <vector>
#include "common/arrow.h"
#include "common/logging.h"

namespace gandiva {

///
/// \brief Signature for a function : includes the base name, input param types and
/// output types.
class FunctionSignature {
 public:
  FunctionSignature(const std::string &base_name,
                    const DataTypeVector &param_types,
                    DataTypePtr ret_type)
      : base_name_(base_name),
        param_types_(param_types),
        ret_type_(ret_type) {
    DCHECK_GT(base_name.length(), 0);
    DCHECK_GT(param_types.size(), 0);
    for (auto it = param_types_.begin(); it != param_types_.end(); it++) {
      DCHECK(*it);
    }
    DCHECK(ret_type);
  }

  bool operator == (const FunctionSignature &other) const {
    if (param_types_.size() != other.param_types_.size() ||
        !DataTypeEquals(ret_type_, other.ret_type_) ||
        base_name_ != other.base_name_) {
      return false;
    }

    int idx = 0;
    for (auto it = param_types_.begin(); it != param_types_.end(); it++, idx++) {
      if (!DataTypeEquals(*it, other.param_types_[idx])) {
        return false;
      }
    }
    return true;
  }

  /// includes only the id of the datatype.
  std::size_t Hash() const {
    size_t result = 17;
    result = result * 31 + std::hash<std::string>()(base_name_);
    result = result * 31 + std::hash<int>()(ret_type_->id());
    for (auto it = param_types_.begin(); it != param_types_.end(); it++) {
      result = result * 31 + std::hash<int>()(it->get()->id());
    }
    return result;
  }

  DataTypePtr ret_type() const { return ret_type_; }

  std::string ToString() const {
    std::stringstream params;
    for (uint32_t i = 0; i < param_types_.size(); i++) {
      if (i > 0) {
        params << ", ";
      }

      params << param_types_[i]->ToString();
    }

    std::stringstream s;
    s << ret_type_->ToString() << " " << base_name_ << "(" << params.str() << ")";
    return s.str();
  }

 private:
  /*
   * TODO : for some of the types, this shouldn't match type specific data. eg. for
   * decimals, this shouldn't match precision/scale.
   */
  bool DataTypeEquals(const DataTypePtr left, const DataTypePtr right) const {
    return left->Equals(right);
  }

  std::string base_name_;
  DataTypeVector param_types_;
  DataTypePtr ret_type_;
};

} // namespace gandiva

#endif //GANDIVA_FUNCTION_SIGNATURE_H

