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
#ifndef GANDIVA_FUNCDESCRIPTOR_H
#define GANDIVA_FUNCDESCRIPTOR_H

#include <string>
#include <vector>
#include "common/arrow.h"

namespace gandiva {

/*
 * Descriptor for a function, as received from the executor.
 */
class FuncDescriptor {
 public:
  FuncDescriptor(const std::string &name,
                 const std::vector<DataTypeSharedPtr> &params,
                 const DataTypeSharedPtr return_type)
      : name_(name),
        params_(params),
        return_type_(return_type)
  {}

  const std::string &name() const { return name_;}

  std::vector<DataTypeSharedPtr> &params() { return params_; }

  const DataTypeSharedPtr return_type() const { return return_type_; }

 private:
  std::string name_;
  std::vector<DataTypeSharedPtr> params_;
  const DataTypeSharedPtr return_type_;
};

} // namespace gandiva

#endif //GANDIVA_FUNCDESCRIPTOR_H
