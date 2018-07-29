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
#ifndef GANDIVA_CONDITION_H
#define GANDIVA_CONDITION_H

#include "gandiva/arrow.h"
#include "gandiva/expression.h"
#include "gandiva/gandiva_aliases.h"

namespace gandiva {

/// \brief A condition expression.
class Condition : public Expression {
 public:
  Condition(const NodePtr root)
      : Expression(root, std::make_shared<arrow::Field>("cond", arrow::boolean())) {}

  virtual ~Condition() = default;
};

}  // namespace gandiva

#endif  // GANDIVA_CONDITION_H
