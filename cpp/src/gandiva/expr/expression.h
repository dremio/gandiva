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
#ifndef GANDIVA_EXPR_EXPRESSION_H
#define GANDIVA_EXPR_EXPRESSION_H

#include "common/gandiva_fwd.h"
#include "expr/node.h"

namespace gandiva {

class Expression {
 public:
  Expression(const NodeSharedPtr root, const FieldSharedPtr result)
    : root_(root), result_(result) {}

  NodeSharedPtr root() { return root_; }

  FieldSharedPtr result() { return result_; }

  ValueValidityPairSharedPtr Decompose(const FunctionRegistry &registry,
                                       Annotator &annotator) {
    return root_->Decompose(registry, annotator);
  }

 private:
  const NodeSharedPtr root_;
  const FieldSharedPtr result_;
};

} // namespace gandiva

#endif // GANDIVA_EXPR_EXPRESSION_H
