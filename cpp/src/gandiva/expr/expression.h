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

#include "gandiva_fwd.h"
#include "node.h"

namespace gandiva {

class Expression {
  public:
    Expression(const NodeSharedPtr node, const FieldSharedPtr field)
      : node_(node), field_(field) {}

    NodeSharedPtr node() { return node_; }

    FieldSharedPtr field() { return field_; }

    virtual ValueValidityPairSharedPtr Decompose(Annotator *annotator) {
      // return whatever node does
      return node_->Decompose(annotator);
    }

  private:
    const NodeSharedPtr node_;
    const FieldSharedPtr field_;
};

} // namespace gandiva

#endif // GANDIVA_EXPR_EXPRESSION_H
