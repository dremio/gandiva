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
#ifndef GANDIVA_EXPR_TREE_BUILDER_H
#define GANDIVA_EXPR_TREE_BUILDER_H

#include <string>
#include <vector>
#include "expr/node.h"
#include "expr/expression.h"

namespace gandiva {

/// \brief Tree Builder for a nested expression.
class TreeExprBuilder {
 public:
  /// \brief create a node on arrow field.
  static NodeSharedPtr MakeField(FieldSharedPtr field);

  /// \brief create a node with a binary function.
  static NodeSharedPtr MakeBinaryFunction(const std::string &function,
                                          NodeSharedPtr left,
                                          NodeSharedPtr right,
                                          DataTypeSharedPtr result);

  /// \brief create a node with a unary function.
  static NodeSharedPtr MakeUnaryFunction(const std::string &function,
                                         NodeSharedPtr param,
                                         DataTypeSharedPtr result);

  /// \brief create an expression with the specified root_node, and the
  /// result written to result_field.
  static ExpressionSharedPtr MakeExpression(NodeSharedPtr root_node,
                                            FieldSharedPtr result_field);

  /// \brief convenience function for simple function expressions.
  static ExpressionSharedPtr MakeExpression(const std::string &function,
                                            const std::vector<FieldSharedPtr> &in_fields,
                                            FieldSharedPtr out_field);
};

} // namespace gandiva

#endif //GANDIVA_EXPR_TREE_BUILDER_H
