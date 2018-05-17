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

#include "expr/tree_expr_builder.h"

namespace gandiva {

NodeSharedPtr TreeExprBuilder::MakeField(FieldSharedPtr field) {
  return NodeSharedPtr(new FieldNode(field));
}

NodeSharedPtr TreeExprBuilder::MakeBinaryFunction(const std::string &function,
                                                  NodeSharedPtr left,
                                                  NodeSharedPtr right,
                                                  DataTypeSharedPtr result) {
  return FunctionNode::CreateFunction(function, {left, right}, result);
}

NodeSharedPtr TreeExprBuilder::MakeUnaryFunction(const std::string &function,
                                                 NodeSharedPtr param,
                                                 DataTypeSharedPtr result) {
  return FunctionNode::CreateFunction(function, {param}, result);
}

ExpressionSharedPtr TreeExprBuilder::MakeExpression(NodeSharedPtr root_node,
                                                    FieldSharedPtr result_field) {
  return ExpressionSharedPtr(new Expression(root_node, result_field));
}

ExpressionSharedPtr TreeExprBuilder::MakeExpression(const std::string &function,
                                                    std::vector<FieldSharedPtr> in_fields,
                                                    FieldSharedPtr out_field) {
  std::vector<NodeSharedPtr> field_nodes;
  for (auto it = in_fields.begin(); it != in_fields.end(); ++it) {
    auto node = MakeField(*it);
    field_nodes.push_back(node);
  }
  auto func_node = FunctionNode::CreateFunction(function, field_nodes, out_field->type());
  return MakeExpression(func_node, out_field);
}

} // namespace gandiva
