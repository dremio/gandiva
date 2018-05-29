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

#include "gandiva/tree_expr_builder.h"

namespace gandiva {

NodePtr TreeExprBuilder::MakeField(FieldPtr field) {
  return NodePtr(new FieldNode(field));
}

NodePtr TreeExprBuilder::MakeBinaryFunction(const std::string &function,
                                            NodePtr left,
                                            NodePtr right,
                                            DataTypePtr result) {
  return FunctionNode::CreateFunction(function, {left, right}, result);
}

NodePtr TreeExprBuilder::MakeUnaryFunction(const std::string &function,
                                           NodePtr param,
                                           DataTypePtr result) {
  return FunctionNode::CreateFunction(function, {param}, result);
}

ExpressionPtr TreeExprBuilder::MakeExpression(NodePtr root_node,
                                              FieldPtr result_field) {
  return ExpressionPtr(new Expression(root_node, result_field));
}

ExpressionPtr TreeExprBuilder::MakeExpression(
    const std::string &function,
    const FieldVector &in_fields,
    FieldPtr out_field) {

  std::vector<NodePtr> field_nodes;
  for (auto it = in_fields.begin(); it != in_fields.end(); ++it) {
    auto node = MakeField(*it);
    field_nodes.push_back(node);
  }
  auto func_node = FunctionNode::CreateFunction(function, field_nodes, out_field->type());
  return MakeExpression(func_node, out_field);
}

} // namespace gandiva
