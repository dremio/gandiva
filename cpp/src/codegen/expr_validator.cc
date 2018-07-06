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

#include <sstream>
#include <string>
#include <vector>

#include "codegen/expr_validator.h"

namespace gandiva {

Status ExprValidator::Validate(const ExpressionPtr &expr) {
  if (expr == nullptr) {
    return Status::ExpressionValidationError("Expression cannot be null.");
  }
  Node &root = *expr->root();
  Status status = root.Accept(*this);
  if (!status.ok()) {
    return status;
  }
  // validate return type matches
  // no need to check if type is supported
  // since root type has been validated.
  if (!root.return_type()->Equals(*expr->result()->type())) {
    std::stringstream ss;
    ss << "Return type of root node " << root.return_type()->name()
       << " does not match that of expression " << *expr->result()->type();
    return Status::ExpressionValidationError(ss.str());
  }
  return Status::OK();
}

Status ExprValidator::Visit(const FieldNode &node) {
  auto llvm_type = types_.IRType(node.return_type()->id());
  if (llvm_type == nullptr) {
    std::stringstream ss;
    ss << "Field " << node.field()->name() << " has unsupported data type "
       << node.return_type()->name();
    return Status::ExpressionValidationError(ss.str());
  }

  auto field_in_schema_entry = field_map_.find(node.field()->name());

  // validate that field is in schema.
  if (field_in_schema_entry == field_map_.end()) {
    std::stringstream ss;
    ss << "Field " << node.field()->name() << " not in schema.";
    return Status::ExpressionValidationError(ss.str());
  }

  FieldPtr field_in_schema = field_in_schema_entry->second;
  // validate that field matches the definition in schema.
  if (!field_in_schema->Equals(node.field())) {
    std::stringstream ss;
    ss << "Field definition in schema " << field_in_schema->ToString()
       << " different from field in expression " << node.field()->ToString();
    return Status::ExpressionValidationError(ss.str());
  }
  return Status::OK();
}

Status ExprValidator::Visit(const FunctionNode &node) {
  auto desc = node.descriptor();
  FunctionSignature signature(desc->name(), desc->params(), desc->return_type());
  const NativeFunction *native_function = registry_.LookupSignature(signature);
  if (native_function == nullptr) {
    std::stringstream ss;
    ss << "Function " << signature.ToString() << " not supported yet. ";
    return Status::ExpressionValidationError(ss.str());
  }

  for (auto &child : node.children()) {
    Status status = child->Accept(*this);
    GANDIVA_RETURN_NOT_OK(status);
  }
  return Status::OK();
}

Status ExprValidator::Visit(const IfNode &node) {
  Status status = node.condition()->Accept(*this);
  GANDIVA_RETURN_NOT_OK(status);
  status = node.then_node()->Accept(*this);
  GANDIVA_RETURN_NOT_OK(status);
  status = node.else_node()->Accept(*this);
  GANDIVA_RETURN_NOT_OK(status);

  auto if_node_ret_type = node.return_type();
  auto then_node_ret_type = node.then_node()->return_type();
  auto else_node_ret_type = node.else_node()->return_type();

  if (if_node_ret_type != then_node_ret_type) {
    std::stringstream ss;
    ss << "Return type of if " << *if_node_ret_type << " and then "
       << then_node_ret_type->name() << " not matching.";
    return Status::ExpressionValidationError(ss.str());
  }

  if (if_node_ret_type != else_node_ret_type) {
    std::stringstream ss;
    ss << "Return type of if " << *if_node_ret_type << " and else "
       << else_node_ret_type->name() << " not matching.";
    return Status::ExpressionValidationError(ss.str());
  }

  return Status::OK();
}

Status ExprValidator::Visit(const LiteralNode &node) {
  auto llvm_type = types_.IRType(node.return_type()->id());
  if (llvm_type == nullptr) {
    std::stringstream ss;
    ss << "Value " << node.holder() << " has unsupported data type "
       << node.return_type()->name();
    return Status::ExpressionValidationError(ss.str());
  }
  return Status::OK();
}

Status ExprValidator::Visit(const BooleanNode &node) {
  Status status;

  if (node.children().size() < 2) {
    std::stringstream ss;
    ss << "Boolean expression has " << node.children().size()
       << " children, expected atleast two";
    return Status::ExpressionValidationError(ss.str());
  }

  for (auto &child : node.children()) {
    if (child->return_type() != arrow::boolean()) {
      std::stringstream ss;
      ss << "Boolean expression has a child with return type "
         << child->return_type()->name() << ", expected return type boolean";
      return Status::ExpressionValidationError(ss.str());
    }

    status = child->Accept(*this);
    GANDIVA_RETURN_NOT_OK(status);
  }
  return Status::OK();
}

}  // namespace gandiva
