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

#include <memory>
#include <string>
#include <vector>
#include "codegen/dex.h"
#include "codegen/function_registry.h"
#include "codegen/function_signature.h"
#include "expr/annotator.h"
#include "expr/node.h"

namespace gandiva {

ValueValidityPairSharedPtr FieldNode::Decompose(Annotator *annotator) {
  FieldDescriptorSharedPtr desc = annotator->CheckAndAddInputFieldDescriptor(field_);

  DexSharedPtr validity_dex = std::make_shared<VectorReadValidityDex>(desc);
  DexSharedPtr value_dex = std::make_shared<VectorReadValueDex>(desc);
  return std::make_shared<ValueValidityPair>(validity_dex, value_dex);
}

ValueValidityPairSharedPtr FunctionNode::Decompose(Annotator *annotator) {
  FunctionSignature signature(desc_->name(),
                              desc_->params(),
                              desc_->return_type());
  const NativeFunction *native_function = FunctionRegistry::LookupSignature(signature);
  DCHECK(native_function);

  // decompose the children.
  std::vector<ValueValidityPairSharedPtr> args;
  for (auto it = children_.begin(); it != children_.end(); ++it) {
    ValueValidityPairSharedPtr child = (*it)->Decompose(annotator);
    args.push_back(child);
  }

  if (native_function->result_nullable_type() == RESULT_NULL_IF_NULL) {
    // NULL_IF_NULL functions are decomposable, merge the validity bits of the children.

    std::vector<DexSharedPtr> merged_validity;

    for (auto it = args.begin(); it != args.end(); ++it) {
      // Merge the validity_expressions of the children to build a combined validity
      // expression.
      ValueValidityPairSharedPtr child = *it;
      merged_validity.insert(merged_validity.end(),
                             child->validity_exprs().begin(),
                             child->validity_exprs().end());
    }

    auto value_dex = std::make_shared<NonNullableFuncDex>(desc_, native_function, args);
    return std::make_shared<ValueValidityPair>(merged_validity, value_dex);
  } else if (native_function->result_nullable_type() == RESULT_NULL_NEVER) {
      // These functions always output valid results. So, no validity dex.
    auto value_dex = std::make_shared<NullableNeverFuncDex>(desc_, native_function, args);
    return std::make_shared<ValueValidityPair>(value_dex);
  } else {
    // TODO
    DCHECK(0);
    return NULL;
  }
}

NodeSharedPtr FunctionNode::CreateFunction(const std::string &name,
                                           const std::vector<NodeSharedPtr> children,
                                           DataTypeSharedPtr retType) {
  std::vector<DataTypeSharedPtr> paramTypes;
  for (auto it = children.begin(); it != children.end(); ++it) {
    auto arg = (*it)->return_type();
    paramTypes.push_back(arg);
  }

  auto func_desc = FuncDescriptorSharedPtr(new FuncDescriptor(name, paramTypes, retType));
  return NodeSharedPtr(new FunctionNode(func_desc, children, retType));
}

} // namespace gandiva
