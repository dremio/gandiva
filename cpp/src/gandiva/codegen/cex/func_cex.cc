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
#include "func_cex.h"
#include "dex/non_nullable_func_dex.h"
#include "dex/nullable_never_func_dex.h"
#include "func_descriptor.h"
#include "function_registry.h"
#include "function_signature.h"
#include "native_function.h"

namespace gandiva {

ValueValidityPair *FuncCex::Decompose() {
  FunctionSignature signature(func_descriptor_->name(),
                              func_descriptor_->params(),
                              func_descriptor_->return_type());
  const NativeFunction *native_function = FunctionRegistry::LookupSignature(&signature);
  assert(native_function);

  // decompose the children.
  std::vector<ValueValidityPairSharedPtr> args;
  for (auto it = children_.begin(); it != children_.end(); ++it) {
    ValueValidityPairSharedPtr child = ValueValidityPairSharedPtr((*it)->Decompose());
    args.push_back(child);
  }

  if (native_function->result_nullable_type() == RESULT_NULL_IF_NULL) {
    // NULL_IF_NULL functions are decomposable, merge the validity bits of the children.

    std::vector<DexSharedPtr> merged_validity;

    for (auto it = args.begin(); it != args.end(); ++it) {
      // Merge the validity_expressions of the children to build a combined validity expression.
      ValueValidityPairSharedPtr child = *it;
      merged_validity.insert(merged_validity.end(),
                             child->validity_exprs().begin(),
                             child->validity_exprs().end());
    }
    return new ValueValidityPair(merged_validity,
                                 DexSharedPtr(new NonNullableFuncDex(func_descriptor_, native_function, args)));
  } else if (native_function->result_nullable_type() == RESULT_NULL_NEVER) {
      // These functions always output valid results.
    return new ValueValidityPair(DexSharedPtr(new NullableNeverFuncDex(func_descriptor_, native_function, args)));
  } else {
    // TODO
    assert(0);
  }
}

} // namespace gandiva
