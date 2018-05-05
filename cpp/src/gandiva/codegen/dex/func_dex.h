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
#ifndef GANDIVA_DEX_FUNCDEX_H
#define GANDIVA_DEX_FUNCDEX_H

#include <vector>
#include "CodeGen.pb.h"
#include "dex.h"
#include "func_descriptor.h"
#include "native_function.h"
#include "value_validity_pair.h"

namespace gandiva {

/**
 * A function expression.
 */
class FuncDex : public Dex {
 public:
  FuncDex(FuncDescriptorSharedPtr func_descriptor,
          const NativeFunction *native_function,
          std::vector<ValueValidityPairSharedPtr> args)
      : func_descriptor_(func_descriptor),
        native_function_(native_function),
        args_(args) {}

  const FuncDescriptor *func_descriptor() const { return func_descriptor_.get(); }

  const NativeFunction *native_function() const { return native_function_; }

  const std::vector<ValueValidityPairSharedPtr> &args() const { return args_; }

 private:
  FuncDescriptorSharedPtr func_descriptor_;
  const NativeFunction *native_function_;
  std::vector<ValueValidityPairSharedPtr> args_;
};

} // namespace gandiva

#endif //GANDIVA_DEX_FUNCDEX_H
