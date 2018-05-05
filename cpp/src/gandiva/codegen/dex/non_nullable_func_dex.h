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
#ifndef GANDIVA_DEX_NONNULLABLEFUNCDEX_H
#define GANDIVA_DEX_NONNULLABLEFUNCDEX_H

#include "func_dex.h"

namespace gandiva {

/**
 * A function expression that only deals with non-null inputs.
 */
class NonNullableFuncDex : public FuncDex {
 public:
  NonNullableFuncDex(FuncDescriptorSharedPtr func_descriptor,
                     const NativeFunction *native_function,
                     std::vector<ValueValidityPairSharedPtr> args)
      : FuncDex(func_descriptor, native_function, args) {}

  virtual LValueUniquePtr accept(DexVisitor &visitor) override {
    return visitor.visit(*this);
  }
};

} // namespace gandiva

#endif //GANDIVA_DEX_NONNULLABLEFUNCDEX_H
