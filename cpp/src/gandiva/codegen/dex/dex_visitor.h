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
#ifndef GANDIVA_DEX_DEXVISITOR_H
#define GANDIVA_DEX_DEXVISITOR_H

#include "lvalue.h"

namespace gandiva {

class VectorReadValidityDex;
class VectorReadValueDex;
class LiteralDex;
class NonNullableFuncDex;
class NullableNeverFuncDex;

/*
 * Visitor for decomposed expression.
 */
class DexVisitor {
 public:
  virtual LValueUniquePtr visit(const VectorReadValidityDex &dex) = 0;
  virtual LValueUniquePtr visit(const VectorReadValueDex &dex) = 0;
  virtual LValueUniquePtr visit(const LiteralDex &dex) = 0;
  virtual LValueUniquePtr visit(const NonNullableFuncDex &dex) = 0;
  virtual LValueUniquePtr visit(const NullableNeverFuncDex &dex) = 0;
};

} // namespace gandiva

#endif //GANDIVA_DEX_DEXVISITOR_H
