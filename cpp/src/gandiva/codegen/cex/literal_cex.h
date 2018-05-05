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
#ifndef GANDIVA_CEX_LITERALCEX_H
#define GANDIVA_CEX_LITERALCEX_H

#include "CodeGen.pb.h"
#include "cex/cex.h"
#include "dex/literal_dex.h"

namespace gandiva {

/*
 * Composed expression for a literal.
 */
class LiteralCex : public Cex {
 public:
  LiteralCex(const Literal *literal)
      : literal_(literal) {}

  virtual ValueValidityPair *Decompose() override {
    // always valid, so no validity vector.
    return new ValueValidityPair(DexSharedPtr(new LiteralDex(literal_)));
  }

 private:
  const Literal *literal_;
};

} // namespace gandiva

#endif //GANDIVA_CEX_LITERALCEX_H
