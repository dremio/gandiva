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
#ifndef GANDIVA_VALUEVALIDITYPAIR_H
#define GANDIVA_VALUEVALIDITYPAIR_H

#include <vector>
#include "gandiva_fwd.h"

namespace gandiva {

/*
 * Pair of vector/validities generated after decomposing a composed expression.
 */
class ValueValidityPair {
 public:
  ValueValidityPair(std::vector<DexSharedPtr> validity_exprs, DexSharedPtr value_expr)
      : validity_exprs_(validity_exprs),
        value_expr_(value_expr) {}

  ValueValidityPair(DexSharedPtr validity_expr, DexSharedPtr value_expr)
      : value_expr_(value_expr) {
    validity_exprs_.push_back(validity_expr);
  }

  ValueValidityPair(DexSharedPtr value_expr)
      : value_expr_(value_expr) {}

  const std::vector<DexSharedPtr> &validity_exprs() { return validity_exprs_; }

  DexSharedPtr value_expr() { return value_expr_; }

 private:
  std::vector<DexSharedPtr> validity_exprs_;
  DexSharedPtr value_expr_;
};

} // namespace gandiva

#endif //GANDIVA_VALUEVALIDITYPAIR_H
