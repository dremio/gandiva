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
#ifndef GANDIVA_CEX_FUNCCEX_H
#define GANDIVA_CEX_FUNCCEX_H

#include <vector>
#include "CodeGen.pb.h"
#include "cex.h"
#include "func_descriptor.h"

namespace gandiva {

/*
 * Function based Composed expression
 */
class FuncCex : public Cex {
 public:
  FuncCex(FuncDescriptorSharedPtr desc, std::vector<Cex *> children)
      : func_descriptor_(desc),
        children_(children) {}

  ~FuncCex() {
    for (auto it = children_.begin(); it != children_.end(); ++it) {
      delete *it;
    }
  }

  virtual ValueValidityPair *Decompose() override;

 private:
  FuncDescriptorSharedPtr func_descriptor_;
  std::vector<Cex *> children_;
};

} // namespace gandiva

#endif //GANDIVA_CEX_FUNCCEX_H
