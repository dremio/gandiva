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
#ifndef GANDIVA_CEX_VECTORREADCEX_H
#define GANDIVA_CEX_VECTORREADCEX_H

#include "CodeGen.pb.h"
#include "cex/cex.h"
#include "dex/vector_read_value_dex.h"
#include "dex/vector_read_validity_dex.h"

namespace gandiva {

class VectorDescriptor;

/*
 * Value Vector based Composed expression
 */
class VectorReadCex : public Cex {
 public:
  VectorReadCex(VectorDescriptorSharedPtr vector_descriptor)
      : vector_descriptor_(vector_descriptor) {}

  virtual ValueValidityPair *Decompose() override {
    return new ValueValidityPair(DexSharedPtr(new VectorReadValidityDex(vector_descriptor_)),
                                 DexSharedPtr(new VectorReadValueDex(vector_descriptor_)));
  }

 private:
  VectorDescriptorSharedPtr vector_descriptor_;
};

} // namespace gandiva

#endif //GANDIVA_CEX_VECTORREADCEX_H
