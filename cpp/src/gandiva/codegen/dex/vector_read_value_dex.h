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
#ifndef GANDIVA_DEX_VECTORREADVALUEDEX_H
#define GANDIVA_DEX_VECTORREADVALUEDEX_H

#include "CodeGen.pb.h"
#include "dex/dex.h"
#include "vector_descriptor.h"

namespace gandiva {

/*
 * value component of a ValueVector
 */
class VectorReadValueDex : public Dex {
 public:
  VectorReadValueDex(VectorDescriptorSharedPtr vector_descriptor)
      : vector_descriptor_(vector_descriptor) {}

  int DataIdx() const {
    return vector_descriptor_->data_idx();
  }

  int OffsetsIdx() const {
    return vector_descriptor_->offsets_idx();
  }

  const std::string &FieldName() const {
    return vector_descriptor_->name();
  }

  const common::MajorType *MajorType() const {
    return vector_descriptor_->type();
  }

  virtual LValueUniquePtr accept(DexVisitor &visitor) override {
    return visitor.visit(*this);
  }

 private:
  const VectorDescriptorSharedPtr vector_descriptor_;
};

} // namespace gandiva

#endif //GANDIVA_DEX_VECTORREADVALUEDEX_H
