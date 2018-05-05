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
#ifndef GANDIVA_VECTORDESCRIPTOR_H
#define GANDIVA_VECTORDESCRIPTOR_H

#include <string>
#include "CodeGen.pb.h"

namespace gandiva {

/*
 * Descriptor for a vector, as received from the executor.
 */
class VectorDescriptor {
 public:
  VectorDescriptor(const VectorExpr &vector_expr)
      : name_(vector_expr.name()),
        type_(&vector_expr.majortype()),
        validity_idx_(vector_expr.validityidx()),
        data_idx_(vector_expr.dataidx()),
        offsets_idx_(vector_expr.offsetsidx()) {}

  const std::string &name() const { return name_; }
  const common::MajorType *type() const { return type_; }
  int validity_idx() const { return validity_idx_; }
  int data_idx() const { return data_idx_; }
  int offsets_idx() const { return offsets_idx_; }

 private:
  std::string name_;
  const common::MajorType *type_;
  int validity_idx_;
  int data_idx_;
  int offsets_idx_;
};

typedef std::shared_ptr<VectorDescriptor> VectorDescriptorSharedPtr;

} // namespace gandiva

#endif //GANDIVA_VECTORDESCRIPTOR_H
