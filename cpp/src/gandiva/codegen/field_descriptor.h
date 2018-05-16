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
#ifndef GANDIVA_FIELDDESCRIPTOR_H
#define GANDIVA_FIELDDESCRIPTOR_H

#include <string>
#include "common/arrow.h"

namespace gandiva {

/*
 * Descriptor for a vector, as received from the executor.
 */
class FieldDescriptor {
 public:
  static const int kInvalidIdx = -1;

  FieldDescriptor(FieldSharedPtr field,
                  int data_idx,
                  int validity_idx = kInvalidIdx,
                  int offsets_idx = kInvalidIdx)
      : field_(field),
        data_idx_(data_idx),
        validity_idx_(validity_idx),
        offsets_idx_(offsets_idx) {}

  /* Index of validity array in the array-of-pointers argument */
  int validity_idx() const { return validity_idx_; }

  /* Index of data array in the array-of-pointers argument */
  int data_idx() const { return data_idx_; }

  /* Index of offsets array in the array-of-pointers argument */
  int offsets_idx() const { return offsets_idx_; }

  const FieldSharedPtr field() const { return field_; }

  const std::string &Name() const { return field_->name(); }
  const DataTypeSharedPtr Type() const { return field_->type(); }

 private:
  FieldSharedPtr field_;
  int data_idx_;
  int validity_idx_;
  int offsets_idx_;
};

} // namespace gandiva

#endif //GANDIVA_FIELDDESCRIPTOR_H
