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
#ifndef GANDIVA_EXPR_ANNOTATOR_H
#define GANDIVA_EXPR_ANNOTATOR_H

#include <list>
#include <unordered_map>
#include "arrow.h"
#include "gandiva_fwd.h"

namespace gandiva {

/// \brief The buffers corresponding to one batch of records, used for
/// expression evaluation.
class EvalBatch {
 public:
  EvalBatch(int num_buffers)
    : num_buffers_(num_buffers) {
    buffers_ = new uint8_t *[num_buffers];
  }

  ~EvalBatch() {
    delete buffers_;
  }

  uint8_t **buffers() { return buffers_; }

  int num_buffers() { return num_buffers_; }

  void SetBufferAtIdx(int idx, uint8_t *buffer) {
    DCHECK(idx <= num_buffers_);
    buffers_[idx] = buffer;
  }

 private:
  uint8_t **buffers_;
  int num_buffers_;
};

/// \brief annotate the arrow fields in an expression, and use that
/// to convert the incoming arrow-format row batch to an EvalBatch.
class Annotator {
 public:
   Annotator()
     : buffer_count_(0) {}

   ~Annotator() {
     in_name_to_desc_.clear();
     out_descs_.clear();
   }

   /// Add an annotated field descriptor for a field in the input schema.
   /// If the field already exists, returns that instead of creating again.
   FieldDescriptorSharedPtr CheckAndAddInputFieldDescriptor(FieldSharedPtr field);

   /// Add an annotated field descriptor for an output field.
   FieldDescriptorSharedPtr AddOutputFieldDescriptor(FieldSharedPtr field);

   /*
    * Prepare an eval batch for the incoming record batch.
    */
   EvalBatchSharedPtr PrepareEvalBatch(RecordBatchSharedPtr batch, arrow::ArrayVector out_arrays);

 private:
   FieldDescriptorSharedPtr MakeDesc(FieldSharedPtr field);
   void PrepareBuffersForField(FieldDescriptorSharedPtr desc,
                               ArraySharedPtr array,
                               EvalBatchSharedPtr eval_batch);
   int buffer_count_;

   std::unordered_map<std::string, FieldDescriptorSharedPtr> in_name_to_desc_;
   std::vector<FieldDescriptorSharedPtr> out_descs_;
};

} // namespace gandiva

#endif //GANDIVA_EXPR_ANNOTATOR_H
