// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "gandiva/selection_vector.h"

#include <memory>
#include <utility>
#include <vector>

#include "gandiva/status.h"

namespace gandiva {

template <typename C_TYPE, typename A_TYPE>
Status SelectionVector<C_TYPE, A_TYPE>::AllocateBuffer(
    int max_slots, arrow::MemoryPool *pool,
    std::shared_ptr<arrow::MutableBuffer> *buffer) {
  auto abuffer = std::make_shared<arrow::PoolBuffer>(pool);
  auto buffer_len = max_slots * sizeof(C_TYPE);
  auto status = abuffer->Resize(buffer_len);
  GANDIVA_RETURN_ARROW_NOT_OK(status);

  *buffer = abuffer;
  return Status::OK();
}

template <typename C_TYPE, typename A_TYPE>
Status SelectionVector<C_TYPE, A_TYPE>::ValidateBuffer(
    int max_slots, std::shared_ptr<arrow::MutableBuffer> buffer) {
  // verify size of buffer.
  auto min_len = max_slots * sizeof(C_TYPE);
  if (buffer->size() < min_len) {
    std::stringstream ss;
    ss << "buffer for selection_data has size " << buffer->size()
       << ", must have minimum size " << min_len;
    return Status::Invalid(ss.str());
  }
  return Status::OK();
}

Status SelectionVectorInt16::Make(
    int max_slots, std::shared_ptr<arrow::MutableBuffer> buffer,
    std::shared_ptr<SelectionVectorInt16> *selection_vector) {
  auto status = ValidateBuffer(max_slots, buffer);
  if (status.ok()) {
    *selection_vector = std::make_shared<SelectionVectorInt16>(max_slots, buffer);
  }
  return status;
}

Status SelectionVectorInt16::Make(
    int max_slots, arrow::MemoryPool *pool,
    std::shared_ptr<SelectionVectorInt16> *selection_vector) {
  std::shared_ptr<arrow::MutableBuffer> buffer;
  auto status = AllocateBuffer(max_slots, pool, &buffer);
  if (status.ok()) {
    *selection_vector = std::make_shared<SelectionVectorInt16>(max_slots, buffer);
  }
  return status;
}

Status SelectionVectorInt32::Make(
    int max_slots, std::shared_ptr<arrow::MutableBuffer> buffer,
    std::shared_ptr<SelectionVectorInt32> *selection_vector) {
  auto status = ValidateBuffer(max_slots, buffer);
  if (status.ok()) {
    *selection_vector = std::make_shared<SelectionVectorInt32>(max_slots, buffer);
  }
  return status;
}

Status SelectionVectorInt32::Make(
    int max_slots, arrow::MemoryPool *pool,
    std::shared_ptr<SelectionVectorInt32> *selection_vector) {
  std::shared_ptr<arrow::MutableBuffer> buffer;
  auto status = AllocateBuffer(max_slots, pool, &buffer);
  if (status.ok()) {
    *selection_vector = std::make_shared<SelectionVectorInt32>(max_slots, buffer);
  }
  return status;
}

}  // namespace gandiva
