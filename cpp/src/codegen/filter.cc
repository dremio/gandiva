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

#include "gandiva/filter.h"

#include <memory>
#include <utility>
#include <vector>

#include "codegen/bitmap_accumulator.h"
#include "codegen/expr_validator.h"
#include "codegen/llvm_generator.h"
#include "gandiva/condition.h"
#include "gandiva/status.h"

namespace gandiva {

Filter::Filter(std::unique_ptr<LLVMGenerator> llvm_generator, SchemaPtr schema,
               arrow::MemoryPool *pool, std::shared_ptr<Configuration> configuration)
    : llvm_generator_(std::move(llvm_generator)),
      schema_(schema),
      pool_(pool),
      configuration_(configuration) {}

Status Filter::Make(SchemaPtr schema, ConditionPtr cond, arrow::MemoryPool *pool,
                    std::shared_ptr<Filter> *filter) {
  return Filter::Make(schema, cond, pool, ConfigurationBuilder::DefaultConfiguration(),
                      filter);
}

Status Filter::Make(SchemaPtr schema, ConditionPtr cond, arrow::MemoryPool *pool,
                    std::shared_ptr<Configuration> configuration,
                    std::shared_ptr<Filter> *filter) {
  GANDIVA_RETURN_FAILURE_IF_FALSE(schema != nullptr,
                                  Status::Invalid("schema cannot be null"));
  GANDIVA_RETURN_FAILURE_IF_FALSE(cond != nullptr,
                                  Status::Invalid("condition cannot be null"));
  GANDIVA_RETURN_FAILURE_IF_FALSE(configuration != nullptr,
                                  Status::Invalid("configuration cannot be null"));
  // Build LLVM generator, and generate code for the specified expression
  std::unique_ptr<LLVMGenerator> llvm_gen;
  Status status = LLVMGenerator::Make(configuration, &llvm_gen);
  GANDIVA_RETURN_NOT_OK(status);

  // Run the validation on the expression.
  // Return if the expression is invalid since we will not be able to process further.
  ExprValidator expr_validator(llvm_gen->types(), schema);
  status = expr_validator.Validate(cond);
  GANDIVA_RETURN_NOT_OK(status);

  status = llvm_gen->Build({cond});
  GANDIVA_RETURN_NOT_OK(status);

  // Instantiate the filter with the completely built llvm generator
  *filter = std::shared_ptr<Filter>(
      new Filter(std::move(llvm_gen), schema, pool, configuration));
  return Status::OK();
}

Status Filter::Evaluate(const arrow::RecordBatch &batch, ArrayDataPtr out_selection_data,
                        std::shared_ptr<arrow::Array> *out_selection) {
  Status status = ValidateEvaluateArgsCommon(batch);
  GANDIVA_RETURN_NOT_OK(status);

  if (out_selection_data == nullptr) {
    return Status::Invalid("out_selection_buffer is null.");
  }

  status = ValidateSelectionVectorCapacity(*out_selection_data, batch.num_rows());
  GANDIVA_RETURN_NOT_OK(status);

  return EvaluateCommon(batch, out_selection_data, out_selection);
}

Status Filter::Evaluate(const arrow::RecordBatch &batch,
                        std::shared_ptr<arrow::Array> *out_selection) {
  Status status = ValidateEvaluateArgsCommon(batch);
  GANDIVA_RETURN_NOT_OK(status);

  if (out_selection == nullptr) {
    return Status::Invalid("out_selection must be non-null.");
  }

  if (pool_ == nullptr) {
    return Status::Invalid("memory pool must be non-null.");
  }

  // Allocate the array_data for the selection vector.
  ArrayDataPtr array_data;
  status = AllocSelectionVectorData(batch.num_rows(), &array_data);
  GANDIVA_RETURN_NOT_OK(status);

  return EvaluateCommon(batch, array_data, out_selection);
}

Status Filter::EvaluateCommon(const arrow::RecordBatch &batch,
                              ArrayDataPtr out_selection_data,
                              std::shared_ptr<arrow::Array> *out_selection) {
  // Allocate three local_bitmaps (one for output, one for validity, one to compute the
  // intersection).
  LocalBitMapsHolder bitmaps(batch.num_rows(), 3 /*local_bitmaps*/);
  int bitmap_size = bitmaps.GetLocalBitMapSize();

  auto validity = std::make_shared<arrow::Buffer>(bitmaps.GetLocalBitMap(0), bitmap_size);
  auto value = std::make_shared<arrow::Buffer>(bitmaps.GetLocalBitMap(1), bitmap_size);
  auto array_data =
      arrow::ArrayData::Make(arrow::boolean(), batch.num_rows(), {validity, value});

  // Execute the expression(s).
  auto status = llvm_generator_->Execute(batch, {array_data});
  GANDIVA_RETURN_NOT_OK(status);

  // Compute the intersection of the value and validity.
  auto result = bitmaps.GetLocalBitMap(2);
  BitMapAccumulator::IntersectBitMaps(
      result, {bitmaps.GetLocalBitMap(0), bitmaps.GetLocalBitMap((1))}, bitmap_size);

  *out_selection =
      BitMapToSelectionVector(result, bitmap_size, batch.num_rows(), out_selection_data);
  return Status::OK();
}

Status Filter::ValidateSelectionVectorCapacity(const arrow::ArrayData &selection_data,
                                               int num_records) {
  // verify that there is only one buffer (no validity).
  if (selection_data.buffers.size() != 2) {
    std::stringstream ss;
    ss << "number of buffers in selection_data is " << selection_data.buffers.size()
       << ", must be 2.";
    return Status::Invalid(ss.str());
  }

  if (selection_data.type != arrow::int16()) {
    std::stringstream ss;
    ss << "selection_data is of type " << selection_data.type->ToString()
       << ", must be Int16Type.";
    return Status::Invalid(ss.str());
  }
  // verify size of data buffer.
  auto min_data_len =
      arrow::BitUtil::BytesForBits(num_records * arrow::Int16Type().bit_width());
  int64_t data_len = selection_data.buffers.at(1)->capacity();
  if (data_len < min_data_len) {
    std::stringstream ss;
    ss << "data buffer for selection_data has size " << data_len
       << ", must have minimum size " << min_data_len;
    return Status::Invalid(ss.str());
  }
  return Status::OK();
}

Status Filter::ValidateEvaluateArgsCommon(const arrow::RecordBatch &batch) {
  if (!batch.schema()->Equals(*schema_)) {
    return Status::Invalid("Schema in RecordBatch must match the schema in Make()");
  }
  if (batch.num_rows() == 0) {
    return Status::Invalid("RecordBatch must be non-empty.");
  }
  if (batch.num_rows() > INT16_MAX) {
    return Status::Invalid("RecordBatch size must be <= INT16_MAX.");
  }
  return Status::OK();
}

Status Filter::AllocSelectionVectorData(int num_records, ArrayDataPtr *selection_data) {
  auto data = std::make_shared<arrow::PoolBuffer>(pool_);
  auto data_len =
      arrow::BitUtil::BytesForBits(num_records * arrow::Int16Type().bit_width());
  auto status = data->Resize(data_len);
  GANDIVA_RETURN_ARROW_NOT_OK(status);

  *selection_data = arrow::ArrayData::Make(arrow::int16(), num_records, {nullptr, data});
  return Status::OK();
}

std::shared_ptr<arrow::Array> Filter::BitMapToSelectionVector(
    const uint8_t *bitmap, int bitmap_size, int num_rows, ArrayDataPtr selection_data) {
  int16_t *selection_buf =
      reinterpret_cast<int16_t *>(selection_data->buffers.at(1)->mutable_data());
  int selection_count = 0;

  // jump  8-bytes at a time, add the index corresponding to each valid bit to the
  // the selection vector.
  const uint64_t *bitmap_64 = reinterpret_cast<const uint64_t *>(bitmap);
  for (int i = 0; i < bitmap_size; i += 8) {
    uint64_t current_word = bitmap_64[i];

    while (current_word != 0) {
      uint64_t highest_only = current_word & -current_word;
      int pos_in_word = __builtin_ctzl(highest_only);

      int pos_in_bitmap = i * 64 + pos_in_word;
      if (pos_in_bitmap >= num_rows) {
        // the bitmap may be slighly larger for alignment/padding.
        break;
      }

      selection_buf[selection_count] = pos_in_bitmap;
      ++selection_count;

      current_word ^= highest_only;
    }
  }

  return arrow::MakeArray(selection_data)->Slice(0, selection_count);
}

}  // namespace gandiva
