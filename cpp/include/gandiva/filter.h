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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either condess or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef GANDIVA_EXPR_FILTER_H
#define GANDIVA_EXPR_FILTER_H

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gandiva/arrow.h"
#include "gandiva/condition.h"
#include "gandiva/configuration.h"
#include "gandiva/status.h"

namespace gandiva {

class LLVMGenerator;

/// \brief filter records based on an condition.
///
/// A filter is built for a specific schema and condition. Once the filter is built, it
/// can be used to evaluate many row batches.
class Filter {
 public:
  /// Build a default filter for the given schema and condition.
  ///
  /// \param[in] : schema schema for the record batches, and the condition.
  /// \param[in] : cond filter condition.
  /// \param[in] : pool memory pool used to allocate selection vector (if required).
  /// \param[out]: filter the returned filter object
  static Status Make(SchemaPtr schema, ConditionPtr cond, arrow::MemoryPool *pool,
                     std::shared_ptr<Filter> *filter);

  /// \brief Build a default filter for the given schema and condition.
  /// Customize the filter with runtime configuration.
  ///
  /// \param[in] : schema schema for the record batches, and the condition.
  /// \param[in] : cond filter conditions.
  /// \param[in] : pool memory pool used to allocate output arrays (if required).
  /// \param[in] : config run time configuration.
  /// \param[out]: filter the returned filter object
  static Status Make(SchemaPtr schema, ConditionPtr cond, arrow::MemoryPool *pool,
                     std::shared_ptr<Configuration> config,
                     std::shared_ptr<Filter> *filter);

  /// Evaluate the specified record batch, and return the allocated and populated output
  /// selection array. The output array will be allocated from the memory pool 'pool',
  ///
  /// \param[in] : batch the record batch. schema should be the same as the one in 'Make'
  /// \param[out]: out_selection the vector of allocated/populated output selection array.
  Status Evaluate(const arrow::RecordBatch &batch,
                  std::shared_ptr<arrow::Array> *out_selection);

  /// Evaluate the specified record batch, and populate the output selection arrays. The
  /// output array of sufficient capacity must be allocated by the caller.
  ///
  /// \param[in] : batch the record batch. schema should be the same as the one in 'Make'
  /// \param[in/out]: out_selection_buffer, the array data is allocated by the caller and
  ///                 populated by Evaluate.
  /// \param[out]: out_selection the vector of populated output selection array.
  Status Evaluate(const arrow::RecordBatch &batch, ArrayDataPtr out_selection_buffer,
                  std::shared_ptr<arrow::Array> *out_selection);

 private:
  Filter(std::unique_ptr<LLVMGenerator> llvm_generator, SchemaPtr schema,
         arrow::MemoryPool *pool, std::shared_ptr<Configuration> config);

  Status EvaluateCommon(const arrow::RecordBatch &batch,
                        ArrayDataPtr out_selection_buffer,
                        std::shared_ptr<arrow::Array> *out_selection);

  /// Validate the common args for Evaluate() APIs.
  Status ValidateEvaluateArgsCommon(const arrow::RecordBatch &batch);

  /// Allocate ArrayData for a selection vector of capacity 'length'.
  Status AllocSelectionVectorData(int length, ArrayDataPtr *array_data);

  /// Validate that the ArrayData has sufficient capacity to accomodate 'num_records'.
  Status ValidateSelectionVectorCapacity(const arrow::ArrayData &selection_data,
                                         int num_records);

  std::shared_ptr<arrow::Array> BitMapToSelectionVector(const uint8_t *bitmap,
                                                        int bitmap_size, int num_rows,
                                                        ArrayDataPtr selection_data);

  const std::unique_ptr<LLVMGenerator> llvm_generator_;
  const SchemaPtr schema_;
  const FieldVector output_fields_;
  arrow::MemoryPool *pool_;
  const std::shared_ptr<Configuration> configuration_;
};

}  // namespace gandiva

#endif  // GANDIVA_EXPR_FILTER_H
