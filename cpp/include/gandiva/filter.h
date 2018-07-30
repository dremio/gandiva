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
#include "gandiva/selection_vector.h"
#include "gandiva/status.h"

namespace gandiva {

class LLVMGenerator;

/// \brief filter records based on an condition.
///
/// A filter is built for a specific schema and condition. Once the filter is built, it
/// can be used to evaluate many row batches.
template <typename SELECT_TYPE>
class Filter {
 public:
  Filter(std::unique_ptr<LLVMGenerator> llvm_generator, SchemaPtr schema,
         std::shared_ptr<Configuration> config);

  ~Filter() = default;

 protected:
  static Status MakeGenerator(SchemaPtr schema, ConditionPtr cond,
                              std::shared_ptr<Configuration> configuration,
                              std::unique_ptr<LLVMGenerator> *generator);

  /// Evaluate the specified record batch, and populate output selection vector.
  ///
  /// \param[in] : batch the record batch. schema should be the same as the one in 'Make'
  /// \param[in/out]: out_selection the output selection array.
  Status EvaluateCommon(const arrow::RecordBatch &batch,
                        std::shared_ptr<SELECT_TYPE> out_selection);

  const std::unique_ptr<LLVMGenerator> llvm_generator_;
  const SchemaPtr schema_;
  arrow::MemoryPool *pool_;
  const std::shared_ptr<Configuration> configuration_;
};

class FilterWithSVInt16 : public Filter<SelectionVectorInt16> {
 public:
  FilterWithSVInt16(std::unique_ptr<LLVMGenerator> llvm_generator, SchemaPtr schema,
                    std::shared_ptr<Configuration> config);

  /// Build a default filter for the given schema and condition.
  ///
  /// \param[in] : schema schema for the record batches, and the condition.
  /// \param[in] : cond filter condition.
  /// \param[out]: filter the returned filter object
  static Status Make(SchemaPtr schema, ConditionPtr cond,
                     std::shared_ptr<FilterWithSVInt16> *filter) {
    return Make(schema, cond, ConfigurationBuilder::DefaultConfiguration(), filter);
  }

  /// \brief Build a default filter for the given schema and condition.
  /// Customize the filter with runtime configuration.
  ///
  /// \param[in] : schema schema for the record batches, and the condition.
  /// \param[in] : cond filter conditions.
  /// \param[in] : config run time configuration.
  /// \param[out]: filter the returned filter object
  static Status Make(SchemaPtr schema, ConditionPtr cond,
                     std::shared_ptr<Configuration> config,
                     std::shared_ptr<FilterWithSVInt16> *filter);

  /// Evaluate the specified record batch, and populate output selection vector.
  ///
  /// \param[in] : batch the record batch. schema should be the same as the one in 'Make'
  /// \param[in/out]: out_selection the output selection array.
  Status Evaluate(const arrow::RecordBatch &batch,
                  std::shared_ptr<SelectionVectorInt16> out_selection);
};

class FilterWithSVInt32 : public Filter<SelectionVectorInt32> {
 public:
  FilterWithSVInt32(std::unique_ptr<LLVMGenerator> llvm_generator, SchemaPtr schema,
                    std::shared_ptr<Configuration> config);

  /// Build a default filter for the given schema and condition.
  ///
  /// \param[in] : schema schema for the record batches, and the condition.
  /// \param[in] : cond filter condition.
  /// \param[out]: filter the returned filter object
  static Status Make(SchemaPtr schema, ConditionPtr cond,
                     std::shared_ptr<FilterWithSVInt32> *filter) {
    return Make(schema, cond, ConfigurationBuilder::DefaultConfiguration(), filter);
  }

  /// \brief Build a default filter for the given schema and condition.
  /// Customize the filter with runtime configuration.
  ///
  /// \param[in] : schema schema for the record batches, and the condition.
  /// \param[in] : cond filter conditions.
  /// \param[in] : config run time configuration.
  /// \param[out]: filter the returned filter object
  static Status Make(SchemaPtr schema, ConditionPtr cond,
                     std::shared_ptr<Configuration> config,
                     std::shared_ptr<FilterWithSVInt32> *filter);

  /// Evaluate the specified record batch, and populate output selection vector.
  ///
  /// \param[in] : batch the record batch. schema should be the same as the one in 'Make'
  /// \param[in/out]: out_selection the output selection array.
  Status Evaluate(const arrow::RecordBatch &batch,
                  std::shared_ptr<SelectionVectorInt32> out_selection);
};

}  // namespace gandiva

#endif  // GANDIVA_EXPR_FILTER_H
