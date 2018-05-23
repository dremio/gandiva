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
#ifndef GANDIVA_EXPR_EVALUATOR_H
#define GANDIVA_EXPR_EVALUATOR_H

#include <memory>
#include <utility>
#include <vector>
#include "common/arrow.h"
#include "expr/expression.h"
#include "codegen/llvm_generator.h"

namespace gandiva {

/// \brief Evaluator for expressions.
///
/// An evaluator is built for a specific schema and vector of expressions.
/// Once the evaluator is built, it can be used to evaluate many row batches.
class Evaluator {
 public:
  /// Build an evaluator for the given schema to evaluate the vector of expressions.
  static std::shared_ptr<Evaluator> Make(SchemaPtr schema,
                                         const ExpressionVector &exprs,
                                         arrow::MemoryPool *pool);

  /// Evaluate the specified record batch, and fill the output vectors.
  /// TODO : need a zero-copy variant if the caller can alloc the output vectors.
  arrow::ArrayVector Evaluate(const arrow::RecordBatch &batch);

 private:
  Evaluator(std::unique_ptr<LLVMGenerator> llvm_generator,
            SchemaPtr schema,
            const FieldVector &output_fields,
            arrow::MemoryPool *pool)
    : llvm_generator_(std::move(llvm_generator)),
      schema_(schema),
      output_fields_(output_fields),
      pool_(pool) {}

  ArrayPtr AllocArray(DataTypePtr type, int length);

  const std::unique_ptr<LLVMGenerator> llvm_generator_;
  const SchemaPtr schema_;
  const FieldVector output_fields_;
  arrow::MemoryPool *pool_;
};

} // namespace gandiva

#endif // GANDIVA_EXPR_EVALUATOR_H
