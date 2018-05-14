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

#include "arrow.h"
#include "expression.h"

namespace gandiva {

/// \brief Evaluator for expressions.
///
/// An evaluator is built for a specific schema and vector of expressions.
/// Once the evaluator is built, it can be used to evaluate many row batches.
class Evaluator {
 public:
  /// Evaluate the specified record batch, and fill the output vectors.
  void Evaluate(RecordBatchSharedPtr batch,
                std::vector<std::unique_ptr<arrow::ArrayBuilder>> &builders);

  /// Build an evlautor for the given schema to evaluate the vector of
  static std::shared_ptr<Evaluator> Make(SchemaSharedPtr schema,
                                         ExpressionVector exprs);
};

} // namespace gandiva

#endif // GANDIVA_EXPR_EVALUATOR_H
