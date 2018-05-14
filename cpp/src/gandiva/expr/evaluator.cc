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

#include "evaluator.h"

namespace gandiva {

std::shared_ptr<Evaluator> Evaluator::Make(SchemaSharedPtr schema, ExpressionVector exprs) {
  LLVMGenerator *llvm_gen = new LLVMGenerator();

  // TODO: What is the schema used for?
  // Gandiva shouldn't be doing schema checks right
  llvm_gen->Build(exprs);

  return std::shared_ptr<Evaluator>(new Evaluator(llvm_gen));
}

void Evaluator::Evaluate(RecordBatchSharedPtr batch, std::vector<std::unique_ptr<arrow::ArrayBuilder>> &builders) {
}

} // namespace gandiva
