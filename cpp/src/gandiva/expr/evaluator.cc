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

#include <memory>
#include <vector>
#include <utility>
#include "expr/evaluator.h"

namespace gandiva {

// TODO : exceptions
std::shared_ptr<Evaluator> Evaluator::Make(SchemaSharedPtr schema,
                                           const ExpressionVector &exprs) {
  // TODO: validate schema
  // TODO : validate expressions (fields, function signatures, output types, ..)

  /*
   * Build LLVM generator, and generate code for the specified expressions.
   */
  std::unique_ptr<LLVMGenerator> llvm_gen(new LLVMGenerator());
  llvm_gen->Build(exprs);

  /*
   * save the output field types. Used for validation at Evaluate() time.
   */
  std::vector<FieldSharedPtr> output_fields;
  for (auto it = exprs.begin(); it != exprs.end(); ++it) {
    output_fields.push_back((*it)->result());
  }
  return std::shared_ptr<Evaluator>(new Evaluator(std::move(llvm_gen),
                                                  schema,
                                                  output_fields));
}

void Evaluator::Evaluate(RecordBatchSharedPtr batch,
                         const arrow::ArrayVector &outputs) {
  // TODO : validate that the schema from the batch matches the one used in the
  //        constructor.
  // TODO : validate that the datatype of the output vectors matches the one used in the
  //        constructor.
  // TODO : validate that the outputs vectors have sufficient capcity.
  llvm_generator_->Execute(batch, outputs);
}

} // namespace gandiva
