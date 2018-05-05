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
#include "CodeGen.pb.h"
#include "expression_annotator.h"
#include "cex/func_cex.h"
#include "cex/literal_cex.h"
#include "cex/vector_read_cex.h"
#include "dex/literal_dex.h"
#include "dex/vector_read_validity_dex.h"
#include "dex/vector_read_value_dex.h"
#include "dex/non_nullable_func_dex.h"
#include "func_descriptor.h"
#include "vector_descriptor.h"

namespace gandiva {

ValueValidityPair *ExpressionAnnotator::Decompose(const Expr *expr) {
  std::unique_ptr<ExpressionAnnotator> annotator(new ExpressionAnnotator());
  std::unique_ptr<Cex> cex(annotator->BuildCex(expr));

  return cex.get()->Decompose();
}

/*
 * Recursively build a composed expression from the protobuf.
 */
Cex *ExpressionAnnotator::BuildCex(const Expr *expr) {
  switch (expr->type()) {
    case Expr_Type_FUNCTION:
      return BuildFuncCex(&expr->function());

    case Expr_Type_VECTOR:
      return BuildVectorReadCex(&expr->vector());

    case Expr_Type_LITERAL:
      return BuildLiteralCex(&expr->literal());

    default:
      // TODO : not implemented yet
      assert(0);
      return NULL;
  }
}

/*
 * Build composed expression for a function.
 */
Cex *ExpressionAnnotator::BuildFuncCex(const FunctionExpr *function_expr) {
  std::vector<Cex *> children;
  std::vector<const common::MajorType *> params;
  for (int i = 0; i < function_expr->children().size(); ++i) {
    params.push_back(&function_expr->argtypes(i));
    children.push_back(BuildCex(&function_expr->children(i)));
  }

  FuncDescriptorSharedPtr func_descriptor(new FuncDescriptor(function_expr->name(),
                                                             params,
                                                             &function_expr->returntype()));
  return new FuncCex(func_descriptor, children);
}

/*
 * Build composed expression for a vector.
 */
Cex *ExpressionAnnotator::BuildVectorReadCex(const VectorExpr *vector_expr) {
  VectorDescriptorSharedPtr vectorDescriptor(new VectorDescriptor(*vector_expr));
  return new VectorReadCex(vectorDescriptor);
}

/*
 * Build composed expression for a literal.
 */
Cex *ExpressionAnnotator::BuildLiteralCex(const Literal *literal_expr) {
  return new LiteralCex(literal_expr);
}

} // namespace gandiva
