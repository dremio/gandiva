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
#ifndef GANDIVA_EXPRESSION_ANNOTATOR_H
#define GANDIVA_EXPRESSION_ANNOTATOR_H

#include "CodeGen.pb.h"
#include "cex/cex.h"

namespace gandiva {

/*
 * Decompose an expression received from the executor.
 */
class ExpressionAnnotator {
 public:
  static ValueValidityPair *Decompose(const Expr *expr);

 private:
  ExpressionAnnotator() {}

  Cex *BuildCex(const Expr *expr);
  Cex *BuildFuncCex(const FunctionExpr *expr);
  Cex *BuildVectorReadCex(const VectorExpr *expr);
  Cex *BuildLiteralCex(const Literal *expr);
};

} // namespace gandiva

#endif //GANDIVA_EXPRESSION_ANNOTATOR_H
