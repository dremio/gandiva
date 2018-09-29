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

#ifndef GANDIVA_COMPILED_EXPR_H
#define GANDIVA_COMPILED_EXPR_H

#include <llvm/IR/IRBuilder.h>
#include "codegen/value_validity_pair.h"

namespace gandiva {

using EvalFunc = int (*)(uint8_t **buffers, uint8_t **local_bitmaps,
                         int64_t execution_ctx_ptr, int record_count);

using EvalFunc_Sel_Vec_16 = int (*)(uint8_t **buffers, uint8_t **local_bitmaps,
                                    uint8_t *selection_vector, int64_t execution_ctx_ptr,
                                    int record_count);

using EvalFunc_Sel_Vec_32 = int (*)(uint8_t **buffers, uint8_t **local_bitmaps,
                                    uint8_t *selection_vector, int64_t execution_ctx_ptr,
                                    int record_count);

/// \brief Tracks the compiled state for one expression.
class CompiledExpr {
 public:
  CompiledExpr(ValueValidityPairPtr value_validity, FieldDescriptorPtr output,
               llvm::Function *ir_function)
      : value_validity_(value_validity),
        output_(output),
        ir_function_(ir_function),
        ir_function_sel_vec_16_(NULL),
        ir_function_sel_vec_32_(NULL),
        jit_function_(NULL),
        jit_function_sel_vec_16_(NULL),
        jit_function_sel_vec_32_(NULL) {}

  CompiledExpr(ValueValidityPairPtr value_validity, FieldDescriptorPtr output,
               llvm::Function *ir_function, llvm::Function *ir_function_sel_vec_16,
               llvm::Function *ir_function_sel_vec_32)
      : value_validity_(value_validity),
        output_(output),
        ir_function_(ir_function),
        ir_function_sel_vec_16_(ir_function_sel_vec_16),
        ir_function_sel_vec_32_(ir_function_sel_vec_32),
        jit_function_(NULL),
        jit_function_sel_vec_16_(NULL),
        jit_function_sel_vec_32_(NULL) {}

  ValueValidityPairPtr value_validity() const { return value_validity_; }

  FieldDescriptorPtr output() const { return output_; }

  llvm::Function *ir_function() const { return ir_function_; }

  llvm::Function *ir_function_sel_vec_16() const { return ir_function_sel_vec_16_; }

  llvm::Function *ir_function_sel_vec_32() const { return ir_function_sel_vec_32_; }

  EvalFunc jit_function() const { return jit_function_; }

  EvalFunc_Sel_Vec_16 jit_function_sel_vec_16() const { return jit_function_sel_vec_16_; }

  EvalFunc_Sel_Vec_32 jit_function_sel_vec_32() const { return jit_function_sel_vec_32_; }

  void set_jit_function(EvalFunc jit_function) { jit_function_ = jit_function; }

  void set_jit_function_sel_vec_16(EvalFunc_Sel_Vec_16 jit_function_sel_vec_16) {
    jit_function_sel_vec_16_ = jit_function_sel_vec_16;
  }

  void set_jit_function_sel_vec_32(EvalFunc_Sel_Vec_32 jit_function_sel_vec_32) {
    jit_function_sel_vec_32_ = jit_function_sel_vec_32;
  }

 private:
  // value & validities for the expression tree (root)
  ValueValidityPairPtr value_validity_;

  // output field
  FieldDescriptorPtr output_;

  // IR function in the generated code
  llvm::Function *ir_function_;

  // IR function in the generated code which is for 16 bit selection vector
  llvm::Function *ir_function_sel_vec_16_;

  // IR function in the generated code whoch is for 32 bit selection  vector
  llvm::Function *ir_function_sel_vec_32_;

  // JIT function in the generated code (set after the module is optimised and finalized)
  EvalFunc jit_function_;

  // JIT function in the generated code 16 bit selection vector
  EvalFunc_Sel_Vec_16 jit_function_sel_vec_16_;

  // JIT fuction in the generated code for 32 bit selection vector
  EvalFunc_Sel_Vec_32 jit_function_sel_vec_32_;
};

}  // namespace gandiva

#endif  // GANDIVA_COMPILED_EXPR_H
