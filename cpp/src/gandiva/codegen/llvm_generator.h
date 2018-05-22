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
#ifndef GANDIVA_LLVMGENERATOR_H
#define GANDIVA_LLVMGENERATOR_H

#include <memory>
#include <string>
#include <vector>
#include <cstdint>
#include <gtest/gtest_prod.h>
#include "common/gandiva_fwd.h"
#include "codegen/dex_visitor.h"
#include "codegen/compiled_expr.h"
#include "codegen/engine.h"
#include "codegen/function_registry.h"
#include "codegen/value_validity_pair.h"
#include "codegen/llvm_types.h"
#include "codegen/lvalue.h"
#include "expr/annotator.h"
#include "expr/expression.h"

namespace gandiva {

/// Builds an LLVM mode and generates code for the specified set of expressions.
class LLVMGenerator {
 public:
  LLVMGenerator();
  ~LLVMGenerator();

  /// \brief Build from expression tree, represented by an element of the vector
  void Build(const ExpressionVector &exprs);

  /// \brief Execute the built expression against the provided arguments.
  int Execute(RecordBatchSharedPtr record_batch, const arrow::ArrayVector &outputs);

 private:
  FRIEND_TEST(TestLLVMGenerator, TestAdd);
  FRIEND_TEST(TestLLVMGenerator, TestIntersectBitMaps);

  llvm::Module *module() { return engine_->module(); }
  llvm::LLVMContext &context() { return *(engine_->context()); }
  llvm::IRBuilder<> &ir_builder() { return engine_->ir_builder(); }
  LLVMTypes &types() { return types_; }

  llvm::Function *CodeGenVecAdd();

  class Visitor : public DexVisitor {
   public:
    Visitor(LLVMGenerator *generator,
            llvm::Function *function,
            llvm::BasicBlock *entry_block,
            llvm::BasicBlock *loop_block,
            llvm::Value *arg_addrs,
            llvm::Value *loop_var);

    void Visit(const VectorReadValidityDex &dex) override;
    void Visit(const VectorReadValueDex &dex) override;
    void Visit(const LiteralDex &dex) override;
    void Visit(const NonNullableFuncDex &dex) override;
    void Visit(const NullableNeverFuncDex &dex) override;

    LValueSharedPtr result() { return result_; }

   private:
    llvm::IRBuilder<> &ir_builder() { return generator_->ir_builder(); }
    llvm::Module *module() { return generator_->module(); }

    llvm::Value *BuildCombinedValidity(const std::vector<DexSharedPtr> &validities);
    void AddTrace(const std::string &msg, llvm::Value *value = NULL);

    LLVMGenerator *generator_;
    LValueSharedPtr result_;
    llvm::BasicBlock *entry_block_;
    llvm::BasicBlock *loop_block_;
    llvm::Value *arg_addrs_;
    llvm::Value *loop_var_;
  };

  void Add(const ExpressionSharedPtr expr, const FieldDescriptorSharedPtr output);

  llvm::Value *LoadVectorAtIndex(llvm::Value *arg_addrs,
                                 int idx,
                                 const std::string &name);
  llvm::Value *GetValidityReference(llvm::Value *arg_addrs,
                                    int idx,
                                    FieldSharedPtr field);
  llvm::Value *GetDataReference(llvm::Value *arg_addrs,
                                int idx,
                                FieldSharedPtr field);

  /// Generate code for the value array of one expression.
  llvm::Function *CodeGenExprValue(DexSharedPtr value_expr,
                                   FieldDescriptorSharedPtr output,
                                   int suffix_idx);

  llvm::Value *GetPackedBitValue(llvm::Value *bitMap, llvm::Value *position);
  void SetPackedBitValue(llvm::Value *bitMap, llvm::Value *position, llvm::Value *value);
  llvm::Value *AddFunctionCall(const std::string &full_name,
                               llvm::Type *ret_type,
                               const std::vector<llvm::Value *> &args);

  void ComputeBitMapsForExpr(CompiledExpr *compiledExpr,
                             uint8_t **buffers,
                             int record_count);

  static void IntersectBitMaps(uint8_t *dst_map,
                               const std::vector<uint8_t *> &src_maps,
                               int num_records);

  // tracing related
  std::string ReplaceFormatInTrace(const std::string &msg,
                                   llvm::Value *value,
                                   std::string *print_fn);
  void AddTrace(const std::string &msg, llvm::Value *value = NULL);

  std::unique_ptr<Engine> engine_;
  std::vector<CompiledExpr *> compiled_exprs_;
  LLVMTypes types_;
  FunctionRegistry function_registry_;
  Annotator annotator_;

  // used in replay/debug
  bool in_replay_;
  bool optimise_ir_;
  bool enable_ir_traces_;
  std::vector<std::string> trace_strings_;
};

} // namespace gandiva

#endif // GANDIVA_LLVMGENERATOR_H
