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

#include <stdint.h>
#include "gandiva_fwd.h"
#include "CodeGen.pb.h"
#include "dex_visitor.h"
#include "compiled_expr.h"
#include "engine.h"
#include "value_validity_pair.h"
#include "llvm_types.h"
#include "lvalue.h"

namespace gandiva {

class LLVMGenerator {
 public:
  LLVMGenerator();
  ~LLVMGenerator();

  /*
   * Build an IR module for the expressions described in projection.
   */
  void Build(std::unique_ptr<Projection> projection);

  /*
   * Execute the built expression against the provided arguments.
   */
  int Execute(int64_t addrs[], int num_addrs, int record_count);

  /*
   * Debug only : replay the last evaluated expression & batch.
   */
  static int ReproReplay(bool optimise_ir, bool trace_ir);

 private:
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

    virtual void Visit(const VectorReadValidityDex &dex) override;
    virtual void Visit(const VectorReadValueDex &dex) override;
    virtual void Visit(const LiteralDex &dex) override;
    virtual void Visit(const NonNullableFuncDex &dex) override;
    virtual void Visit(const NullableNeverFuncDex &dex) override;

    LValueSharedPtr result() { return result_; }

   private:
    llvm::IRBuilder<> &ir_builder() { return generator_->ir_builder(); }
    llvm::Module *module() { return generator_->module(); }

    llvm::Value *BuildCombinedValidity(std::vector<DexSharedPtr> validities);

    LLVMGenerator *generator_;
    LValueSharedPtr result_;
    llvm::BasicBlock *entry_block_;
    llvm::BasicBlock *loop_block_;
    llvm::Value *arg_addrs_;
    llvm::Value *loop_var_;
  };

  void Add(const Expr *expr, const VectorExpr *output);

  llvm::Value *LoadVectorAtIndex(llvm::Value *arg_addrs, int idx, const std::string &name);
  llvm::Value *GetValidityReference(llvm::Value *arg_addrs, int idx, FieldSharedPtr field);
  llvm::Value *GetDataReference(llvm::Value *arg_addrs, int idx, FieldSharedPtr field);

  /// Generate code for the value array of one expression.
  llvm::Function *CodeGenExprValue(DexSharedPtr value_expr,
                                   FieldDescriptorSharedPtr output,
                                   int suffix_idx);

  llvm::Value *GetPackedBitValue(llvm::Value *bitMap, llvm::Value *position);
  void SetPackedBitValue(llvm::Value *bitMap, llvm::Value *position, llvm::Value *value);

  llvm::Value *AddFunctionCall(std::string full_name, llvm::Type *ret_type, const std::vector<llvm::Value *> &args);

  std::string ReplaceFormatInTrace(std::string msg, llvm::Value *value, std::string *print_fn);
  void AddTrace(const std::string &msg, llvm::Value *value = NULL);

  void ComputeBitMapsForExpr(CompiledExpr *compiledExpr, int64_t addrs[], int record_count);
  void IntersectBitMaps(int64_t *dst_map, int64_t **src_maps, int nmaps, int num_records);

  // Repro/replay related
  void ReproSaveBuild();
  void ReproSaveExecute(int64_t *addrs, int naddrs, int nrecords);
  void ReproUpdateValiditySlot(int slot);
  void ReproUpdateSlotSize(int slot, const common::MajorType &type);

  std::unique_ptr<Engine> engine_;
  std::unique_ptr<Projection> projection_;
  std::vector<CompiledExpr *> compiled_exprs_;
  LLVMTypes types_;

  // used in replay/debug
  bool in_replay_;
  bool optimise_ir_;
  bool enable_ir_traces_;
  std::map<int, int> slot_size_in_bits_;
  std::vector<std::string> trace_strings_;
};

} // namespace gandiva

#endif // GANDIVA_LLVMGENERATOR_H
