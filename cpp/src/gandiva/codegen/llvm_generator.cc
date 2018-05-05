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
#include <fstream>
#include <iostream>
#include <vector>
#include "codegen_exception.h"
#include "dex/dex.h"
#include "dex/literal_dex.h"
#include "dex/non_nullable_func_dex.h"
#include "dex/nullable_never_func_dex.h"
#include "dex/vector_read_value_dex.h"
#include "dex/vector_read_validity_dex.h"
#include "expression_annotator.h"
#include "function_registry.h"
#include "llvm_generator.h"
#include "lvalue.h"

#define REPRO_SAVE

namespace gandiva {

LLVMGenerator::LLVMGenerator()
    : engine_(new Engine()),
      types_(engine_->context()),
      in_replay_(false),
      optimise_ir_(true),
      enable_ir_traces_(false) {}

LLVMGenerator::~LLVMGenerator() {
  for (auto it = compiled_exprs_.begin(); it != compiled_exprs_.end(); ++it) {
    delete *it;
  }
}

void LLVMGenerator::Add(const Expr *expr, const VectorExpr *output) {
  int idx = compiled_exprs_.size();

  // decompose the expression to separate out value and validities.
  ValueValidityPairSharedPtr value_validity = ValueValidityPairSharedPtr(ExpressionAnnotator::Decompose(expr));

  // Generate the IR function for the decomposed expression.
  llvm::Function *ir_function = CodeGenExprValue(value_validity->value_expr(), *output, idx);

  CompiledExpr *compiled_expr = new CompiledExpr(value_validity, output, ir_function);
  compiled_exprs_.push_back(compiled_expr);
}

/*
 * Build and optimise module for projection expression.
 */
void LLVMGenerator::Build(std::unique_ptr<Projection> projection) {
  projection_ = std::move(projection);
  ReproSaveBuild();

  Projection *projection_ptr = projection_.get();
  for (int i = 0; i < projection_ptr->exprs_size(); i++) {
    const ColumnExpr &column_expr = projection_ptr->exprs(i);
    Add(&column_expr.expr(), &column_expr.output());
  }

  // optimise, compile and finalize the module
  engine_->FinalizeModule(optimise_ir_, in_replay_);

  // setup the jit functions for each expression.
  for (auto it = compiled_exprs_.begin(); it != compiled_exprs_.end(); it++) {
    CompiledExpr *compiled_expr = *it;
    eval_func_t fn = reinterpret_cast<eval_func_t>(engine_->IRFunctionToFunctionPointer(compiled_expr->ir_function()));
    compiled_expr->set_jit_function(fn);
  }
}

/*
 * Execute the compiled module against the provided vectors.
 */
int LLVMGenerator::Execute(int64_t addrs[], int naddrs, int record_count) {

  ReproSaveExecute(addrs, naddrs, record_count);
  assert(record_count > 0 && naddrs > 0);

  // generate bitmap vectors, by doing an intersection.
  for (auto it = compiled_exprs_.begin(); it != compiled_exprs_.end(); it++) {
    CompiledExpr *compiled_expr = *it;

    // generate data/offset vectors.
    eval_func_t jit_function = compiled_expr->jit_function();
    jit_function(addrs, record_count);

    // generate validity vectors.
    ComputeBitMapsForExpr(compiled_expr, addrs, record_count);
  }
  return 0;
}

llvm::Value *LLVMGenerator::LoadVectorAtIndex(llvm::Value *arg_addrs,
                                              int idx,
                                              const std::string &name) {
  llvm::IRBuilder<> &builder = ir_builder();
  llvm::Value *offset = builder.CreateGEP(arg_addrs, types_.i32_constant(idx), name + "_mem_addr");
  return builder.CreateLoad(offset, name + "_mem");
}

/*
 * Get reference to validity vector at specified index in the args list.
 */
llvm::Value *LLVMGenerator::GetValidityReference(llvm::Value *arg_addrs,
                                                 int idx,
                                                 const std::string &name) {
  ReproUpdateValiditySlot(idx);

  llvm::Value *load = LoadVectorAtIndex(arg_addrs, idx, name);
  return ir_builder().CreateIntToPtr(load, types_.i64_ptr_type(), name + "_vvec");
}

/*
 * Get reference to data vector at specified index in the args list.
 */
llvm::Value *LLVMGenerator::GetDataReference(llvm::Value *arg_addrs,
                                             int idx,
                                             const std::string &name,
                                             const common::MajorType &major_type) {
  ReproUpdateSlotSize(idx, major_type);

  llvm::Value *load = LoadVectorAtIndex(arg_addrs, idx, name);
  llvm::Type *base_type = types_.DataVecType(major_type);
  llvm::Type *pointer_type = types_.ptr_type(base_type);
  return ir_builder().CreateIntToPtr(load, pointer_type, name + "_dvec");
}

/*
 * Generate code for one expression.
 *
 * Sample IR code for "c1:int + c2:int"
 *
 * The C-code equivalent is :
 * ------------------------------
 * int expr_0(long *addrs, int nrecords) {
 *   int *outVec = (int *) addrs[5];
 *   int *c0Vec = (int *) addrs[1];
 *   int *c1Vec = (int *) addrs[3];
 *   for (int loop_var = 0; loop_var < nrecords; ++loop_var) {
 *     int c0 = c0Vec[loop_var];
 *     int c1 = c1Vec[loop_var];
 *     int out = c0 + c1;
 *     outVec[loop_var] = out;
 *   }
 * }
 *
 * IR Code
 * --------
 *
 * define i32 @expr_0(i64* %args, i32 %nrecords) {
 * entry:
 *   %outmemAddr = getelementptr i64, i64* %args, i32 5
 *   %outmem = load i64, i64* %outmemAddr
 *   %outVec = inttoptr i64 %outmem to i32*
 *   %c0memAddr = getelementptr i64, i64* %args, i32 1
 *   %c0mem = load i64, i64* %c0memAddr
 *   %c0Vec = inttoptr i64 %c0mem to i32*
 *   %c1memAddr = getelementptr i64, i64* %args, i32 3
 *   %c1mem = load i64, i64* %c1memAddr
 *   %c1Vec = inttoptr i64 %c1mem to i32*
 *   br label %loop
 * loop:                                             ; preds = %loop, %entry
 *   %loop_var = phi i32 [ 0, %entry ], [ %"loop_var+1", %loop ]
 *   %"loop_var+1" = add i32 %loop_var, 1
 *   %0 = getelementptr i32, i32* %c0Vec, i32 %loop_var
 *   %c0 = load i32, i32* %0
 *   %1 = getelementptr i32, i32* %c1Vec, i32 %loop_var
 *   %c1 = load i32, i32* %1
 *   %add_int_int = call i32 @add_int_int(i32 %c0, i32 %c1)
 *   %2 = getelementptr i32, i32* %outVec, i32 %loop_var
 *   store i32 %add_int_int, i32* %2
 *   %"loop_var < nrec" = icmp slt i32 %"loop_var+1", %nrecords
 *   br i1 %"loop_var < nrec", label %loop, label %exit
 * exit:                                             ; preds = %loop
 *   ret i32 0
 * }
 *
 */
llvm::Function *LLVMGenerator::CodeGenExprValue(Dex *value_expr,
                                                const VectorExpr &output,
                                                int suffix_idx) {
  llvm::IRBuilder<> &builder = ir_builder();

  // Create fn prototype :
  //   int expr_1 (long **addrs, int nrec)
  std::vector<llvm::Type *> arguments;
  arguments.push_back(types_.i64_ptr_type());
  arguments.push_back(types_.i32_type());
  llvm::FunctionType *prototype = llvm::FunctionType::get(types_.i32_type(), arguments, false /*isVarArg*/);

  // Create fn
  std::string func_name = "expr_" + std::to_string(suffix_idx);
  engine_->AddFunctionToCompile(func_name);
  llvm::Function *fn = llvm::Function::Create(prototype, llvm::GlobalValue::ExternalLinkage, func_name, module());
  assert(fn != NULL);

  // Name the arguments
  llvm::Function::arg_iterator args = fn->arg_begin();
  llvm::Value *arg_addrs = &*args;
  arg_addrs->setName("args");
  ++args;
  llvm::Value *arg_nrecords = &*args;
  arg_nrecords->setName("nrecords");
  ++args;

  llvm::BasicBlock *loop_entry = llvm::BasicBlock::Create(context(), "entry", fn);
  llvm::BasicBlock *loop_body = llvm::BasicBlock::Create(context(), "loop", fn);
  llvm::BasicBlock *loop_exit = llvm::BasicBlock::Create(context(), "exit", fn);

  // Add reference to output vector (in entry block)
  builder.SetInsertPoint(loop_entry);
  llvm::Value *output_ref = GetDataReference(arg_addrs, output.dataidx(), output.name(), output.majortype());

  // Loop body
  builder.SetInsertPoint(loop_body);

  // define loop_var : start with 0, +1 after each iter
  llvm::PHINode *loop_var = builder.CreatePHI(types_.i32_type(), 2, "loop_var");
  loop_var->addIncoming(types_.i32_constant(0), loop_entry);
  llvm::Value *loop_update = builder.CreateAdd(loop_var, types_.i32_constant(1), "loop_var+1");
  loop_var->addIncoming(loop_update, loop_body);

  // The visitor can add code to both the entry/loop blocks.
  std::unique_ptr<Visitor> visitor(new LLVMGenerator::Visitor(this, fn, loop_entry, loop_body, arg_addrs, loop_var));
  LValueUniquePtr output_value = value_expr->accept(*visitor.get());

  // add jump to "loop block" at the end of the "setup block".
  builder.SetInsertPoint(loop_entry);
  builder.CreateBr(loop_body);

  // save the value in the output vector
  builder.SetInsertPoint(loop_body);

  if (output.majortype().minor_type() == common::BIT) {
    SetPackedBitValue(output_ref, loop_var, output_value->data());
  } else {
    llvm::Value *slot_offset = builder.CreateGEP(output_ref, loop_var);
    builder.CreateStore(output_value->data(), slot_offset);
  }
  AddTrace("saving result " + output.name() + " value %T", output_value->data());

  // check loop_var
  llvm::Value *loop_var_check = builder.CreateICmpSLT(loop_update, arg_nrecords, "loop_var < nrec");
  builder.CreateCondBr(loop_var_check, loop_body, loop_exit);

  // Loop exit
  builder.SetInsertPoint(loop_exit);
  builder.CreateRet(types_.i32_constant(0));
  return fn;
}

/*
 * Return value of a bit in bitMap.
 */
llvm::Value *LLVMGenerator::GetPackedBitValue(llvm::Value *bitmap, llvm::Value *position) {
  AddTrace("fetch bit at position %T", position);

  llvm::Value *bitmap8 = ir_builder().CreateBitCast(bitmap, types_.ptr_type(types_.i8_type()), "bitMapCast");
  return AddFunctionCall("bitMapGetBit", types_.i1_type(), std::vector<llvm::Value *> {bitmap8, position});
}

/*
 * Set the value of a bit in bitMap.
 */
void LLVMGenerator::SetPackedBitValue(llvm::Value *bitmap, llvm::Value *position, llvm::Value *value) {
  AddTrace("set bit at position %T", position);
  AddTrace("  to value %T ", value);

  llvm::Value *bitmap8 = ir_builder().CreateBitCast(bitmap, types_.ptr_type(types_.i8_type()), "bitMapCast");
  AddFunctionCall("bitMapSetBit", types_.void_type(), std::vector<llvm::Value *> {bitmap8, position, value});
}

llvm::Value *LLVMGenerator::AddFunctionCall(std::string full_name,
                                            llvm::Type *ret_type,
                                            const std::vector<llvm::Value *> &args) {

  // add to list of functions that need to be compiled.
  engine_->AddFunctionToCompile(full_name);

  // find the llvm function.
  llvm::Function *fn = module()->getFunction(full_name);
  assert(fn != NULL);

  if (enable_ir_traces_ &&
      full_name.compare("printf") &&
      full_name.compare("printff")) {
    // Trace for debugging
    AddTrace("invoke native fn " + full_name);
  }

  // build a call to the llvm function.
  if (ret_type->isVoidTy()) {
    // void functions can't have a name for the call.
    return ir_builder().CreateCall(fn, args);
  } else {
    llvm::Value *value = ir_builder().CreateCall(fn, args, full_name);
    assert(value->getType() == ret_type);
    return value;
  }
}

/*
 * replace %T with the type-specific format specifier.
 * For some reason, float/double literals are getting lost when printing with the generic printf. so, use a
 * wrapper instead.
 */
std::string LLVMGenerator::ReplaceFormatInTrace(std::string msg, llvm::Value *value, std::string *print_fn) {
  std::size_t pos = msg.find("%T");
  if (pos == std::string::npos) {
    assert(0);
    return msg;
  }

  llvm::Type *type = value->getType();
  const char *fmt = "";
  if (type->isIntegerTy(1) || type->isIntegerTy(32)) {
    // bit or int
    fmt = "%d";
  } else if (type->isIntegerTy(64)) {
    // bigint
    fmt = "%lld";
  } else if (type->isFloatTy()) {
    // float
    fmt = "%f";
    *print_fn = "print_float";
  } else if (type->isDoubleTy()) {
    // float
    fmt = "%lf";
    *print_fn = "print_double";
  } else {
    assert(0);
  }
  msg.replace(pos, 2, fmt);
  return msg;
}

void LLVMGenerator::AddTrace(const std::string &msg, llvm::Value *value) {
  if (!enable_ir_traces_) {
    return;
  }

  std::string dmsg = "IR_TRACE:: " + msg + "\n";
  std::string print_fn_name = "printf";
  if (value) {
    dmsg = ReplaceFormatInTrace(dmsg, value, &print_fn_name);
  }
  trace_strings_.push_back(dmsg);

  // cast this to an llvm pointer.
  const char *str = trace_strings_.back().c_str();
  llvm::Constant *str_int_cast = types_.i64_constant((int64_t)str);
  llvm::Constant *str_ptr_cast = llvm::ConstantExpr::getIntToPtr(str_int_cast, types_.ptr_type(types_.i8_type()));

  std::vector<llvm::Value *> args;
  args.push_back(str_ptr_cast);
  if (value) {
    args.push_back(value);
  }
  AddFunctionCall(print_fn_name, types_.i32_type(), args);
}

/*
 * Extract the bitmap addresses, and do an intersection.
 */
void LLVMGenerator::ComputeBitMapsForExpr(CompiledExpr *compiled_expr, int64_t addrs[], int record_count) {
  auto validities = compiled_expr->value_validity()->validity_exprs();

  int num_bitmaps = validities.size();
  int64_t *src_bitmaps[num_bitmaps];
  for (int i = 0; i < num_bitmaps; i++) {
    Dex *validity_dex = validities.at(i).get();
    VectorReadValidityDex *value_dex = dynamic_cast<VectorReadValidityDex *>(validity_dex);
    src_bitmaps[i] = (int64_t *) addrs[value_dex->ValidityIdx()];
  }

  int out_idx = compiled_expr->output()->validityidx();
  int64_t *dst_bitmap = (int64_t *) addrs[out_idx];

  IntersectBitMaps(dst_bitmap, src_bitmaps, num_bitmaps, record_count);
}

/*
 * Compute the intersection of multiple bitmaps.
 */
void
LLVMGenerator::IntersectBitMaps(int64_t *dst_map, int64_t **src_maps, int nmaps, int num_records) {
  int num_bytes = (num_records + 63) / 8; // aligned to 8-byte.
  int num_words = num_bytes / 8;

  switch (nmaps) {
    case 0: {
      /* no src_maps bitmap. simply set all bits */
      memset((uint8_t *) dst_map, 0xff, num_bytes);
      break;
    }

    case 1: {
      /* one src_maps bitmap. copy to dst_map */
      memcpy((uint8_t *) dst_map, (uint8_t *) src_maps[0], num_bytes);
      break;
    }

    case 2:
      /* two src_maps bitmaps. do 64-bit ANDs */
      for (int i = 0; i < num_words; ++i) {
        dst_map[i] = src_maps[0][i] & src_maps[1][i];
      }
      break;

    default: {
      /* > 2 src_maps bitmaps. do 64-bit ANDs */
      memcpy((uint8_t *) dst_map, (uint8_t *) src_maps[0], num_bytes);
      for (int m = 1; m < nmaps; ++m) {
        for (int i = 0; i < num_words; ++i) {
          dst_map[i] &= src_maps[m][i];
        }
      }

      break;
    }
  }
}

/*
 * Visitor for generating the code for a decompsed expression.
 */
LLVMGenerator::Visitor::Visitor(LLVMGenerator *generator,
                                llvm::Function *function,
                                llvm::BasicBlock *entry_block,
                                llvm::BasicBlock *loop_block,
                                llvm::Value *arg_addrs,
                                llvm::Value *loop_var)
    : generator_(generator),
      function_(function),
      entry_block_(entry_block),
      loop_block_(loop_block),
      arg_addrs_(arg_addrs),
      loop_var_(loop_var) {

  AddIRTrace("Iteration %T", loop_var);
}

/*
 * If IR traces are enabled, add a trace.
 */
inline void LLVMGenerator::Visitor::AddIRTrace(const std::string &msg, llvm::Value *value) {
  if (generator_->enable_ir_traces_) {
    generator_->AddTrace(msg, value);
  }
}

LValueUniquePtr LLVMGenerator::Visitor::visit(const VectorReadValueDex &dex) {
  llvm::IRBuilder<> &builder = ir_builder();
  llvm::BasicBlock *saved_block = builder.GetInsertBlock();

  builder.SetInsertPoint(entry_block_);
  llvm::Value *slot_ref = generator_->GetDataReference(arg_addrs_,
                                                       dex.DataIdx(),
                                                       dex.FieldName(),
                                                       *(dex.MajorType()));

  builder.SetInsertPoint(saved_block);
  llvm::Value *slot_value;
  if (dex.MajorType()->minor_type() == common::BIT) {
    slot_value = generator_->GetPackedBitValue(slot_ref, loop_var_);
  } else {
    llvm::Value *slot_offset = builder.CreateGEP(slot_ref, loop_var_);
    slot_value = builder.CreateLoad(slot_offset, dex.FieldName());
  }

  AddIRTrace("visit data vector " + dex.FieldName() + " value %T", slot_value);
  return LValueUniquePtr(new LValue(slot_value));
}

LValueUniquePtr LLVMGenerator::Visitor::visit(const VectorReadValidityDex &dex) {
  llvm::IRBuilder<> &builder = ir_builder();
  llvm::BasicBlock *saved_block = builder.GetInsertBlock();

  builder.SetInsertPoint(entry_block_);
  llvm::Value *slot_ref = generator_->GetValidityReference(arg_addrs_,
                                                           dex.ValidityIdx(),
                                                           dex.FieldName());

  builder.SetInsertPoint(saved_block);
  llvm::Value *validity = generator_->GetPackedBitValue(slot_ref, loop_var_);
  return LValueUniquePtr(new LValue(validity));
}

LValueUniquePtr LLVMGenerator::Visitor::visit(const LiteralDex &dex) {
  const Literal *literal = dex.literal();
  LLVMTypes &types = generator_->types_;

  llvm::Value *value = NULL;
  switch (literal->type()) {
    case Literal_Type_INT:
      value = types.i32_constant(literal->intliteral());
      AddIRTrace("visit int literal %T", value);
      break;

    case Literal_Type_BIGINT:
      value = types.i64_constant(literal->bigintliteral());
      AddIRTrace("visit bigint literal %T", value);
      break;

    case Literal_Type_FLOAT:
      value = types.float_constant(literal->floatliteral());
      AddIRTrace("visit float literal %T", value);
      break;

    case Literal_Type_DOUBLE:
      value = types.double_constant(literal->doubleliteral());
      AddIRTrace("visit double literal %T", value);
      break;

    default:
      assert(0);
  }
  return LValueUniquePtr(new LValue(value));
}

LValueUniquePtr LLVMGenerator::Visitor::visit(const NonNullableFuncDex &dex) {
  AddIRTrace("visit NonNullableFunc base function " + dex.func_descriptor()->name());
  LLVMTypes &types = generator_->types_;

  // build the function params.
  std::vector<llvm::Value *> args;
  for (auto it = dex.args().begin(); it != dex.args().end(); it++) {
    // add value
    Dex *value_expr = (*it)->value_expr();
    LValueUniquePtr lvalue = value_expr->accept(*this);
    args.push_back(lvalue->data());
  }

  const NativeFunction *native_function = dex.native_function();
  llvm::Type *ret_type = types.IRType(native_function->signature()->ret_type());
  llvm::Value *value = generator_->AddFunctionCall(native_function->pc_name(), ret_type, args);
  return LValueUniquePtr(new LValue(value));
}

LValueUniquePtr LLVMGenerator::Visitor::visit(const NullableNeverFuncDex &dex) {
  AddIRTrace("visit NullableNever base function " + dex.func_descriptor()->name());
  LLVMTypes &types = generator_->types_;

  // build the function params, along with the validities.
  std::vector<llvm::Value *> args;
  for (auto it = dex.args().begin(); it != dex.args().end(); it++) {
    ValueValidityPairSharedPtr pair = *it;

    // build value.
    Dex *value_expr = pair->value_expr();
    LValueUniquePtr lvalue = value_expr->accept(*this);
    args.push_back(lvalue.get()->data());

    // build validity.
    llvm::Value *validity_expr = BuildCombinedValidity(pair->validity_exprs());
    args.push_back(validity_expr);
  }

  const NativeFunction *native_function = dex.native_function();
  llvm::Type *ret_type = types.IRType(native_function->signature()->ret_type());
  llvm::Value *value = generator_->AddFunctionCall(native_function->pc_name(), ret_type, args);
  return LValueUniquePtr(new LValue(value));
}

/*
 * Bitwise-AND of a vector of bits to get the combined validity.
 */
llvm::Value *LLVMGenerator::Visitor::BuildCombinedValidity(std::vector<DexSharedPtr> validities) {
  llvm::IRBuilder<> &builder = ir_builder();
  LLVMTypes &types = generator_->types_;

  llvm::Value *isValid = types.true_constant();
  for (auto it = validities.begin(); it != validities.end(); it++) {
    LValueUniquePtr current = (*it)->accept(*this);
    isValid = builder.CreateAnd(isValid, current->data(), "validityBitAnd");
  }
  AddIRTrace("combined validity is %T", isValid);
  return isValid;
}

/*
 * Hooks to repro and debug.
 */

#ifdef REPRO_SAVE
static const char *kReproFileNameExpr = "/tmp/repro_expr.pb";
static const char *kReproFileNameVectors = "/tmp/repro_vector.pb";

/*
 * Save expression in an on-disk file.
 */
void
LLVMGenerator::ReproSaveBuild() {
  if (in_replay_) {
    return;
  }

  // Ignore errors.
  std::fstream output(kReproFileNameExpr, std::ios::out | std::ios::trunc | std::ios::binary);
  projection_.get()->SerializeToOstream(&output);
}

/*
 * Save vectors in an on-disk file.
 */
void
LLVMGenerator::ReproSaveExecute(int64_t *addrs, int naddrs, int nrecords) {
  if (in_replay_) {
    return;
  }

  VectorDump dump;
  dump.set_nrecords(nrecords);
  for (int i = 0; i < naddrs; ++i) {
    if (slot_size_in_bits_[i] == 0) {
      // don't know size, assume validity.
      slot_size_in_bits_[i] = 1;
    }
    int sz_in_bits = nrecords * slot_size_in_bits_[i];
    int sz_in_bytes = (sz_in_bits + 63) / 8;
    dump.add_binarydata((char *) addrs[i], sz_in_bytes);
  }

  // Ignore errors.
  std::fstream output(kReproFileNameVectors, std::ios::out | std::ios::trunc | std::ios::binary);
  dump.SerializeToOstream(&output);
}

void
LLVMGenerator::ReproUpdateValiditySlot(int slot) {
  slot_size_in_bits_[slot] = 1;
}

void
LLVMGenerator::ReproUpdateSlotSize(int slot, const common::MajorType &type) {
  int len = 0;
  switch (type.minor_type()) {
    case common::BIT:
      len = 1;
      break;

    case common::INT:
    case common::FLOAT4:
      len = 32;
      break;

    case common::BIGINT:
    case common::FLOAT8:
    case common::TIME:
    case common::TIMESTAMP:
    case common::DATE:
      len = 64;
      break;

    default:
      assert(0);
      break;
  }
  slot_size_in_bits_[slot] = len;
}

/*
 * Read expression and vectors from disk, and replay.
 */
int
LLVMGenerator::ReproReplay(bool optimise_ir, bool trace_ir) {
  LLVMGenerator *llvm_generator = NULL;
  try {
    std::unique_ptr<Projection> projection(new Projection());
    llvm_generator = new LLVMGenerator();
    llvm_generator->in_replay_ = true;
    llvm_generator->optimise_ir_ = optimise_ir;
    llvm_generator->enable_ir_traces_ = trace_ir;

    // read projection from disk
    std::fstream input_expr(kReproFileNameExpr, std::ios::in | std::ios::binary);
    if (!projection.get()->ParseFromIstream(&input_expr)) {
      fprintf(stderr, "failed to parse expression from input file %s\n", kReproFileNameExpr);
      return 1;
    }
    llvm_generator->Build(std::move(projection));

    // read vectors from disk
    VectorDump dump;
    std::fstream input_vectors(kReproFileNameVectors, std::ios::in | std::ios::binary);
    if (!dump.ParseFromIstream(&input_vectors)) {
      fprintf(stderr, "failed to parse vectors from input file %s\n", kReproFileNameVectors);
      return 1;
    }

    int nvectors = dump.binarydata_size();
    int64_t addrs[nvectors];
    for (int i = 0; i < nvectors; ++i) {
      int len = dump.binarydata(i).length();
      char *buf = new char[len];
      memcpy(buf, dump.binarydata(i).data(), len);

      addrs[i] = (int64_t) buf;
    }

    // replay the batch
    llvm_generator->Execute(addrs, nvectors, dump.nrecords());
    for (int i = 0; i < nvectors; ++i) {
      delete (char *) addrs[i];
    }
  } catch (CodeGenException e) {
    fprintf(stderr, "Repro with LLVMGenerator failed %d msg %s\n", e.error(), e.error_msg().c_str());
    return 1;
  }
  delete llvm_generator;
  return 0;
}
#else // !REPRO_SAVE

// Add dummy functions.
void LLVMGenerator::ReproSaveBuild() {}
void LLVMGenerator::ReproSaveExecute(int64_t *addrs, int naddrs, int nrecords) {}
void LLVMGenerator::ReproUpdateValiditySlot(int slot) {}
void LLVMGenerator::ReproUpdateSlotSize(int slot, const common::MajorType &type) {}
int LLVMGenerator::ReproReplay(bool optimise_ir, bool trace_ir) {return 0;}

#endif // REPRO_SAVE

} // namespace gandiva
