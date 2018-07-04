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

#ifndef GANDIVA_DEX_DEX_H
#define GANDIVA_DEX_DEX_H

#include <string>
#include <vector>

#include "codegen/dex_visitor.h"
#include "codegen/field_descriptor.h"
#include "codegen/func_descriptor.h"
#include "codegen/literal_holder.h"
#include "codegen/native_function.h"
#include "codegen/value_validity_pair.h"
#include "gandiva/gandiva_aliases.h"

namespace gandiva {

/// \brief Decomposed expression : the validity and value are separated.
class Dex {
 public:
  /// Derived classes should simply invoke the Visit api of the visitor.
  virtual void Accept(DexVisitor &visitor) = 0;
  virtual ~Dex() = default;
};

/// Base class for other Vector related Dex.
class VectorReadBaseDex : public Dex {
 public:
  explicit VectorReadBaseDex(FieldDescriptorPtr field_desc) : field_desc_(field_desc) {}

  const std::string &FieldName() const { return field_desc_->Name(); }

  DataTypePtr FieldType() const { return field_desc_->Type(); }

  FieldPtr Field() const { return field_desc_->field(); }

 protected:
  FieldDescriptorPtr field_desc_;
};

/// validity component of a ValueVector
class VectorReadValidityDex : public VectorReadBaseDex {
 public:
  explicit VectorReadValidityDex(FieldDescriptorPtr field_desc)
      : VectorReadBaseDex(field_desc) {}

  int ValidityIdx() const { return field_desc_->validity_idx(); }

  void Accept(DexVisitor &visitor) override { visitor.Visit(*this); }
};

/// value component of a fixed-len ValueVector
class VectorReadFixedLenValueDex : public VectorReadBaseDex {
 public:
  explicit VectorReadFixedLenValueDex(FieldDescriptorPtr field_desc)
      : VectorReadBaseDex(field_desc) {}

  int DataIdx() const { return field_desc_->data_idx(); }

  void Accept(DexVisitor &visitor) override { visitor.Visit(*this); }
};

/// value component of a variable-len ValueVector
class VectorReadVarLenValueDex : public VectorReadBaseDex {
 public:
  explicit VectorReadVarLenValueDex(FieldDescriptorPtr field_desc)
      : VectorReadBaseDex(field_desc) {}

  int DataIdx() const { return field_desc_->data_idx(); }

  int OffsetsIdx() const { return field_desc_->offsets_idx(); }

  void Accept(DexVisitor &visitor) override { visitor.Visit(*this); }
};

/// validity based on a local bitmap.
class LocalBitMapValidityDex : public Dex {
 public:
  explicit LocalBitMapValidityDex(int local_bitmap_idx)
      : local_bitmap_idx_(local_bitmap_idx) {}

  int local_bitmap_idx() const { return local_bitmap_idx_; }

  void Accept(DexVisitor &visitor) override { visitor.Visit(*this); }

 private:
  int local_bitmap_idx_;
};

/// base function expression
class FuncDex : public Dex {
 public:
  FuncDex(FuncDescriptorPtr func_descriptor, const NativeFunction *native_function,
          const ValueValidityPairVector &args)
      : func_descriptor_(func_descriptor),
        native_function_(native_function),
        args_(args) {}

  FuncDescriptorPtr func_descriptor() const { return func_descriptor_; }

  const NativeFunction *native_function() const { return native_function_; }

  const ValueValidityPairVector &args() const { return args_; }

 private:
  FuncDescriptorPtr func_descriptor_;
  const NativeFunction *native_function_;
  ValueValidityPairVector args_;
};

/// A function expression that only deals with non-null inputs, and generates non-null
/// outputs.
class NonNullableFuncDex : public FuncDex {
 public:
  NonNullableFuncDex(FuncDescriptorPtr func_descriptor,
                     const NativeFunction *native_function,
                     const ValueValidityPairVector &args)
      : FuncDex(func_descriptor, native_function, args) {}

  void Accept(DexVisitor &visitor) override { visitor.Visit(*this); }
};

/// A function expression that deals with nullable inputs, but generates non-null
/// outputs.
class NullableNeverFuncDex : public FuncDex {
 public:
  NullableNeverFuncDex(FuncDescriptorPtr func_descriptor,
                       const NativeFunction *native_function,
                       const ValueValidityPairVector &args)
      : FuncDex(func_descriptor, native_function, args) {}

  void Accept(DexVisitor &visitor) override { visitor.Visit(*this); }
};

/// A function expression that deals with nullable inputs, and
/// nullable outputs.
class NullableInternalFuncDex : public FuncDex {
 public:
  NullableInternalFuncDex(FuncDescriptorPtr func_descriptor,
                          const NativeFunction *native_function,
                          const ValueValidityPairVector &args, int local_bitmap_idx)
      : FuncDex(func_descriptor, native_function, args),
        local_bitmap_idx_(local_bitmap_idx) {}

  void Accept(DexVisitor &visitor) override { visitor.Visit(*this); }

  /// The validity of the function result is saved in this bitmap.
  int local_bitmap_idx() const { return local_bitmap_idx_; }

 private:
  int local_bitmap_idx_;
};

/// special validity type that always returns true.
class TrueDex : public Dex {
  void Accept(DexVisitor &visitor) override { visitor.Visit(*this); }
};

/// special validity type that always returns false.
class FalseDex : public Dex {
  void Accept(DexVisitor &visitor) override { visitor.Visit(*this); }
};

/// decomposed expression for a literal.
class LiteralDex : public Dex {
 public:
  LiteralDex(DataTypePtr type, const LiteralHolder &holder)
      : type_(type), holder_(holder) {}

  const DataTypePtr &type() const { return type_; }

  const LiteralHolder &holder() const { return holder_; }

  void Accept(DexVisitor &visitor) override { visitor.Visit(*this); }

 private:
  DataTypePtr type_;
  LiteralHolder holder_;
};

/// decomposed if-else expression.
class IfDex : public Dex {
 public:
  IfDex(ValueValidityPairPtr condition_vv, ValueValidityPairPtr then_vv,
        ValueValidityPairPtr else_vv, DataTypePtr result_type, int local_bitmap_idx,
        bool is_terminal_else)
      : condition_vv_(condition_vv),
        then_vv_(then_vv),
        else_vv_(else_vv),
        result_type_(result_type),
        local_bitmap_idx_(local_bitmap_idx),
        is_terminal_else_(is_terminal_else) {}

  void Accept(DexVisitor &visitor) override { visitor.Visit(*this); }

  const ValueValidityPair &condition_vv() const { return *condition_vv_; }
  const ValueValidityPair &then_vv() const { return *then_vv_; }
  const ValueValidityPair &else_vv() const { return *else_vv_; }

  /// The validity of the result is saved in this bitmap.
  int local_bitmap_idx() const { return local_bitmap_idx_; }

  /// is this a terminal else ? i.e no nested if-else underneath.
  bool is_terminal_else() const { return is_terminal_else_; }

  const DataTypePtr &result_type() const { return result_type_; }

 private:
  ValueValidityPairPtr condition_vv_;
  ValueValidityPairPtr then_vv_;
  ValueValidityPairPtr else_vv_;
  DataTypePtr result_type_;
  int local_bitmap_idx_;
  bool is_terminal_else_;
};

// decomposed boolean expression.
class BooleanDex : public Dex {
 public:
  BooleanDex(const ValueValidityPairVector &args, int local_bitmap_idx)
      : args_(args), local_bitmap_idx_(local_bitmap_idx) {}

  const ValueValidityPairVector &args() const { return args_; }

  /// The validity of the result is saved in this bitmap.
  int local_bitmap_idx() const { return local_bitmap_idx_; }

 private:
  ValueValidityPairVector args_;
  int local_bitmap_idx_;
};

/// Boolean-AND expression
class BooleanAndDex : public BooleanDex {
 public:
  BooleanAndDex(const ValueValidityPairVector &args, int local_bitmap_idx)
      : BooleanDex(args, local_bitmap_idx) {}

  void Accept(DexVisitor &visitor) override { visitor.Visit(*this); }
};

/// Boolean-OR expression
class BooleanOrDex : public BooleanDex {
 public:
  BooleanOrDex(const ValueValidityPairVector &args, int local_bitmap_idx)
      : BooleanDex(args, local_bitmap_idx) {}

  void Accept(DexVisitor &visitor) override { visitor.Visit(*this); }
};

}  // namespace gandiva

#endif  // GANDIVA_DEX_DEX_H
