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
#ifndef GANDIVA_DEX_DEX_H
#define GANDIVA_DEX_DEX_H

#include <string>
#include <vector>
#include "common/gandiva_fwd.h"
#include "codegen/dex_visitor.h"
#include "codegen/field_descriptor.h"
#include "codegen/func_descriptor.h"
#include "codegen/native_function.h"
#include "codegen/value_validity_pair.h"

namespace gandiva {

/// \brief Decomposed expression : the validity and value are separated.
class Dex {
 public:
  virtual void Accept(DexVisitor *visitor) = 0;
  virtual ~Dex() = default;
};

// validity component of a ValueVector
class VectorReadValidityDex : public Dex {
 public:
  explicit VectorReadValidityDex(FieldDescriptorSharedPtr field_desc)
      : field_desc_(field_desc) {}

  int ValidityIdx() const {
    return field_desc_->validity_idx();
  }

  const std::string &FieldName() const {
    return field_desc_->Name();
  }

  const DataTypeSharedPtr FieldType() const {
    return field_desc_->Type();
  }

  const FieldSharedPtr Field() const {
    return field_desc_->field();
  }

  void Accept(DexVisitor *visitor) override {
    visitor->Visit(*this);
  }

 private:
  FieldDescriptorSharedPtr field_desc_;
};

// value component of a ValueVector
class VectorReadValueDex : public Dex {
 public:
  explicit VectorReadValueDex(FieldDescriptorSharedPtr field_desc)
  : field_desc_(field_desc) {}

  int DataIdx() const {
    return field_desc_->data_idx();
  }

  int OffsetsIdx() const {
    return field_desc_->offsets_idx();
  }

  const std::string &FieldName() const {
    return field_desc_->Name();
  }

  const DataTypeSharedPtr FieldType() const {
    return field_desc_->Type();
  }

  const FieldSharedPtr Field() const {
    return field_desc_->field();
  }

  void Accept(DexVisitor *visitor) override {
    visitor->Visit(*this);
  }

 private:
  FieldDescriptorSharedPtr field_desc_;
};

// base function expression
class FuncDex : public Dex {
 public:
  FuncDex(FuncDescriptorSharedPtr func_descriptor,
          const NativeFunction *native_function,
          const std::vector<ValueValidityPairSharedPtr> args)
      : func_descriptor_(func_descriptor),
        native_function_(native_function),
        args_(args) {}

  const FuncDescriptorSharedPtr func_descriptor() const { return func_descriptor_; }

  const NativeFunction *native_function() const { return native_function_; }

  const std::vector<ValueValidityPairSharedPtr> &args() const { return args_; }

 private:
  FuncDescriptorSharedPtr func_descriptor_;
  const NativeFunction *native_function_;
  std::vector<ValueValidityPairSharedPtr> args_;
};

// A function expression that only deals with non-null inputs, and generates non-null
// outputs.
class NonNullableFuncDex : public FuncDex {
 public:
  NonNullableFuncDex(FuncDescriptorSharedPtr func_descriptor,
                     const NativeFunction *native_function,
                     const std::vector<ValueValidityPairSharedPtr> &args)
      : FuncDex(func_descriptor, native_function, args) {}

  void Accept(DexVisitor *visitor) override {
    visitor->Visit(*this);
  }
};

// A function expression that deals with nullable inputs, but generates non-null
// outputs.
class NullableNeverFuncDex : public FuncDex {
 public:
  NullableNeverFuncDex(FuncDescriptorSharedPtr func_descriptor,
                       const NativeFunction *native_function,
                       const std::vector<ValueValidityPairSharedPtr> &args)
      : FuncDex(func_descriptor, native_function, args) {}

  void Accept(DexVisitor *visitor) override {
    visitor->Visit(*this);
  }
};

// decomposed expression for a literal.
class LiteralDex : public Dex {
 public:
  explicit LiteralDex(const DataTypeSharedPtr type)
      : type_(type) {}

  const DataTypeSharedPtr type() const {
    return type_;
  }

  void Accept(DexVisitor *visitor) override {
    visitor->Visit(*this);
  }

 private:
  const DataTypeSharedPtr type_;
};


} // namespace gandiva

#endif //GANDIVA_DEX_DEX_H
