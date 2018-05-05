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
#include "llvm_types.h"

namespace gandiva {

LLVMTypes::LLVMTypes(llvm::LLVMContext *context)
    : context_(context) {

  minor_to_native_type_map_ = {
      {common::BIT, i1_type()},
      {common::INT, i32_type()},
      {common::BIGINT, i64_type()},
      {common::FLOAT4, float_type()},
      {common::FLOAT8, double_type()},
      {common::DATE, i64_type()},
      {common::TIME, i64_type()},
      {common::TIMESTAMP, i64_type()},
  };
}

} // namespace gandiva
