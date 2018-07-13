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
#ifndef GANDIVA_TYPES_H
#define GANDIVA_TYPES_H

#include <vector>

#include "gandiva/arrow.h"
#include "gandiva/function_signature.h"
#include "gandiva/gandiva_aliases.h"

namespace gandiva {

/// \brief Exports types supported by Gandiva for processing.
///
/// Has helper methods for clients to programatically discover
/// data types and functions supported by Gandiva.
class Types {
 public:
  static DataTypeVector supported_types() { return supported_types_; }
  // make the vector immutable.
  // compiler should use move, so no need for returning a ref.
  static const FuncSignatureVector supported_functions();

 private:
  static DataTypeVector supported_types_;
  static FuncSignatureVector supported_functions_;
  static DataTypeVector InitSupportedTypes();
  static void AddArrowTypeToVector(arrow::Type::type &type, DataTypeVector &vector);
};
}  // namespace gandiva
#endif  // GANDIVA_TYPES_H
