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
#ifndef GANDIVA_FUNCTION_REGISTRY_H
#define GANDIVA_FUNCTION_REGISTRY_H

#include <unordered_map>
#include "native_function.h"

namespace gandiva {

/*
 * Registry of pre-compiled functions (either IR or .so).
 */
class FunctionRegistry {
 public:
  static const NativeFunction *LookupSignature(const FunctionSignature *signature);

 private:
  struct KeyHash
  {
    std::size_t operator()(const FunctionSignature *k) const {
      return k->Hash();
    }
  };

  struct KeyEquals
  {
    bool operator() (const FunctionSignature *s1, const FunctionSignature *s2) const {
      return *s1 == *s2;
    }
  };

 private:
  static DataTypeSharedPtr time() {
    return arrow::time64(arrow::TimeUnit::MILLI);
  }

  static DataTypeSharedPtr timestamp() {
    return arrow::timestamp(arrow::TimeUnit::MILLI);
  }

  typedef std::unordered_map<const FunctionSignature *, const NativeFunction *, KeyHash, KeyEquals> SignatureMap;
  static SignatureMap InitPCMap();

  static NativeFunction pc_registry_[];
  static SignatureMap pc_registry_map_;
};

} // namespace gandiva

#endif //GANDIVA_FUNCTION_REGISTRY_H
