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
#include "codegen/native_function.h"

namespace gandiva {

///\brief Registry of pre-compiled IR functions.
class FunctionRegistry {
 public:
  /// Lookup a pre-compiled function by its signature.
  const NativeFunction *LookupSignature(const FunctionSignature &signature) const;

  /// Get the singleton instance of the registry.
  static FunctionRegistry& GetInstance() {
    /// Used for thread safety.
    static FunctionRegistry instance;
    return instance;
  }

 private:
  struct KeyHash {
    std::size_t operator()(const FunctionSignature *k) const {
      return k->Hash();
    }
  };

  struct KeyEquals {
    bool operator() (const FunctionSignature *s1, const FunctionSignature *s2) const {
      return *s1 == *s2;
    }
  };

 private:
  FunctionRegistry() {
    pc_registry_map_ = InitPCMap();
  }

  FunctionRegistry(const FunctionRegistry &functionRegistry);

  FunctionRegistry &operator=(const FunctionRegistry &functionRegistry);

  DataTypePtr time64() {
    return arrow::time64(arrow::TimeUnit::MICRO);
  }

  DataTypePtr timestamp64() {
    return arrow::timestamp(arrow::TimeUnit::MILLI);
  }

  using SignatureMap = std::unordered_map<const FunctionSignature *,
                             const NativeFunction *,
                             KeyHash, KeyEquals>;

  SignatureMap InitPCMap();

  SignatureMap pc_registry_map_;
};

} // namespace gandiva

#endif //GANDIVA_FUNCTION_REGISTRY_H
