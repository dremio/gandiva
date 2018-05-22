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

#include <vector>
#include "codegen/function_registry.h"

namespace gandiva {

using std::vector;
using arrow::int32;
using arrow::int64;
using arrow::float32;
using arrow::float64;
using arrow::boolean;
using arrow::date64;

#define STRINGIFY(a) #a

#define BINARY_SYMMETRIC_SAFE_NULL_IF_NULL(NAME, TYPE) \
  NativeFunction(#NAME, \
    vector<DataTypeSharedPtr>{TYPE(), TYPE()}, \
    TYPE(), \
    true, \
    RESULT_NULL_IF_NULL, \
    STRINGIFY(NAME##_##TYPE##_##TYPE))

#define BINARY_GENERIC_SAFE_NULL_IF_NULL(NAME, IN_TYPE1, IN_TYPE2, OUT_TYPE) \
  NativeFunction(#NAME, \
    vector<DataTypeSharedPtr>{IN_TYPE1(), IN_TYPE2()}, \
    OUT_TYPE(), \
    true, \
    RESULT_NULL_IF_NULL, \
    STRINGIFY(NAME##_##IN_TYPE1##_##IN_TYPE2))

#define BINARY_RELATIONAL_SAFE_NULL_IF_NULL(NAME, TYPE) \
  NativeFunction(#NAME, \
    vector<DataTypeSharedPtr>{TYPE(), TYPE()}, \
    boolean(), \
    true, \
    RESULT_NULL_IF_NULL, \
    STRINGIFY(NAME##_##TYPE##_##TYPE))

#define CAST_UNARY_SAFE_NULL_IF_NULL(NAME, IN_TYPE, OUT_TYPE) \
  NativeFunction(#NAME, \
    vector<DataTypeSharedPtr>{IN_TYPE()}, \
    OUT_TYPE(), \
    true, \
    RESULT_NULL_IF_NULL, \
    STRINGIFY(NAME##_##IN_TYPE))

#define UNARY_SAFE_NULL_NEVER_BOOL(NAME, TYPE) \
  NativeFunction(#NAME, \
    vector<DataTypeSharedPtr>{TYPE()}, \
    boolean(), \
    true, \
    RESULT_NULL_NEVER, \
    STRINGIFY(NAME##_##TYPE))

#define EXTRACT_SAFE_NULL_IF_NULL(NAME, TYPE) \
  NativeFunction(#NAME, \
    vector<DataTypeSharedPtr>{TYPE()}, \
    int64(), \
    true, \
    RESULT_NULL_IF_NULL, \
    STRINGIFY(NAME##_##TYPE))

#define NUMERIC_TYPES(INNER, NAME) \
  INNER(NAME, int32), \
  INNER(NAME, int64), \
  INNER(NAME, float32), \
  INNER(NAME, float64)

#define NUMERIC_AND_BOOL_TYPES(INNER, NAME) \
  NUMERIC_TYPES(INNER, NAME), \
  INNER(NAME, boolean)

#define DATE_TYPES(INNER, NAME) \
  INNER(NAME, date64), \
  INNER(NAME, time64), \
  INNER(NAME, timestamp64)

/*
 * list of registered native functions.
 */
NativeFunction FunctionRegistry::pc_registry_[] = {
    /* Arithmetic operations */
    NUMERIC_TYPES(BINARY_SYMMETRIC_SAFE_NULL_IF_NULL, add),
    NUMERIC_TYPES(BINARY_SYMMETRIC_SAFE_NULL_IF_NULL, subtract),
    NUMERIC_TYPES(BINARY_SYMMETRIC_SAFE_NULL_IF_NULL, multiply),
    NUMERIC_TYPES(BINARY_SYMMETRIC_SAFE_NULL_IF_NULL, divide),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(mod, int64, int32, int32),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(mod, int64, int64, int64),
    NUMERIC_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, equal),
    NUMERIC_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, not_equal),
    NUMERIC_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, less_than),
    NUMERIC_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, less_than_or_equal_to),
    NUMERIC_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, greater_than),
    NUMERIC_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, greater_than_or_equal_to),

    /* cast operations */
    CAST_UNARY_SAFE_NULL_IF_NULL(castBIGINT, int32, int64),
    CAST_UNARY_SAFE_NULL_IF_NULL(castFLOAT4, int32, float32),
    CAST_UNARY_SAFE_NULL_IF_NULL(castFLOAT4, int64, float32),
    CAST_UNARY_SAFE_NULL_IF_NULL(castFLOAT8, int32, float64),
    CAST_UNARY_SAFE_NULL_IF_NULL(castFLOAT8, int64, float64),
    CAST_UNARY_SAFE_NULL_IF_NULL(castFLOAT8, float32, float64),

    /* nullable never operations */
    NUMERIC_AND_BOOL_TYPES(UNARY_SAFE_NULL_NEVER_BOOL, isnull),
    NUMERIC_AND_BOOL_TYPES(UNARY_SAFE_NULL_NEVER_BOOL, isnotnull),
    NUMERIC_AND_BOOL_TYPES(UNARY_SAFE_NULL_NEVER_BOOL, isnumeric),

    // date/time operations */
    DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractYear),
    DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractMonth),
    DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractDay),
    DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractHour),
    DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractMinute),
};

FunctionRegistry::SignatureMap FunctionRegistry::pc_registry_map_ = InitPCMap();

FunctionRegistry::SignatureMap FunctionRegistry::InitPCMap() {
  SignatureMap map;

  int num_entries = sizeof (pc_registry_) / sizeof (NativeFunction);
  printf("Registry has %d pre-compiled functions\n", num_entries);

  for (int i = 0; i < num_entries; i++) {
    const NativeFunction *entry = &pc_registry_[i];

    DCHECK(map.find(&entry->signature()) == map.end());
    map[&entry->signature()] = entry;
    //printf("%s -> %s\n", entry->signature().ToString().c_str(),
    //      entry->pc_name().c_str());
  }
  return map;
}

const NativeFunction *FunctionRegistry::LookupSignature(
    const FunctionSignature &signature) const {
  auto got = pc_registry_map_.find(&signature);
  return got == pc_registry_map_.end() ? NULL : got->second;
}

} // namespace gandiva

