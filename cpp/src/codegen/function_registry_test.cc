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

#include "codegen/function_registry.h"

#include <gtest/gtest.h>

namespace gandiva {

class TestFunctionRegistry : public ::testing::Test {
 protected:
  FunctionRegistry registry_;
};

TEST_F(TestFunctionRegistry, TestFound) {
  FunctionSignature add_i32_i32("add", {arrow::int32(), arrow::int32()}, arrow::int32());

  const NativeFunction *function = registry_.LookupSignature(add_i32_i32);
  EXPECT_NE(function, nullptr);
  EXPECT_EQ(function->signature(), add_i32_i32);
  EXPECT_EQ(function->pc_name(), "add_int32_int32");
}

TEST_F(TestFunctionRegistry, TestNotFound) {
  FunctionSignature addX_i32_i32("addX", {arrow::int32(), arrow::int32()},
                                 arrow::int32());
  EXPECT_EQ(registry_.LookupSignature(addX_i32_i32), nullptr);

  FunctionSignature add_i32_i32_ret64("add", {arrow::int32(), arrow::int32()},
                                      arrow::int64());
  EXPECT_EQ(registry_.LookupSignature(add_i32_i32_ret64), nullptr);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace gandiva
