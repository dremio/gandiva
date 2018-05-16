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

#include <gtest/gtest.h>
#include "llvm_generator.h"
#include "function_registry.h"
#include "codegen_exception.h"

namespace gandiva {

typedef int64_t (*add_vector_func_t)(int64_t *elements, int nelements);

class TestLLVMGenerator : public ::testing::Test {
 protected:
  FunctionRegistry registry_;
};

TEST_F(TestLLVMGenerator, TestAdd) {
  // Setup LLVM generator to do an arithmetic add of two vectors
  LLVMGenerator generator;
  Annotator annotator;

  auto field0 = std::make_shared<arrow::Field>("f0", arrow::int32());
  auto desc0 = annotator.CheckAndAddInputFieldDescriptor(field0);
  auto validity_dex0 = std::make_shared<VectorReadValidityDex>(desc0);
  auto value_dex0 = std::make_shared<VectorReadValueDex>(desc0);
  auto pair0 = std::make_shared<ValueValidityPair>(validity_dex0, value_dex0);

  auto field1 = std::make_shared<arrow::Field>("f1", arrow::int32());
  auto desc1 = annotator.CheckAndAddInputFieldDescriptor(field1);
  auto validity_dex1 = std::make_shared<VectorReadValidityDex>(desc1);
  auto value_dex1 = std::make_shared<VectorReadValueDex>(desc1);
  auto pair1 = std::make_shared<ValueValidityPair>(validity_dex1, value_dex1);

  std::vector<DataTypeSharedPtr> params{arrow::int32(), arrow::int32()};
  auto func_desc = std::make_shared<FuncDescriptor>("add", params, arrow::int32());
  FunctionSignature signature(func_desc->name(), func_desc->params(), func_desc->return_type());
  const NativeFunction *native_func = FunctionRegistry::LookupSignature(signature);

  std::vector<ValueValidityPairSharedPtr> pairs{pair0, pair1};
  auto func_dex = std::make_shared<NonNullableFuncDex>(func_desc, native_func, pairs);

  auto field_sum = std::make_shared<arrow::Field>("out", arrow::int32());
  auto desc_sum = annotator.CheckAndAddInputFieldDescriptor(field_sum);

  llvm::Function *ir_func = generator.CodeGenExprValue(func_dex, desc_sum, 0);

  generator.engine_->AddFunctionToCompile("eval_0");
  generator.engine_->FinalizeModule(false, true);

  eval_func_t eval_func = (eval_func_t)generator.engine_->CompiledFunction(ir_func);

  int num_records = 4;
  uint32_t a0[] = {1, 2, 3, 4};
  uint32_t a1[] = {5, 6, 7, 8};
  uint64_t in_bitmap = 0xffffffffffffffffull;

  uint32_t out[] = {0, 0, 0, 0};
  uint64_t out_bitmap = 0;

  uint8_t *addrs[] = {
    (uint8_t *)a0,
    (uint8_t *)&in_bitmap,
    (uint8_t *)a1,
    (uint8_t *)&in_bitmap,
    (uint8_t *)out,
    (uint8_t *)&out_bitmap,
  };
  eval_func(addrs, num_records);

  uint32_t expected[] = { 6, 8, 10, 12 };
  for (int i = 0; i  < num_records; i++) {
    EXPECT_EQ(expected[i], out[i]);
  }
}

TEST_F(TestLLVMGenerator, TestIntersectBitMaps) {
  uint64_t src_bitmaps[] = { 0xffbcabdcdfcab345ll,
                            0xabcd12345678cdefll,
                            0x12345678abcdefabll,
                            0x1122334455667788ll };

  uint64_t *src_bitmap_ptrs[] = {
    &src_bitmaps[0],
    &src_bitmaps[1],
    &src_bitmaps[2],
    &src_bitmaps[3],
  };
  uint64_t dst_bitmap;
  int nrecords = 16;

  LLVMGenerator::IntersectBitMaps(&dst_bitmap, src_bitmap_ptrs, 0, nrecords);
  EXPECT_EQ(dst_bitmap, 0xffffffffffffffffll);

  LLVMGenerator::IntersectBitMaps(&dst_bitmap, src_bitmap_ptrs, 1, nrecords);
  EXPECT_EQ(dst_bitmap, src_bitmaps[0]);

  LLVMGenerator::IntersectBitMaps(&dst_bitmap, src_bitmap_ptrs, 2, nrecords);
  EXPECT_EQ(dst_bitmap, src_bitmaps[0] & src_bitmaps[1]);

  LLVMGenerator::IntersectBitMaps(&dst_bitmap, src_bitmap_ptrs, 3, nrecords);
  EXPECT_EQ(dst_bitmap, src_bitmaps[0] & src_bitmaps[1] & src_bitmaps[2]);

  LLVMGenerator::IntersectBitMaps(&dst_bitmap, src_bitmap_ptrs, 4, nrecords);
  EXPECT_EQ(dst_bitmap, src_bitmaps[0] & src_bitmaps[1] & src_bitmaps[2] & src_bitmaps[3]);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

} // namespace gandiva
