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
#include "codegen_exception.h"

namespace gandiva {

typedef int64_t (*add_vector_func_t)(int64_t *elements, int nelements);

class TestLLVMGenerator : public ::testing::Test {
};

#if 0
TEST_F(TestLLVMGenerator, TestAdd) {
  // Setup LLVM generator to do an arithmetic add of two vectors
  LLVMGenerator generator;

  FieldSharedPtr field0 = std::make_shared<Field>("f0", arrow::int32());
  FieldDescriptorSharedPtr desc0 = std::make_shared<FieldDescriptor>(field0, 1, 0);
  VectorReadValidityDex validity_dex0 = std::make_shared<VectorReadValidityDex>(desc0);
  VectorReadValueDex value_dex0 = std::make_shared<VectorReadValueDex>(desc0);

  FieldSharedPtr field1 = std::make_shared<Field>("f1", arrow::int32());
  FieldDescriptorSharedPtr desc1 = std::make_shared<FieldDescriptor>(field1, 3, 2);
  VectorReadValidityDex validity_dex1 = std::make_shared<VectorReadValidityDex>(desc1);
  VectorReadValueDex value_dex1 = std::make_shared<VectorReadValueDex>(desc1);

  FuncDescriptor func = std::make_shared("add", {arrow::int32(), arrow::int32()}, arrow::int32());
  NativeFunction *native_function =
  NonNullableFuncDex func_dex = std::make_shared<>(func, native_func, args);
}
#endif

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
