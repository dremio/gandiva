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

#include "gandiva/selection_vector.h"

#include <memory>

#include <gtest/gtest.h>

namespace gandiva {

class TestSelectionVector : public ::testing::Test {
 protected:
  virtual void SetUp() { pool_ = arrow::default_memory_pool(); }

  arrow::MemoryPool *pool_;
};

TEST_F(TestSelectionVector, TestInt16Make) {
  int max_slots = 10;

  // Test with pool allocation
  std::shared_ptr<SelectionVectorInt16> selection;
  auto status = SelectionVectorInt16::Make(max_slots, pool_, &selection);
  EXPECT_EQ(status.ok(), true);
  EXPECT_EQ(selection->GetMaxSlots(), max_slots);
  EXPECT_EQ(selection->GetNumSlots(), 0);

  // Test with pre-alloced buffer
  std::shared_ptr<SelectionVectorInt16> selection2;
  auto buffer = std::make_shared<arrow::PoolBuffer>(pool_);
  auto buffer_len = max_slots * sizeof(int16_t);
  auto astatus = buffer->Resize(buffer_len);
  EXPECT_EQ(astatus.ok(), true);

  status = SelectionVectorInt16::Make(max_slots, buffer, &selection2);
  EXPECT_EQ(status.ok(), true);
  EXPECT_EQ(selection2->GetMaxSlots(), max_slots);
  EXPECT_EQ(selection2->GetNumSlots(), 0);
}

TEST_F(TestSelectionVector, TestInt16Set) {
  int max_slots = 10;

  std::shared_ptr<SelectionVectorInt16> selection;
  auto status = SelectionVectorInt16::Make(max_slots, pool_, &selection);
  EXPECT_EQ(status.ok(), true);

  selection->SetIndex(0, 100);
  EXPECT_EQ(selection->GetIndex(0), 100);

  selection->SetIndex(1, 200);
  EXPECT_EQ(selection->GetIndex(1), 200);

  selection->SetNumSlots(2);
  EXPECT_EQ(selection->GetNumSlots(), 2);

  // TopArray() should return an array with 100,200
  auto array_raw = selection->ToArray();
  const auto &array = dynamic_cast<const arrow::Int16Array &>(*array_raw);
  EXPECT_EQ(array.length(), 2) << array_raw->ToString();
  EXPECT_EQ(array.Value(0), 100) << array_raw->ToString();
  EXPECT_EQ(array.Value(1), 200) << array_raw->ToString();
}

TEST_F(TestSelectionVector, TestInt16PopulateFromBitMap) {
  int max_slots = 200;

  std::shared_ptr<SelectionVectorInt16> selection;
  auto status = SelectionVectorInt16::Make(max_slots, pool_, &selection);
  EXPECT_EQ(status.ok(), true);

  int bitmap_size = arrow::BitUtil::RoundUpNumi64(max_slots) * 8;
  std::unique_ptr<uint8_t> bitmap(new uint8_t[bitmap_size]);
  memset(bitmap.get(), 0, bitmap_size);

  arrow::BitUtil::SetBit(bitmap.get(), 0);
  arrow::BitUtil::SetBit(bitmap.get(), 5);
  arrow::BitUtil::SetBit(bitmap.get(), 121);
  arrow::BitUtil::SetBit(bitmap.get(), 220);

  status = selection->PopulateFromBitMap(bitmap.get(), bitmap_size, max_slots - 1);
  EXPECT_EQ(selection->GetNumSlots(), 3);
  EXPECT_EQ(selection->GetIndex(0), 0);
  EXPECT_EQ(selection->GetIndex(1), 5);
  EXPECT_EQ(selection->GetIndex(2), 121);
}

TEST_F(TestSelectionVector, TestInt32Set) {
  int max_slots = 10;

  std::shared_ptr<SelectionVectorInt32> selection;
  auto status = SelectionVectorInt32::Make(max_slots, pool_, &selection);
  EXPECT_EQ(status.ok(), true);

  selection->SetIndex(0, 100);
  EXPECT_EQ(selection->GetIndex(0), 100);

  selection->SetIndex(1, 200);
  EXPECT_EQ(selection->GetIndex(1), 200);

  selection->SetIndex(2, 100000);
  EXPECT_EQ(selection->GetIndex(2), 100000);

  selection->SetNumSlots(3);
  EXPECT_EQ(selection->GetNumSlots(), 3);

  // TopArray() should return an array with 100,200,100000
  auto array_raw = selection->ToArray();
  const auto &array = dynamic_cast<const arrow::Int32Array &>(*array_raw);
  EXPECT_EQ(array.length(), 3) << array_raw->ToString();
  EXPECT_EQ(array.Value(0), 100) << array_raw->ToString();
  EXPECT_EQ(array.Value(1), 200) << array_raw->ToString();
  EXPECT_EQ(array.Value(2), 100000) << array_raw->ToString();
}

TEST_F(TestSelectionVector, TestInt32PopulateFromBitMap) {
  int max_slots = 200;

  std::shared_ptr<SelectionVectorInt32> selection;
  auto status = SelectionVectorInt32::Make(max_slots, pool_, &selection);
  EXPECT_EQ(status.ok(), true);

  int bitmap_size = arrow::BitUtil::RoundUpNumi64(max_slots) * 8;
  std::unique_ptr<uint8_t> bitmap(new uint8_t[bitmap_size]);
  memset(bitmap.get(), 0, bitmap_size);

  arrow::BitUtil::SetBit(bitmap.get(), 0);
  arrow::BitUtil::SetBit(bitmap.get(), 5);
  arrow::BitUtil::SetBit(bitmap.get(), 121);
  arrow::BitUtil::SetBit(bitmap.get(), 220);

  status = selection->PopulateFromBitMap(bitmap.get(), bitmap_size, max_slots - 1);
  EXPECT_EQ(selection->GetNumSlots(), 3);
  EXPECT_EQ(selection->GetIndex(0), 0);
  EXPECT_EQ(selection->GetIndex(1), 5);
  EXPECT_EQ(selection->GetIndex(2), 121);
}

}  // namespace gandiva
