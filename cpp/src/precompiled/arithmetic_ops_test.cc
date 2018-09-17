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

#include <gtest/gtest.h>
#include "precompiled/types.h"

namespace gandiva {

TEST(TestArithmeticOps, TestIsDistinctFrom) {
  EXPECT_EQ(is_distinct_from_timestamp_timestamp((int64)1000, true, (int64)1000, false),
            true);
  EXPECT_EQ(is_distinct_from_timestamp_timestamp((int64)1000, false, (int64)1000, true),
            true);
  EXPECT_EQ(is_distinct_from_timestamp_timestamp((int64)1000, false, (int64)1000, false),
            false);
  EXPECT_EQ(is_distinct_from_timestamp_timestamp((int64)1000, true, (int64)1000, true),
            false);

  EXPECT_EQ(is_not_distinct_from_int32_int32(1000, true, 1000, false), false);
  EXPECT_EQ(is_not_distinct_from_int32_int32(1000, false, 1000, true), false);
  EXPECT_EQ(is_not_distinct_from_int32_int32(1000, false, 1000, false), true);
  EXPECT_EQ(is_not_distinct_from_int32_int32(1000, true, 1000, true), true);
}

TEST(TestArithmeticOps, TestMod) { EXPECT_EQ(mod_int64_int32(10, 0), 10); }

TEST(TestArithmeticOps, TestDivide) {
  boolean is_valid;
  int64 out = divide_int64_int64(10, true, 0, true, &is_valid);
  EXPECT_EQ(out, 0);
  EXPECT_EQ(is_valid, false);

  out = divide_int64_int64(10, true, 2, false, &is_valid);
  EXPECT_EQ(out, 0);
  EXPECT_EQ(is_valid, false);

  out = divide_int64_int64(10, true, 2, true, &is_valid);
  EXPECT_EQ(out, 5);
  EXPECT_EQ(is_valid, true);
}

}  // namespace gandiva
