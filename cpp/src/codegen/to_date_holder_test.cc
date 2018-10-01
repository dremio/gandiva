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

#include <memory>
#include <vector>

#include "../precompiled/epoch_time_point.h"
#include "codegen/execution_context.h"
#include "codegen/to_date_holder.h"

#include <gtest/gtest.h>

namespace gandiva {

class TestToDateHolder : public ::testing::Test {
 public:
  FunctionNode BuildToDate(std::string pattern) {
    auto field = std::make_shared<FieldNode>(arrow::field("in", arrow::utf8()));
    auto pattern_node =
        std::make_shared<LiteralNode>(arrow::utf8(), LiteralHolder(pattern), false);
    auto suppres_error_node =
        std::make_shared<LiteralNode>(arrow::int32(), LiteralHolder(0), false);
    return FunctionNode("to_date_utf8_utf8_int32",
                        {field, pattern_node, suppres_error_node}, arrow::int64());
  }
};

TEST_F(TestToDateHolder, TestSimpleDateTime) {
  std::shared_ptr<ToDateHolder> to_date_holder;

  auto status = ToDateHolder::Make("YYYY-MM-DD HH:MI:SS", 1, &to_date_holder);
  EXPECT_EQ(status.ok(), true) << status.message();
  ExecutionContext execution_context;
  auto &to_date = *to_date_holder;
  bool out_valid;
  int64_t millis_since_epoch =
      to_date("1986-12-01 01:01:01", true, (int64_t)&execution_context, &out_valid);
  EXPECT_EQ(millis_since_epoch, 533779200000);

  millis_since_epoch =
      to_date("1986-12-01 01:01:01.11", true, (int64_t)&execution_context, &out_valid);
  EXPECT_EQ(millis_since_epoch, 533779200000);

  millis_since_epoch =
      to_date("1986-12-01 01:01:01 +0800", true, (int64_t)&execution_context, &out_valid);
  EXPECT_EQ(millis_since_epoch, 533779200000);

  millis_since_epoch =
      to_date("1986-12-11 01:30:00", true, (int64_t)&execution_context, &out_valid);
  EXPECT_EQ(millis_since_epoch, 534643200000);
}

TEST_F(TestToDateHolder, TestSimpleDate) {
  std::shared_ptr<ToDateHolder> to_date_holder;

  auto status = ToDateHolder::Make("YYYY-MM-DD", 1, &to_date_holder);
  EXPECT_EQ(status.ok(), true) << status.message();
  ExecutionContext execution_context;
  auto &to_date = *to_date_holder;
  bool out_valid;
  int64_t millis_since_epoch =
      to_date("1986-12-01", true, (int64_t)&execution_context, &out_valid);
  EXPECT_EQ(millis_since_epoch, 533779200000);

  millis_since_epoch =
      to_date("1986-12-1", true, (int64_t)&execution_context, &out_valid);
  EXPECT_EQ(millis_since_epoch, 533779200000);

  millis_since_epoch =
      to_date("1886-12-1", true, (int64_t)&execution_context, &out_valid);
  EXPECT_EQ(millis_since_epoch, -2621894400000);

  millis_since_epoch =
      to_date("2012-12-1", true, (int64_t)&execution_context, &out_valid);
  EXPECT_EQ(millis_since_epoch, 1354320000000);

  // wrong month. should return 0 since we are suppresing errors.
  millis_since_epoch =
      to_date("1986-21-01 01:01:01 +0800", true, (int64_t)&execution_context, &out_valid);
  EXPECT_EQ(millis_since_epoch, 0);
}

TEST_F(TestToDateHolder, TestSimpleDateTimeError) {
  std::shared_ptr<ToDateHolder> to_date_holder;

  auto status = ToDateHolder::Make("YYYY-MM-DD HH:MI:SS", 0, &to_date_holder);
  EXPECT_EQ(status.ok(), true) << status.message();
  ExecutionContext execution_context;
  auto &to_date = *to_date_holder;
  bool out_valid;

  int64_t millis_since_epoch =
      to_date("1986-21-01 01:01:01 +0800", true, (int64_t)&execution_context, &out_valid);
  std::string expected_error =
      "Error parsing value 1986-21-01 01:01:01 +0800 for given format";
  EXPECT_TRUE(execution_context.get_error().find(expected_error) != std::string::npos)
      << status.message();

  ExecutionContext execution_context1;
  // not valid should not return error
  millis_since_epoch =
      to_date("nullptr", false, (int64_t)&execution_context1, &out_valid);
  EXPECT_EQ(millis_since_epoch, 0);
  EXPECT_TRUE(execution_context1.has_error() == false);
}

TEST_F(TestToDateHolder, TestSimpleDateTimeMakeError) {
  std::shared_ptr<ToDateHolder> to_date_holder;
  // reject time stamps for now.
  auto status = ToDateHolder::Make("YYYY-MM-DD HH:MI:SS tzo", 0, &to_date_holder);
  EXPECT_EQ(status.IsInvalid(), true) << status.message();
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace gandiva
