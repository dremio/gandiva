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

#include "gandiva/filter.h"
#include <gtest/gtest.h>
#include "arrow/memory_pool.h"
#include "gandiva/tree_expr_builder.h"
#include "integ/test_util.h"

namespace gandiva {

using arrow::boolean;
using arrow::float32;
using arrow::int32;

class TestFilter : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(TestFilter, TestSimple) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f1", int32());
  auto schema = arrow::schema({field0, field1});

  // Build condition f0 + f1 < 10
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  auto node_f1 = TreeExprBuilder::MakeField(field1);
  auto sum_func =
      TreeExprBuilder::MakeFunction("add", {node_f0, node_f1}, arrow::int32());
  auto literal_10 = TreeExprBuilder::MakeLiteral((int32_t)10);
  auto less_than_10 = TreeExprBuilder::MakeFunction("less_than", {sum_func, literal_10},
                                                    arrow::boolean());
  auto condition = TreeExprBuilder::MakeCondition(less_than_10);

  std::shared_ptr<Filter> filter;
  Status status = Filter::Make(schema, condition, pool_, &filter);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 5;
  auto array0 = MakeArrowArrayInt32({1, 2, 3, 4, 6}, {true, true, true, false, true});
  auto array1 = MakeArrowArrayInt32({5, 9, 6, 17, 3}, {true, true, false, true, true});
  // expected output (indices for which condition matches)
  auto exp = MakeArrowArrayInt16({0, 4});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  // Evaluate expression
  ArrayPtr output;
  status = filter->Evaluate(*in_batch, &output);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, output);
}

TEST_F(TestFilter, TestSimpleCustomConfig) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f1", int32());
  auto schema = arrow::schema({field0, field1});

  // Build condition f0 != f1
  auto condition = TreeExprBuilder::MakeCondition("not_equal", {field0, field1});

  std::shared_ptr<Filter> projector;
  ConfigurationBuilder config_builder;
  std::shared_ptr<Configuration> config = config_builder.build();

  std::shared_ptr<Filter> filter;
  Status status = Filter::Make(schema, condition, pool_, config, &filter);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array0 = MakeArrowArrayInt32({1, 2, 3, 4}, {true, true, true, false});
  auto array1 = MakeArrowArrayInt32({11, 2, 3, 17}, {true, true, false, true});
  // expected output
  auto exp = MakeArrowArrayInt16({0});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  // Evaluate expression
  ArrayPtr output;
  status = filter->Evaluate(*in_batch, &output);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, output);
}

TEST_F(TestFilter, TestZeroCopy) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto schema = arrow::schema({field0});

  // Build condition
  auto condition = TreeExprBuilder::MakeCondition("isnotnull", {field0});

  std::shared_ptr<Filter> filter;
  Status status = Filter::Make(schema, condition, pool_, &filter);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array0 = MakeArrowArrayInt32({1, 2, 3, 4}, {true, true, true, false});
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0});

  // expected output
  auto exp = MakeArrowArrayInt16({0, 1, 2});

  // allocate selection buffers
  int64_t data_sz = sizeof(int16_t) * num_records;
  std::unique_ptr<uint8_t[]> data(new uint8_t[data_sz]);
  std::shared_ptr<arrow::MutableBuffer> data_buf =
      std::make_shared<arrow::MutableBuffer>(data.get(), data_sz);

  auto array_data =
      arrow::ArrayData::Make(arrow::int16(), num_records, {nullptr, data_buf});

  // Evaluate expression
  ArrayPtr output;
  status = filter->Evaluate(*in_batch, {array_data}, &output);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, output);
}

TEST_F(TestFilter, TestZeroCopyNegative) {
  ArrayPtr output;

  // schema for input fields
  auto field0 = field("f0", int32());
  auto schema = arrow::schema({field0});

  // Build expression
  auto condition = TreeExprBuilder::MakeCondition("isnotnull", {field0});

  std::shared_ptr<Filter> filter;
  Status status = Filter::Make(schema, {condition}, nullptr /*pool*/, &filter);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array0 = MakeArrowArrayInt32({1, 2, 3, 4}, {true, true, true, false});
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0});

  // expected output
  auto exp = MakeArrowArrayInt16({0, 1, 2});

  // allocate output buffers
  int64_t data_sz = sizeof(int16_t) * num_records;
  std::unique_ptr<uint8_t[]> data(new uint8_t[data_sz]);
  std::shared_ptr<arrow::MutableBuffer> data_buf =
      std::make_shared<arrow::MutableBuffer>(data.get(), data_sz);

  auto array_data = arrow::ArrayData::Make(arrow::int16(), num_records, {nullptr, data_buf});

  // the batch can't be empty.
  auto bad_batch = arrow::RecordBatch::Make(schema, 0 /*num_records*/, {array0});
  status = filter->Evaluate(*bad_batch, {array_data}, &output);
  EXPECT_EQ(status.code(), StatusCode::Invalid);

  // the output array can't be null.
  std::shared_ptr<arrow::ArrayData> null_array_data;
  status = filter->Evaluate(*in_batch, {null_array_data}, &output);
  EXPECT_EQ(status.code(), StatusCode::Invalid);

  // the output array must have atleast two buffers.
  auto bad_array_data = arrow::ArrayData::Make(arrow::int16(), num_records, {data_buf});
  status = filter->Evaluate(*in_batch, {bad_array_data}, &output);
  EXPECT_EQ(status.code(), StatusCode::Invalid);

  // the output buffer must have sufficiently sized data_buf.
  std::shared_ptr<arrow::MutableBuffer> bad_data_buf =
      std::make_shared<arrow::MutableBuffer>(data.get(), data_sz - 1);
  auto bad_array_data2 =
      arrow::ArrayData::Make(arrow::int16(), num_records, {nullptr, bad_data_buf});
  status = filter->Evaluate(*in_batch, {bad_array_data2}, &output);
  EXPECT_EQ(status.code(), StatusCode::Invalid);

  // the output buffer must be of type int16.
  auto bad_array_data3 =
      arrow::ArrayData::Make(arrow::int8(), num_records, {nullptr, data_buf});
  status = filter->Evaluate(*in_batch, {bad_array_data3}, &output);
  EXPECT_EQ(status.code(), StatusCode::Invalid);
}

}  // namespace gandiva
