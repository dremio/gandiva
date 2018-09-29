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
#include "arrow/memory_pool.h"
#include "gandiva/filter.h"
#include "gandiva/projector.h"
#include "gandiva/tree_expr_builder.h"
#include "integ/test_util.h"

namespace gandiva {

using arrow::boolean;
using arrow::float32;
using arrow::int32;

class TestFilterProject : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(TestFilterProject, TestSimple) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f1", int32());
  auto field2 = field("f2", int32());
  auto schema = arrow::schema({field0, field1});

  // Build condition f0 < f1
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  auto node_f1 = TreeExprBuilder::MakeField(field1);
  auto less_than_function =
      TreeExprBuilder::MakeFunction("less_than", {node_f0, node_f1}, arrow::boolean());
  auto condition = TreeExprBuilder::MakeCondition(less_than_function);
  auto sum_expr = TreeExprBuilder::MakeExpression("add", {field0, field1}, field2);

  std::shared_ptr<Filter> filter;
  std::shared_ptr<Projector> projector;

  Status status = Filter::Make(schema, condition, &filter);
  EXPECT_TRUE(status.ok());

  status = Projector::Make(schema, {sum_expr}, &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 5;
  auto array0 = MakeArrowArrayInt32({1, 2, 6, 40, 3}, {true, true, true, true, true});
  auto array1 = MakeArrowArrayInt32({5, 9, 3, 17, 6}, {true, true, true, true, true});
  // expected output
  auto result = MakeArrowArrayInt32({6, 11, 9}, {true, true, true});
  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  std::shared_ptr<SelectionVector> selection_vector;
  status = SelectionVector::MakeInt16(num_records, pool_, &selection_vector);
  EXPECT_TRUE(status.ok());

  // Evaluate expression
  status = filter->Evaluate(*in_batch, selection_vector);
  EXPECT_TRUE(status.ok());

  // Evaluate expression
  arrow::ArrayVector outputs;
  int mode = 0;

  in_batch =
      arrow::RecordBatch::Make(schema, selection_vector->GetNumSlots(), {array0, array1});

  auto data_buffer = selection_vector->ToArray()->data()->buffers[1];
  status = projector->Evaluate(*in_batch, mode, *data_buffer, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(result, outputs.at(0));

  // Validate results
}
}  // namespace gandiva
