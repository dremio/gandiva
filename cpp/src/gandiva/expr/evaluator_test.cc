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
#include <utility>
#include <gtest/gtest.h>
#include "arrow/memory_pool.h"
#include "arrow/test-util.h"
#include "expr/evaluator.h"
#include "expr/tree_expr_builder.h"

namespace gandiva {

using arrow::int32;
using arrow::Status;

class TestEvaluator : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

#if 0
// Added to compare Array buffers on our own
static bool Compare(ArraySharedPtr src, ArraySharedPtr dst) {
  if (src->length() != dst->length()) {
    std::cout << "Lengths differ\n";
    return false;
  }

  auto length = src->length();
  for (auto i = 0; i < length; i++) {
    if (src->IsValid(i) != dst->IsValid(i)) {
      std::cout << "Validity bits differ at " << i << "\n";
      return false;
    }
  }

  return true;
}
#endif

/*
 * Helper function to create an arrow-array of type ARROWTYPE
 * from primitive vectors of data & validity.
 *
 * arrow/test-util.h has good utility classes for this purpose.
 * Using those
 */
static void MakeArrowArrayInt32(std::vector<int32_t> values,
                                std::vector<bool> validity,
                                ArraySharedPtr *out) {
  arrow::ArrayFromVector<arrow::Int32Type, int32_t>(validity, values, out);
}


TEST_F(TestEvaluator, TestSumSub) {
  /* schema for input/output fields */
  auto field0 = field("f0", int32());
  auto field1 = field("f2", int32());
  auto in_schema = arrow::schema({field0, field1});

  auto field_sum = field("add", int32());
  auto field_sub = field("subtract", int32());
  auto out_schema = arrow::schema({field_sum, field_sub});

  /* sample data */
  ArraySharedPtr arrow_v0, arrow_v1, arrow_exp_sum, arrow_exp_sub;
  MakeArrowArrayInt32({ 1, 2, 3, 4},
                      { true, true, true, false },
                      &arrow_v0);
  MakeArrowArrayInt32({ 11, 13, 15, 17},
                      { true, true, false, true },
                      &arrow_v1);

  /* prepare input record batch */
  arrow::ArrayVector array_vector = {arrow_v0, arrow_v1};
  auto in_batch = arrow::RecordBatch::Make(in_schema, 4, array_vector);

  /* expected output */
  MakeArrowArrayInt32({ 12, 15, 0, 0},
                      { true, true, false, false },
                      &arrow_exp_sum);

  MakeArrowArrayInt32({ -10, -11, 0, 0},
                      { true, true, false, false },
                      &arrow_exp_sub);

  /*
   * output builders
   */

  std::unique_ptr<arrow::ArrayBuilder> sum_array_builder;
  Status ret = MakeBuilder(pool_, int32(), &sum_array_builder);
  assert(ret.ok());

  // make arrow::ArrayVector for the output
  arrow::ArrayVector output;
  ArraySharedPtr add_out, sub_out;
  MakeArrowArrayInt32({0, 0, 0, 0}, {false, false, false, false}, &add_out);
  MakeArrowArrayInt32({0, 0, 0, 0}, {false, false, false, false}, &sub_out);

  output.push_back(add_out);
  output.push_back(sub_out);

  /*
   * Build expression
   */
  auto node0 = TreeExprBuilder::MakeField(field0);
  auto node1 = TreeExprBuilder::MakeField(field1);
  auto func_sum = TreeExprBuilder::MakeBinaryFunction("add", node0, node1,
                                                      int32() /*outType*/);
  auto sum_expr = TreeExprBuilder::MakeExpression(func_sum /*expression root */,
                                                  field_sum /*output field*/);

  auto func_sub = TreeExprBuilder::MakeBinaryFunction("subtract", node0, node1,
                                                      int32() /*outType*/);
  auto sub_expr = TreeExprBuilder::MakeExpression(func_sub, field_sub);


  /*
   * Evaluate expression
   */
  auto evaluator = Evaluator::Make(in_schema, {sum_expr, sub_expr});
  evaluator->Evaluate(in_batch, output);

  /*
   * Validate results
   */
  // TODO: Need to figure out why this fails without the Slice
  // This also succeeds with RangeEquals(output.at(0), 0, 3, 0)
  EXPECT_TRUE(arrow_exp_sum->Equals(output.at(0)->Slice(0, 4)));
  EXPECT_TRUE(arrow_exp_sub->Equals(output.at(1)->Slice(0, 4)));
}

} // namespace gandiva
