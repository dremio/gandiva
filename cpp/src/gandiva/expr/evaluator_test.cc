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
#include "arrow/memory_pool.h"
#include "evaluator.h"
#include "tree_expr_builder.h"

using namespace arrow;

namespace gandiva {

class TestEvaluator : public ::testing::Test {
 public:
  void SetUp() { pool_ = default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

/*
 * Helper function to create an arrow-array of type ARROWTYPE
 * from primitive vectors of data & validity.
 */
template<typename ARROWTYPE, typename CTYPE>
static ArraySharedPtr MakeArrowArray(std::vector<CTYPE> values,
                                      std::vector<bool> validity) {
  return NULL;
}

#define MakeArrowArrayInt32 MakeArrowArray<Int32Type, int32_t>


TEST_F(TestEvaluator, TestSumSub) {
  /* schema for input/output fields */
  auto field0 = field("f0", int32());
  auto field1 = field("f2", int32());
  auto in_schema = schema({field0, field1});

  auto field_sum = field("sum", int32());
  auto field_sub = field("sub", int32());
  auto out_schema = schema({field_sum, field_sub});

  /* sample data */
  auto arrow_v0 = MakeArrowArrayInt32({ 1, 2, 3, 4},
                                      { true, true, true, false });
  auto arrow_v1 = MakeArrowArrayInt32({ 11, 13, 15, 17},
                                      { true, true, false, true });

  /* prepare input record batch */
  ArrayVector array_vector = {arrow_v0, arrow_v1};
  auto in_batch = RecordBatch::Make(in_schema, 4, array_vector);

  /* expected output */
  auto arrow_exp_sum = MakeArrowArrayInt32({ 12, 15, 0, 0},
                                           { true, true, false, false });

  auto arrow_exp_sub = MakeArrowArrayInt32({ -10, -11, 0, 0},
                                           { true, true, false, false });

  /*
   * output builders
   */

  std::unique_ptr<ArrayBuilder> sum_array_builder;
  Status ret = MakeBuilder(pool_, int32(), &sum_array_builder);
  assert(ret.ok());

  std::unique_ptr<ArrayBuilder> sub_array_builder;
  ret = MakeBuilder(pool_, int32(), &sub_array_builder);
  assert(ret.ok());

  std::vector<std::unique_ptr<ArrayBuilder>> builders(2);
  builders[0] = std::move(sum_array_builder);
  builders[1] = std::move(sub_array_builder);

  /*
   * Build expression
   */
  auto node0 = TreeExprBuilder::MakeField(field0);
  auto node1 = TreeExprBuilder::MakeField(field1);
  auto func_sum = TreeExprBuilder::MakeBinaryFunction("sum", node0, node1,
                                                      int32() /*outType*/);
  auto sum_expr = TreeExprBuilder::MakeExpression(func_sum /*expression root */,
                                                  field_sum /*output field*/);

  auto func_sub = TreeExprBuilder::MakeBinaryFunction("sub", node0, node1,
                                                      int32() /*outType*/);
  auto sub_expr = TreeExprBuilder::MakeExpression(func_sub, field_sub);


  /*
   * Evaluate expression
   */
  auto evaluator = Evaluator::Make(in_schema, {sum_expr, sub_expr});
  evaluator->Evaluate(in_batch, builders);

  /*
   * Validate results
   */
  ArraySharedPtr arrow_sum;
  ret = sum_array_builder->Finish(&arrow_sum);
  assert(ret.ok());
  EXPECT_TRUE(arrow_sum->Equals(arrow_exp_sum));

  ArraySharedPtr arrow_sub;
  ret = sub_array_builder->Finish(&arrow_sub);
  assert(ret.ok());
  EXPECT_TRUE(arrow_sub->Equals(arrow_exp_sub));
}

} // namespace gandiva
