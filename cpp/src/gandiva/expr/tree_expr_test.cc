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
#include "tree_expr_builder.h"

using namespace arrow;

namespace gandiva {

class TestExprTree : public ::testing::Test {
  public:
    void SetUp() {
      i0_ = field("i0", int32());
      i1_ = field("i1", int32());

      b0_ = field("b0", boolean());
    }

  protected:
    FieldSharedPtr i0_; // int32
    FieldSharedPtr i1_; // int32

    FieldSharedPtr b0_; // bool
};

TEST_F(TestExprTree, TestLiteral) {
  auto n0 = TreeExprBuilder::MakeField(i0_);
  EXPECT_EQ(n0->getReturnType(), int32());

  auto n1 = TreeExprBuilder::MakeField(b0_);
  EXPECT_EQ(n1->getReturnType(), boolean());
}

TEST_F(TestExprTree, TestBinary) {
  auto left = TreeExprBuilder::MakeField(i0_);
  auto right = TreeExprBuilder::MakeField(i1_);

  auto n = TreeExprBuilder::MakeBinaryFunction("add", left, right, int32());
  auto add = std::dynamic_pointer_cast<FunctionNode>(n);

  EXPECT_EQ(add->getReturnType(), int32());
  EXPECT_TRUE(add->func_signature() == FunctionSignature("add", {int32(), int32()}, int32()));
}

TEST_F(TestExprTree, TestUnary) {
  auto arg = TreeExprBuilder::MakeField(i0_);
  auto n = TreeExprBuilder::MakeUnaryFunction("isPositive", arg, boolean());

  auto unaryFn = std::dynamic_pointer_cast<FunctionNode>(n);

  EXPECT_EQ(unaryFn->getReturnType(), boolean());
  EXPECT_TRUE(unaryFn->func_signature() == FunctionSignature("isPositive", {int32()}, boolean()));
}

TEST_F(TestExprTree, TestExpression) {
  auto left = TreeExprBuilder::MakeField(i0_);
  auto right = TreeExprBuilder::MakeField(i1_);

  auto n = TreeExprBuilder::MakeBinaryFunction("add", left, right, int32());
  auto e = TreeExprBuilder::MakeExpression(n, field("r", int32()));

  auto exp = std::dynamic_pointer_cast<NoOpExpression>(e);

  auto root_node = exp->node();
  EXPECT_EQ(root_node->getReturnType(), int32());

  auto add_node = std::dynamic_pointer_cast<FunctionNode>(root_node);
  EXPECT_TRUE(add_node->func_signature() == FunctionSignature("add", {int32(), int32()}, int32()));
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

} // namespace gandiva
