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
#include <jni.h>
#include "arrow/memory_pool.h"
#include "gandiva/projector.h"
#include "gandiva/tree_expr_builder.h"
#include "id_to_module_map.h"
#include "module_holder.h"

namespace gandiva {

using arrow::boolean;
using arrow::float32;
using arrow::int32;

class TestProjectorHolderMap : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(TestProjectorHolderMap, TestMapLookUp) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f2", int32());
  auto field2 = field("f3", int32());

  auto schema = arrow::schema({field0, field1});
  auto schema_1 = arrow::schema({field0, field1, field2});

  // output fields
  auto field_sum = field("add", int32());
  auto field_sub = field("subtract", int32());

  // Build expression
  auto sum_expr = TreeExprBuilder::MakeExpression("add", {field0, field1}, field_sum);
  auto sub_expr =
      TreeExprBuilder::MakeExpression("subtract", {field0, field1}, field_sub);

  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {sum_expr, sub_expr}, pool_, &projector);
  gandiva::IdToModuleMap<std::shared_ptr<ProjectorHolder>> projector_modules_;
  std::vector<FieldPtr> ret_types;
  std::shared_ptr<ProjectorHolder> holder(
      new ProjectorHolder(schema, ret_types, projector, {sum_expr, sub_expr},
                          ConfigurationBuilder::DefaultConfiguration()));
  projector_modules_.Insert(holder);

  // try looking up project with same schema, config and expression list as above.
  // should match the one in the map.
  std::shared_ptr<ProjectorHolder> lookup(
      new ProjectorHolder(schema, ret_types, nullptr, {sum_expr, sub_expr},
                          ConfigurationBuilder::DefaultConfiguration()));
  holder = projector_modules_.Lookup(lookup);
  EXPECT_TRUE(holder != nullptr);

  {
    // same function created through different function
    auto node_0 = TreeExprBuilder::MakeField(field0);
    auto node_1 = TreeExprBuilder::MakeField(field1);
    auto func_1 = TreeExprBuilder::MakeFunction("add", {node_0, node_1}, int32());
    auto sum_expr_1 = TreeExprBuilder::MakeExpression(func_1, field_sum);
    // try looking with same expression list but different expressions
    lookup.reset(new ProjectorHolder(schema, ret_types, nullptr, {sum_expr_1, sub_expr},
                                     ConfigurationBuilder::DefaultConfiguration()));
    holder = projector_modules_.Lookup(lookup);
    EXPECT_TRUE(holder != nullptr);
  }

  // try looking with same expression list but different schema
  lookup.reset(new ProjectorHolder(schema_1, ret_types, nullptr, {sum_expr, sub_expr},
                                   ConfigurationBuilder::DefaultConfiguration()));
  holder = projector_modules_.Lookup(lookup);
  EXPECT_TRUE(holder == nullptr);

  // try looking with same expression list but different expressions
  lookup.reset(new ProjectorHolder(schema, ret_types, nullptr, {sum_expr, sum_expr},
                                   ConfigurationBuilder::DefaultConfiguration()));
  holder = projector_modules_.Lookup(lookup);
  EXPECT_TRUE(holder == nullptr);

  // same function signature but using a literal
  auto node_0 = TreeExprBuilder::MakeField(field0);
  auto literal_2 = TreeExprBuilder::MakeLiteral((int32_t)2);
  auto func_1 = TreeExprBuilder::MakeFunction("add", {node_0, literal_2}, int32());
  auto sum_expr_literal = TreeExprBuilder::MakeExpression(func_1, field_sum);
  // try looking with same expression list but different expressions
  lookup.reset(new ProjectorHolder(schema, ret_types, nullptr,
                                   {sum_expr_literal, sub_expr},
                                   ConfigurationBuilder::DefaultConfiguration()));
  holder = projector_modules_.Lookup(lookup);
  EXPECT_TRUE(holder == nullptr);
}

TEST_F(TestProjectorHolderMap, TestMapErase) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f2", int32());
  auto field2 = field("f3", int32());

  auto schema = arrow::schema({field0, field1});
  auto schema_1 = arrow::schema({field0, field1, field2});

  // output fields
  auto field_sum = field("add", int32());
  auto field_sub = field("subtract", int32());

  // Build expression
  auto sum_expr = TreeExprBuilder::MakeExpression("add", {field0, field1}, field_sum);
  auto sub_expr =
      TreeExprBuilder::MakeExpression("subtract", {field0, field1}, field_sub);

  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {sum_expr, sub_expr}, pool_, &projector);
  gandiva::IdToModuleMap<std::shared_ptr<ProjectorHolder>> projector_modules_;
  std::vector<FieldPtr> ret_types;
  std::shared_ptr<ProjectorHolder> holder(
      new ProjectorHolder(schema, ret_types, projector, {sum_expr, sub_expr},
                          ConfigurationBuilder::DefaultConfiguration()));
  long moduleId1 = projector_modules_.Insert(holder);

  // try looking up project with same schema, config and expression list as above.
  // should match the one in the map.
  std::shared_ptr<ProjectorHolder> lookup(
      new ProjectorHolder(schema, ret_types, nullptr, {sum_expr, sub_expr},
                          ConfigurationBuilder::DefaultConfiguration()));
  holder = projector_modules_.Lookup(lookup);
  EXPECT_TRUE(holder != nullptr);
  long moduleId2 = projector_modules_.Insert(holder);

  projector_modules_.Erase(moduleId1);
  // try looking up project again after erase
  lookup.reset(new ProjectorHolder(schema, ret_types, nullptr, {sum_expr, sub_expr},
                                   ConfigurationBuilder::DefaultConfiguration()));
  holder = projector_modules_.Lookup(lookup);
  EXPECT_TRUE(holder != nullptr);

  projector_modules_.Erase(moduleId2);
  EXPECT_TRUE(projector_modules_.map_.size() == 0);
}
}  // namespace gandiva
