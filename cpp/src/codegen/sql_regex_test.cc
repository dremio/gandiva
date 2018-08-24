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

#include "codegen/sql_regex.h"

#include <memory>
#include <vector>

#include <gtest/gtest.h>

namespace gandiva {

class TestSqlRegex : public ::testing::Test {};

TEST_F(TestSqlRegex, TestMatchAny) {
  std::shared_ptr<SqlRegex> regex;

  auto status = SqlRegex::Make("ab%", &regex);
  EXPECT_EQ(status.ok(), true) << status.message();

  EXPECT_TRUE(regex->Like("ab"));
  EXPECT_TRUE(regex->Like("abc"));
  EXPECT_TRUE(regex->Like("abcd"));

  EXPECT_FALSE(regex->Like("a"));
  EXPECT_FALSE(regex->Like("cab"));
}

TEST_F(TestSqlRegex, TestMatchOne) {
  std::shared_ptr<SqlRegex> regex;

  auto status = SqlRegex::Make("ab_", &regex);
  EXPECT_EQ(status.ok(), true) << status.message();

  EXPECT_TRUE(regex->Like("abc"));
  EXPECT_TRUE(regex->Like("abd"));

  EXPECT_FALSE(regex->Like("a"));
  EXPECT_FALSE(regex->Like("abcd"));
  EXPECT_FALSE(regex->Like("dabc"));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace gandiva
