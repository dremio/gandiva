# Copyright (C) 2017-2018 Dremio Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

project(gandiva)

#find LLVM
find_package(LLVM)

# Find arrow
find_package(ARROW)

# build googletest
BUILD_EXTERNAL(googletest)

find_package(Boost REQUIRED)

add_library(gandiva SHARED
    codegen/engine.cc
    codegen/function_registry.cc
    codegen/llvm_generator.cc
    codegen/llvm_types.cc
    expr/annotator.cc
    expr/evaluator.cc
    expr/tree_expr_builder.cc
    expr/node.cc)
#TODO : seperate out the public and private header files.
target_include_directories(gandiva PUBLIC . codegen expr)
target_link_libraries(gandiva
  PUBLIC ARROW::ARROW
  PRIVATE Boost::boost
)
TARGET_LINK_LLVM(gandiva PRIVATE)

add_subdirectory(precompiled)

#args: label test-file src-files
ADD_GANDIVA_UNIT_TEST(codegen/dex_llvm_test.cc)
ADD_GANDIVA_UNIT_TEST(codegen/engine_llvm_test.cc codegen/engine.cc codegen/llvm_types.cc)
ADD_GANDIVA_UNIT_TEST(codegen/function_signature_test.cc)
ADD_GANDIVA_UNIT_TEST(codegen/function_registry_test.cc codegen/function_registry.cc)
ADD_GANDIVA_UNIT_TEST(codegen/llvm_types_test.cc codegen/llvm_types.cc)
ADD_GANDIVA_UNIT_TEST(codegen/llvm_generator_test.cc codegen/llvm_generator.cc codegen/engine.cc codegen/llvm_types.cc codegen/function_registry.cc expr/annotator.cc)
ADD_GANDIVA_UNIT_TEST(expr/annotator_test.cc expr/annotator.cc)
ADD_GANDIVA_UNIT_TEST(expr/tree_expr_test.cc expr/tree_expr_builder.cc expr/node.cc expr/annotator.cc codegen/function_registry.cc)

ADD_GANDIVA_INTEG_TEST(expr/evaluator_test.cc)
