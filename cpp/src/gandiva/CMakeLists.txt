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

cmake_minimum_required(VERSION 3.10)
project(gandiva)

find_package(LLVM REQUIRED CONFIG)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")
include_directories(${LLVM_INCLUDE_DIRS})
add_definitions(${LLVM_DEFINITIONS})

include_directories(codegen)
include_directories(codegen/cex)
include_directories(codegen/dex)
include_directories(precompiled)
include_directories(${CMAKE_BINARY_DIR})

set(GANDIVA_SRCS
    codegen/cex/func_cex.cc
    codegen/engine.cc
    codegen/expression_annotator.cc
    codegen/function_registry.cc
    codegen/llvm_generator.cc
    codegen/llvm_types.cc)

set(PRECOMPILED_SOURCED_FILES
    precompiled/arithmetic_ops.cc
    precompiled/bitmap.cc
    precompiled/nativehelpers.cc
    precompiled/time.cc
    precompiled/types.h)

SET(PRECOMPILED_SRC precompiled/irhelpers.cc)
#TODO : fix this
SET(PRECOMPILED_OUTPUT /tmp/irhelpers.bc)

find_program(CLANG_EXECUTABLE clang++)
message(STATUS "Found clang++ ${CLANG_EXECUTABLE}")

get_filename_component(PRECOMPILED_ABSOLUTE_SRC ${PRECOMPILED_SRC} ABSOLUTE)
add_custom_command(
  OUTPUT ${PRECOMPILED_OUTPUT}
  COMMAND ${CLANG_EXECUTABLE} -flto -O2 -c ${PRECOMPILED_ABSOLUTE_SRC} -o ${PRECOMPILED_OUTPUT}
  DEPENDS ${PRECOMPILED_SRC} ${PRECOMPILED_SOURCED_FILES})
add_custom_target(precompile-to-ir ALL DEPENDS ${PRECOMPILED_OUTPUT})

add_library(gandiva SHARED ${GANDIVA_SRCS})
target_link_libraries(gandiva proto ${PROTOBUF_LIBRARY})

# Find the libraries that correspond to the LLVM components
llvm_map_components_to_libnames(llvm_libs core mcjit native ipo bitreader target linker analysis debuginfodwarf)
# Link against LLVM libraries
target_link_libraries(gandiva ${llvm_libs})

add_executable(replay codegen/replay.cc)
target_link_libraries(replay gandiva)
