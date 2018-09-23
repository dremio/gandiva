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
#
# Usage of this module as follows:
#
#  find_package(LLVM)
#

set(GANDIVA_LLVM_VERSION 6.0)
find_package(LLVM ${GANDIVA_LLVM_VERSION} REQUIRED CONFIG HINTS
             /usr/local/opt/llvm
             /usr/share)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")

# Find the libraries that correspond to the LLVM components
llvm_map_components_to_libnames(LLVM_LIBS core mcjit native ipo bitreader target linker analysis debuginfodwarf)

set(CLANG_EXECUTABLE ${LLVM_TOOLS_BINARY_DIR}/clang CACHE STRING "clang++")
set(LINK_EXECUTABLE ${LLVM_TOOLS_BINARY_DIR}/llvm-link CACHE STRING "link")
set(CLANG_FORMAT_EXECUTABLE ${LLVM_TOOLS_BINARY_DIR}/clang-format CACHE STRING "clang-format")

add_library(LLVM::LLVM_INTERFACE INTERFACE IMPORTED)

set_target_properties(LLVM::LLVM_INTERFACE PROPERTIES
  INTERFACE_INCLUDE_DIRECTORIES "${LLVM_INCLUDE_DIRS}"
  INTERFACE_COMPILE_FLAGS "${LLVM_DEFINITIONS}"
  INTERFACE_LINK_LIBRARIES "${LLVM_LIBS}"
)

