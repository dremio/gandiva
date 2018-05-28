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

# Include modules
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake")
include(BuildUtils)

prevent_in_source_builds()

# Default build type if none is set.
if(NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE debug)
endif()
message(STATUS "using build type ${CMAKE_BUILD_TYPE}")

# This ensures that things like gnu++11 get passed correctly
set(CMAKE_CXX_STANDARD 11)

# We require a C++11 compliant compiler
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Select compiler flags.
add_compile_options(-Wall -Werror)

# set the build-support dir.
set(BUILD_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/build-support")

# build googletest
build_external(googletest)

include(CTest)
enable_testing()

add_subdirectory(src)

add_lint()  # "make lint" target
