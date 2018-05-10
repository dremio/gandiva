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

cmake_minimum_required(VERSION 3.2)
project(gandiva)

# This ensures that things like gnu++11 get passed correctly
set(CMAKE_CXX_STANDARD 11)

# We require a C++11 compliant compiler
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# CMake generators if no build type is set.
IF(NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
   SET(CMAKE_BUILD_TYPE debug)
ENDIF(NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)

# Select flags.
SET(CMAKE_CXX_FLAGS "-Wall")
SET(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g")
SET(CMAKE_CXX_FLAGS_RELEASE "-O2")
SET(CMAKE_CXX_FLAGS_DEBUG  "-O0 -g")

# Build protobufs
include(FindProtobuf)
set(PROTO_FILES
    ../protobuf/Types.proto
    ../protobuf/CodeGen.proto)
find_package(Protobuf REQUIRED)
include_directories(${PROTOBUF_INCLUDE_DIR})
protobuf_generate_cpp(PROTO_SRC PROTO_HEADER ${PROTO_FILES})
add_library(proto ${PROTO_HEADER} ${PROTO_SRC})

add_subdirectory(src/gandiva)
