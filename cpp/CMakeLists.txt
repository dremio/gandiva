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
set(COMMON_FLAGS "-Wall -Werror ")
SET(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${COMMON_FLAGS} -O2 -g")
SET(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS} -O2")
SET(CMAKE_CXX_FLAGS_DEBUG "${COMMON_FLAGS} -O0 -g")
set(BUILD_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/build-support")

include(CTest)
enable_testing()
add_subdirectory(src/gandiva)

if (UNIX)
  file(GLOB_RECURSE LINT_FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.h"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc"
    )

  find_program(CPPLINT_BIN NAMES cpplint cpplint.py HINTS ${BUILD_SUPPORT_DIR})
  message(STATUS "Found cpplint executable at ${CPPLINT_BIN}")

  FOREACH(item ${LINT_FILES})
    IF(NOT ((item MATCHES "irhelpers.cc") OR
            (item MATCHES "nativehelpers.cc")))
      LIST(APPEND FILTERED_LINT_FILES ${item})
    ENDIF()
  ENDFOREACH(item ${LINT_FILES})

  # Full lint
  # Balancing act: cpplint.py takes a non-trivial time to launch,
  # so process 12 files per invocation, while still ensuring parallelism
  add_custom_target(lint echo ${FILTERED_LINT_FILES} | xargs -n12 -P8
  ${CPPLINT_BIN}
  --verbose=2
  --linelength=90
  --filter=-whitespace/comments,-readability/todo,-build/header_guard,-build/c++11,-runtime/references,-build/include_order
  )
endif (UNIX)
