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

# Find Arrow library and includes.

find_library(ARROW_LIB_STATIC libarrow.a REQUIRED)
find_library(ARROW_LIB_SHARED arrow REQUIRED)
message(STATUS "Found arrow static library at ${ARROW_LIB_STATIC}")
message(STATUS "Found arrow shared library at ${ARROW_LIB_SHARED}")

find_path(ARROW_INCLUDE_DIR arrow/type.h)
include_directories("${ARROW_INCLUDE_DIR}")
message(STATUS "found arrow includes at ${ARROW_INCLUDE_DIR}")

# add an imported target ARROW::ARROW so that gandiva can take a dependency.
add_library(ARROW::ARROW_STATIC STATIC IMPORTED)
add_library(ARROW::ARROW_SHARED INTERFACE IMPORTED)

set_target_properties(ARROW::ARROW_STATIC PROPERTIES
  INTERFACE_INCLUDE_DIRECTORIES "${ARROW_INCLUDE_DIR}"
  IMPORTED_LOCATION "${ARROW_LIB_STATIC}"
)
set_target_properties(ARROW::ARROW_SHARED PROPERTIES
  INTERFACE_INCLUDE_DIRECTORIES "${ARROW_INCLUDE_DIR}"
  INTERFACE_LINK_LIBRARIES "${ARROW_LIB_SHARED}"
)
