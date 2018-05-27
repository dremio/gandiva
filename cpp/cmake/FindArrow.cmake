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

find_library(ARROW_LIB arrow REQUIRED)
message(STATUS "Found arrow library at ${ARROW_LIB}")

find_path(ARROW_INCLUDE_DIR arrow/type.h)
message(STATUS "found arrow includes at ${ARROW_INCLUDE_DIR}")

add_library(ARROW::ARROW INTERFACE IMPORTED)
set_target_properties(ARROW::ARROW PROPERTIES
  INTERFACE_INCLUDE_DIRECTORIES "${ARROW_INCLUDE_DIR}"
  INTERFACE_LINK_LIBRARIES "${ARROW_LIB}"
)
