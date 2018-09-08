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

# Find re2 library and includes.

find_library(RE2_LIB_STATIC libre2.a REQUIRED)
message(STATUS "Found re2 static library at ${RE2_LIB_STATIC}")

find_path(RE2_INCLUDE_DIR re2/re2.h)
message(STATUS "found re2 includes at ${RE2_INCLUDE_DIR}")

# add an imported target RE2::RE2 so that gandiva can take a dependency.
add_library(RE2::RE2_STATIC STATIC IMPORTED)

set_target_properties(RE2::RE2_STATIC PROPERTIES
  INTERFACE_INCLUDE_DIRECTORIES "${RE2_INCLUDE_DIR}"
  IMPORTED_LOCATION "${RE2_LIB_STATIC}"
)
