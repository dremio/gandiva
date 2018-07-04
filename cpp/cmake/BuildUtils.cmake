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

# Add a unittest executable, with its dependencies.
function(add_gandiva_unit_test REL_TEST_NAME)
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  add_executable(${TEST_NAME} ${REL_TEST_NAME} ${ARGN})
  if(${REL_TEST_NAME} MATCHES "llvm")
    # If the unit test has llvm in its name, include llvm.
    target_link_llvm(${TEST_NAME} PRIVATE)
  endif()

  target_include_directories(${TEST_NAME} PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src
  )
  target_link_libraries(${TEST_NAME}
    PRIVATE ${ARROW_LIB} gtest_main Boost::boost
  )
  add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
  set_property(TEST ${TEST_NAME} PROPERTY LABELS unittest ${TEST_NAME})
endfunction(add_gandiva_unit_test REL_TEST_NAME)

# Add a unittest executable for a precompiled file (used to generate IR)
function(add_precompiled_unit_test REL_TEST_NAME)
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  add_executable(${TEST_NAME} ${REL_TEST_NAME} ${ARGN})
  target_include_directories(${TEST_NAME} PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(${TEST_NAME} PRIVATE gtest_main)
  target_compile_definitions(${TEST_NAME} PRIVATE GANDIVA_UNIT_TEST=1)
  add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
  set_property(TEST ${TEST_NAME} PROPERTY LABELS unittest ${TEST_NAME})
endfunction(add_precompiled_unit_test REL_TEST_NAME)

# Add an integ executable, with its dependencies.
function(add_gandiva_integ_test REL_TEST_NAME)
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  add_executable(${TEST_NAME} ${REL_TEST_NAME} ${ARGN})
  target_include_directories(${TEST_NAME} PRIVATE ${CMAKE_SOURCE_DIR})
  target_link_libraries(${TEST_NAME} PRIVATE gandiva gtest_main)

  add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
  set_property(TEST ${TEST_NAME} PROPERTY LABELS integ ${TEST_NAME})
endfunction(add_gandiva_integ_test REL_TEST_NAME)

# Download and build external project.
function(build_external PROJ)
  message("Building ${PROJ} as external project")
  # configure the download
  configure_file(${CMAKE_SOURCE_DIR}/cmake/${PROJ}-CMakeLists.txt.in ${CMAKE_BINARY_DIR}/${PROJ}-download/CMakeLists.txt)
  execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
    RESULT_VARIABLE result
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/${PROJ}-download )
  if(result)
    message(FATAL_ERROR "CMake step for ${PROJ} failed: ${result}")
  endif(result)

  # unpack
  execute_process(COMMAND ${CMAKE_COMMAND} --build .
    RESULT_VARIABLE result
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/${PROJ}-download )
  if(result)
    message(FATAL_ERROR "Build step for ${PROJ} failed: ${result}")
  endif(result)

  # Add project directly to the build.
  add_subdirectory(${CMAKE_BINARY_DIR}/${PROJ}-src
                   ${CMAKE_BINARY_DIR}/${PROJ}-build
                   EXCLUDE_FROM_ALL)
endfunction(build_external PROJ)

find_program(CLANG_FORMAT_BIN NAMES clang-format)
message(STATUS "Found clang-format executable at ${CLANG_FORMAT_BIN}")

file(GLOB_RECURSE LINT_FILES
  "${CMAKE_CURRENT_SOURCE_DIR}/include/*.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/*.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/integ/*.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/integ/*.cc"
)

# Add "make stylecheck" target
function(add_stylecheck)
  if (UNIX)
    add_custom_target(stylecheck
      COMMENT "Performing stylecheck on all .cpp/.h files"
      # use ! to check for no replacement
      COMMAND !
      ${CLANG_FORMAT_BIN}
      -style=file
      -output-replacements-xml
      ${LINT_FILES}
      | grep "replacement offset"
    )
  endif (UNIX)
endfunction(add_stylecheck)

# Add "make stylefix" target
function(add_stylefix)
  if (UNIX)
    add_custom_target(stylefix
      COMMENT "Performing stylefix on all .cpp/.h files"
      COMMAND
      echo ${LINT_FILES} | xargs ${CLANG_FORMAT_BIN} -style=file -i
    )
  endif (UNIX)
endfunction(add_stylefix)

function(prevent_in_source_builds)
 file(TO_CMAKE_PATH "${PROJECT_BINARY_DIR}/CMakeLists.txt" LOC_PATH)
 if(EXISTS "${LOC_PATH}")
   message(FATAL_ERROR "Gandiva does not support in-source builds")
 endif()
endfunction(prevent_in_source_builds)
