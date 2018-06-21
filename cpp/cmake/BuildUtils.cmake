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

# Build the gandiva library
function(build_gandiva_lib TYPE)
  string(TOUPPER ${TYPE} TYPE_UPPER_CASE)
  add_library(gandiva_${TYPE} ${TYPE_UPPER_CASE} $<TARGET_OBJECTS:gandiva_obj_lib>)

  target_include_directories(gandiva_${TYPE}
    PUBLIC
      $<INSTALL_INTERFACE:include>
      $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    PRIVATE
      ${CMAKE_SOURCE_DIR}/src
  )

  # ARROW is a public dependency i.e users of gandiva also will have a dependency on arrow.
  target_link_libraries(gandiva_${TYPE}
    PUBLIC
      ARROW::ARROW_${TYPE_UPPER_CASE}
    PRIVATE
      Boost::boost)

  # LLVM is a private dependency i.e users of gandiva will not need to include llvm headers
  # or link with llvm libraries.
  target_link_llvm(gandiva_${TYPE} PRIVATE)

  # Set version for the library.
  set(GANDIVA_VERSION_MAJOR 0)
  set(GANDIVA_VERSION_MINOR 1)
  set(GANDIVA_VERSION_PATCH 0)
  set(GANDIVA_VERSION ${GANDIVA_VERSION_MAJOR}.${GANDIVA_VERSION_MINOR}.${GANDIVA_VERSION_PATCH})

  set_target_properties(gandiva_${TYPE} PROPERTIES
    VERSION ${GANDIVA_VERSION}
    SOVERSION ${GANDIVA_VERSION_MAJOR}
    OUTPUT_NAME gandiva
  )
endfunction(build_gandiva_lib TYPE)

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
    PRIVATE ${ARROW_LIB_SHARED} gtest_main Boost::boost
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
function(add_gandiva_integ_test REL_TEST_NAME GANDIVA_LIB)
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  add_executable(${TEST_NAME}_${GANDIVA_LIB} ${REL_TEST_NAME} ${ARGN})
  target_include_directories(${TEST_NAME}_${GANDIVA_LIB} PRIVATE ${CMAKE_SOURCE_DIR})
  target_link_libraries(${TEST_NAME}_${GANDIVA_LIB} PRIVATE ${GANDIVA_LIB} gtest_main)

  add_test(NAME ${TEST_NAME}_${GANDIVA_LIB} COMMAND ${TEST_NAME}_${GANDIVA_LIB})
  set_property(TEST ${TEST_NAME}_${GANDIVA_LIB} PROPERTY LABELS integ ${TEST_NAME}_${GANDIVA_LIB})
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

# Add "make lint" target
function(add_lint)
  if (UNIX)
    file(GLOB_RECURSE LINT_FILES
      "${CMAKE_CURRENT_SOURCE_DIR}/src/*.h"
      "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc"
      "${CMAKE_CURRENT_SOURCE_DIR}/integ/*.h"
      "${CMAKE_CURRENT_SOURCE_DIR}/integ/*.cc"
      )

    find_program(CPPLINT_BIN NAMES cpplint cpplint.py HINTS ${BUILD_SUPPORT_DIR})
    message(STATUS "Found cpplint executable at ${CPPLINT_BIN}")

    # Full lint
    # Balancing act: cpplint.py takes a non-trivial time to launch,
    # so process 12 files per invocation, while still ensuring parallelism
    add_custom_target(lint echo ${LINT_FILES} | xargs -n12 -P8
    ${CPPLINT_BIN}
    --verbose=2
    --linelength=90
    --filter=-whitespace/comments,-readability/todo,-build/header_guard,-build/c++11,-runtime/references
    )
  endif (UNIX)
endfunction(add_lint)

function(prevent_in_source_builds)
 file(TO_CMAKE_PATH "${PROJECT_BINARY_DIR}/CMakeLists.txt" LOC_PATH)
 if(EXISTS "${LOC_PATH}")
   message(FATAL_ERROR "Gandiva does not support in-source builds")
 endif()
endfunction(prevent_in_source_builds)
