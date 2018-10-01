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
      Boost::boost
      LLVM::LLVM_INTERFACE
      RE2::RE2_STATIC)

  if (${TYPE} MATCHES "static" AND NOT APPLE)
    target_link_libraries(gandiva_${TYPE}
      LINK_PRIVATE
        -static-libstdc++ -static-libgcc)
  endif()

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
  if(${REL_TEST_NAME} MATCHES "llvm" OR
     ${REL_TEST_NAME} MATCHES "expression_registry")
    # If the unit test has llvm in its name, include llvm.
    add_dependencies(${TEST_NAME} LLVM::LLVM_INTERFACE)
    target_link_libraries(${TEST_NAME} PRIVATE LLVM::LLVM_INTERFACE)
  endif()

  target_include_directories(${TEST_NAME} PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src
  )
  target_link_libraries(${TEST_NAME}
    PRIVATE ARROW::ARROW_SHARED gtest_main RE2::RE2_STATIC Boost::boost
  )
  add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
  set_property(TEST ${TEST_NAME} PROPERTY LABELS unittest ${TEST_NAME})
endfunction(add_gandiva_unit_test REL_TEST_NAME)

# Add a unittest executable for a precompiled file (used to generate IR)
function(add_precompiled_unit_test REL_TEST_NAME)
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  add_executable(${TEST_NAME} ${REL_TEST_NAME} ${ARGN})
  target_include_directories(${TEST_NAME} PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(${TEST_NAME} PRIVATE RE2::RE2_STATIC gtest_main)
  target_compile_definitions(${TEST_NAME} PRIVATE GANDIVA_UNIT_TEST=1)
  target_compile_definitions(${TEST_NAME} PRIVATE -DGDV_HELPERS)
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
      ${CLANG_FORMAT_EXECUTABLE}
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
      echo ${LINT_FILES} | xargs ${CLANG_FORMAT_EXECUTABLE} -style=file -i
    )
  endif (UNIX)
endfunction(add_stylefix)

function(prevent_in_source_builds)
 file(TO_CMAKE_PATH "${PROJECT_BINARY_DIR}/CMakeLists.txt" LOC_PATH)
 if(EXISTS "${LOC_PATH}")
   message(FATAL_ERROR "Gandiva does not support in-source builds")
 endif()
endfunction(prevent_in_source_builds)
