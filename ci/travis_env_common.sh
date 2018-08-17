#!/usr/bin/env bash

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

# Adapted from Apache Arrow

export MINICONDA=$HOME/miniconda
export PATH="$MINICONDA/bin:$PATH"
export CONDA_PKGS_DIRS=$HOME/.conda_packages

export GANDIVA_CPP_DIR=$TRAVIS_BUILD_DIR/cpp
export GANDIVA_JAVA_DIR=${TRAVIS_BUILD_DIR}/java
export GANDIVA_INTEGRATION_DIR=$TRAVIS_BUILD_DIR/integration

export CPP_BUILD_DIR=$TRAVIS_BUILD_DIR/cpp-build

export GANDIVA_CPP_INSTALL=$TRAVIS_BUILD_DIR/cpp-install
export GANDIVA_CPP_BUILD_DIR=$TRAVIS_BUILD_DIR/cpp-build

export CMAKE_EXPORT_COMPILE_COMMANDS=1

export GANDIVA_BUILD_TYPE=${GANDIVA_BUILD_TYPE:=Release}
export GANDIVA_BUILD_WARNING_LEVEL=${GANDIVA_BUILD_WARNING_LEVEL:=Production}

if [ "$GANDIVA_TRAVIS_USE_TOOLCHAIN" == "1" ]; then
  # C++ toolchain
  export CPP_TOOLCHAIN=$TRAVIS_BUILD_DIR/cpp-toolchain
  export GANDIVA_BUILD_TOOLCHAIN=$CPP_TOOLCHAIN
  export BOOST_ROOT=$CPP_TOOLCHAIN

  export PATH=$CPP_TOOLCHAIN/bin:$PATH
  export LD_LIBRARY_PATH=$CPP_TOOLCHAIN/lib:$LD_LIBRARY_PATH
  export TRAVIS_MAKE=ninja
else
  export TRAVIS_MAKE=make
fi
