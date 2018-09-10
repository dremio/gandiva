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

set -ex

source $TRAVIS_BUILD_DIR/ci/travis_env_common.sh

using_homebrew=no

while true; do
    case "$1" in
        --homebrew)
            using_homebrew=yes
            shift ;;
        *) break ;;
    esac
done

if [ "$only_library_mode" == "no" ]; then
  source $TRAVIS_BUILD_DIR/ci/travis_install_conda.sh
fi

CMAKE_COMMON_FLAGS="\
-DCMAKE_INSTALL_PREFIX=$GANDIVA_CPP_INSTALL"
CMAKE_LINUX_FLAGS=""
CMAKE_OSX_FLAGS=""

if [ "$GANDIVA_TRAVIS_USE_TOOLCHAIN" == "1" ]; then
  # Set up C++ toolchain from conda-forge packages for faster builds
  source $TRAVIS_BUILD_DIR/ci/travis_install_toolchain.sh
  CMAKE_COMMON_FLAGS="${CMAKE_COMMON_FLAGS}"
fi

source $TRAVIS_BUILD_DIR/ci/travis_install_re2.sh
source $TRAVIS_BUILD_DIR/ci/travis_install_arrow.sh

mkdir -p $GANDIVA_CPP_BUILD_DIR
pushd $GANDIVA_CPP_BUILD_DIR

# Use Ninja for faster builds when using toolchain
if [ $GANDIVA_TRAVIS_USE_TOOLCHAIN == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -GNinja"
fi

if [ $TRAVIS_OS_NAME == "linux" ]; then
    cmake $CMAKE_COMMON_FLAGS \
          $CMAKE_LINUX_FLAGS \
          -DCMAKE_BUILD_TYPE=$GANDIVA_BUILD_TYPE \
          -DBUILD_WARNING_LEVEL=$GANDIVA_BUILD_WARNING_LEVEL \
          $GANDIVA_CPP_DIR
else
    if [ "$using_homebrew" = "yes" ]; then
        # build against homebrew's boost if we're using it
        export BOOST_ROOT=/usr/local/opt/boost
    fi
    cmake $CMAKE_COMMON_FLAGS \
          $CMAKE_OSX_FLAGS \
          -DCMAKE_BUILD_TYPE=$GANDIVA_BUILD_TYPE \
          -DBUILD_WARNING_LEVEL=$GANDIVA_BUILD_WARNING_LEVEL \
          $GANDIVA_CPP_DIR
fi

# Do the style checks
$TRAVIS_MAKE stylefix
if ! git diff-index --quiet HEAD --; then
  # The syle fix required some changes
  echo "Stylecheck failed, requires the following fix"
  git diff
  exit 1
fi

# Build and install libraries
$TRAVIS_MAKE -j4

$TRAVIS_MAKE install

ldd ./src/jni/libgandiva_jni.so

popd
