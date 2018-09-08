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

set -ex

TAG=master
RE2_SRC_DIR=re2-${TAG}

# Use Ninja for faster builds when using toolchain
if [ $GANDIVA_TRAVIS_USE_TOOLCHAIN == "1" ]; then
  CMAKE_RE2_FLAGS="$CMAKE_COMMON_FLAGS -GNinja"
fi

wget https://github.com/google/re2/archive/${TAG}.zip
unzip -qq ${TAG}.zip

mkdir $RE2_SRC_DIR/build

pushd $RE2_SRC_DIR/build

cmake $CMAKE_RE2_FLAGS ..

# Build and install libraries
$TRAVIS_MAKE -j4

$TRAVIS_MAKE install

popd

