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

set -e

source $TRAVIS_BUILD_DIR/ci/travis_env_common.sh

pushd $CPP_BUILD_DIR

# Temporary work around to copy the library to current java directory.
# Follow work to make this as argument in maven and loading the library
# dynamically.
cp $CPP_BUILD_DIR/src/jni/lingandiva_jni.so $GANDIVA_JAVA_DIR/lingandiva_jni.so

mvn test -f $GANDIVA_JAVA_DIR/pom.xml

popd