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

openssl aes-256-cbc -K $encrypted_f7773fd99b03_key -iv $encrypted_f7773fd99b03_iv -in $TRAVIS_BUILD_DIR/ci/codesigning.asc.enc -out $TRAVIS_BUILD_DIR/ci/codesigning.asc -d

gpg --fast-import $TRAVIS_BUILD_DIR/ci/codesigning.asc

mvn versions:set -B -DnewVersion=0.1-$TRAVIS_COMMIT -f $GANDIVA_JAVA_DIR/pom.xml

mvn deploy -P release -f $GANDIVA_JAVA_DIR/pom.xml --settings $TRAVIS_BUILD_DIR/ci/ossrh_settings.xml -Dgandiva.cpp.build.dir=$CPP_BUILD_DIR