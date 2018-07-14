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

source $TRAVIS_BUILD_DIR/ci/travis_env_common.sh

source $TRAVIS_BUILD_DIR/ci/travis_install_conda.sh

# Pinning Gandiva to Arrow 0.8 version. We are seeing seg-faults with
# 0.9 version. Will be tracked in https://dremio.atlassian.net/browse/GDV-69.
if [ ! -e $CPP_TOOLCHAIN ]; then
    # Set up C++ toolchain from conda-forge packages for faster builds
    conda create -y -q -p $CPP_TOOLCHAIN python=2.7 \
        flatbuffers \
        libprotobuf \
        gflags \
        gtest \
        ccache \
        cmake \
        curl \
        ninja \
        arrow-cpp=0.9.0 \
        boost-cpp

    conda update -y -q -p $CPP_TOOLCHAIN ca-certificates -c defaults
fi
