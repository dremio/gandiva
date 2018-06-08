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

set -e

if (! which conda > /dev/null ); then
  if [ $TRAVIS_OS_NAME == "linux" ]; then
    MINICONDA_URL="https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh"
  else
    MINICONDA_URL="https://repo.continuum.io/miniconda/Miniconda3-latest-MacOSX-x86_64.sh"
  fi

  source $TRAVIS_BUILD_DIR/ci/travis_env_common.sh
  mkdir -p $CONDA_PKGS_DIRS

  wget --no-verbose -O miniconda.sh $MINICONDA_URL
  bash miniconda.sh -b -p $MINICONDA
  export PATH="$MINICONDA/bin:$PATH"

  conda update -y -q conda
  conda config --set auto_update_conda false
  conda info -a

  conda config --set show_channel_urls True

  # Help with SSL timeouts to S3
  conda config --set remote_connect_timeout_secs 12

  conda config --add channels https://repo.continuum.io/pkgs/free
  conda config --add channels conda-forge
fi

conda info -a
