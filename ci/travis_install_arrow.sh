#!/usr/bin/env bash

set -ex

ARROW_VERSION=0.9.0

# Use Ninja for faster builds when using toolchain
if [ $GANDIVA_TRAVIS_USE_TOOLCHAIN == "1" ]; then
  CMAKE_ARROW_FLAGS="$CMAKE_COMMON_FLAGS -GNinja"
fi

wget https://github.com/apache/arrow/archive/apache-arrow-${ARROW_VERSION}.zip
unzip apache-arrow-${ARROW_VERSION}.zip

ARROW_SRC_DIR=arrow-apache-arrow-${ARROW_VERSION}
mkdir $ARROW_SRC_DIR/cpp/build

pushd $ARROW_SRC_DIR/cpp/build

cmake $CMAKE_ARROW_FLAGS ..

# Build and install libraries
$TRAVIS_MAKE -j4

$TRAVIS_MAKE install

popd
