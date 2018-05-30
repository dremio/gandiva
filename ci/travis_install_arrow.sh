#!/usr/bin/env bash

set -ex

ARROW_VERSION=0.9.0

wget https://github.com/apache/arrow/archive/apache-arrow-${ARROW_VERSION}.zip
unzip apache-arrow-${ARROW_VERSION}.zip

ARROW_SRC_DIR=arrow-apache-arrow-${ARROW_VERSION}
mkdir $ARROW_SRC_DIR/cpp/build

pushd $ARROW_SRC_DIR/cpp/build

cmake $ARROW_SRC_DIR/cpp

# Build and install libraries
$TRAVIS_MAKE -j4

$TRAVIS_MAKE install

popd
