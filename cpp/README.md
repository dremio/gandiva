# Gandiva C++

## System setup

Gandiva uses CMake as a build configuration system. Currently, it supports 
in-source and out-of-source builds with the latter one being preferred.

Build Gandiva requires:

* A C++11-enabled compiler. On Linux, gcc 4.8 and higher should be sufficient.
* CMake
* LLVM 
* protobuf

On OS X, you can use [Homebrew][1]:

```shell
brew install cmake llvm protobuf
```

## Building Gandiva

    mkdir build
    cd build 
    cmake ..
    make
    
[1]: https://brew.sh/
