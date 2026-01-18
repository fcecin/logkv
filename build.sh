#!/bin/bash
set -e
cmake -S . -B build/release -DCMAKE_BUILD_TYPE=Release
cmake --build build/release -j8
cmake -S . -B build/debug -DCMAKE_BUILD_TYPE=Debug
cmake --build build/debug -j8
