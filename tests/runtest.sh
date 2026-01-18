#!/bin/bash

BLUE='\033[0;34m'
RESET='\033[0m'

BUILD_TYPE="debug"
USE_ASAN=true
COMPILER_FLAGS="-g -O0"
TEST_NAME=""

for arg in "$@"; do
    case $arg in
        --release)
            BUILD_TYPE="release"
            USE_ASAN=false
            COMPILER_FLAGS="-O3"
            ;;
        --asan)
            USE_ASAN=true
            ;;
        --noasan)
            USE_ASAN=false
            ;;
        -*)
            echo "Unknown option: $arg"
            exit 1
            ;;
        *)
            TEST_NAME=$arg
            ;;
    esac
done

if [ -z "$TEST_NAME" ]; then
    echo "Usage: ./runtest.sh <test_name> [--release] [--asan] [--noasan]"
    exit 1
fi

if [ ! -f "${TEST_NAME}.cpp" ]; then
    echo "Error: File '${TEST_NAME}.cpp' not found."
    exit 1
fi

MODE_STR="${BUILD_TYPE^^}"

if [ "$USE_ASAN" = true ]; then
    COMPILER_FLAGS="$COMPILER_FLAGS -fsanitize=address,undefined -fno-omit-frame-pointer"

    if [ "$BUILD_TYPE" == "release" ]; then
        COMPILER_FLAGS="$COMPILER_FLAGS -g"
    fi

    MODE_STR="$MODE_STR + ASAN + UBSAN"
fi

echo -e "${BLUE}Mode: $MODE_STR${RESET}"

BUILD_DIR="../build/${BUILD_TYPE}"
CRC_SRC_DIR="${BUILD_DIR}/_deps/crc32c-src"

CRC_LIB=$(find "${BUILD_DIR}" -name "libcrc32c.a" 2>/dev/null | head -n 1)

if [ -z "$CRC_LIB" ]; then
    echo -e "${BLUE}Dependencies (libcrc32c) not found. Triggering auto-build...${RESET}"

    (cd .. && ./build.sh)

    CRC_LIB=$(find "${BUILD_DIR}" -name "libcrc32c.a" 2>/dev/null | head -n 1)

    if [ -z "$CRC_LIB" ]; then
        echo "Error: Auto-build failed or libcrc32c.a is still missing."
        exit 1
    fi
fi

echo -e "${BLUE}Compiling ${TEST_NAME} ...${RESET}"
g++ -std=c++20 \
    $COMPILER_FLAGS \
    -I ../include \
    -I "${CRC_SRC_DIR}/include" \
    -o "${TEST_NAME}" \
    "${TEST_NAME}.cpp" \
    "${CRC_LIB}" \
    -pthread

if [ $? -eq 0 ]; then
    echo -e "${BLUE}Running ${TEST_NAME}${RESET}"
    echo "---------------------------------------------------"
    ./"${TEST_NAME}"
    RET_CODE=$?
    echo "---------------------------------------------------"

    rm -f "${TEST_NAME}"

    if [ $RET_CODE -eq 0 ]; then
        echo "TEST PASSED"
    else
        echo "TEST FAILED"
    fi
    exit $RET_CODE
else
    echo "Compilation Failed."
    exit 1
fi