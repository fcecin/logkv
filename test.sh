#!/bin/bash

# --- Configuration ---
TEST_DIR="./tests"
CLEAN_SCRIPT="clean.sh"

# --- ANSI Color Codes ---
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# --- Variables ---
PASSED_COUNT=0
FAILED_COUNT=0
TOTAL_TESTS=0
FAILURE_OUTPUT=""
EXIT_CODE=0

# --- Function to run a single test ---
run_test() {
    # NOTE: script_name is passed as basename only, because we cd into $TEST_DIR below.
    local script_name="$1"

    # Ensure the script is executable (check for existence since we are inside TEST_DIR)
    if [ ! -x "$script_name" ]; then
        echo -e "${YELLOW}SKIP${NC}: $script_name is not executable."
        return 0
    fi

    TOTAL_TESTS=$((TOTAL_TESTS + 1))

    local start_time=$(date +%s.%N)

    # Run the test, redirecting stdout and stderr to temporary files
    local temp_stdout=$(mktemp)
    local temp_stderr=$(mktemp)

    # We run in a subshell (()) to capture the specific test's output/exit status cleanly
    # We execute using the script_name only.
    if ("./$script_name" 1>"$temp_stdout" 2>"$temp_stderr"); then
        # --- SUCCESS ---
        local end_time=$(date +%s.%N)
        PASSED_COUNT=$((PASSED_COUNT + 1))
        local duration=$(echo "$end_time - $start_time" | bc -l)
        printf "${GREEN}PASS${NC}: %-30s (Time: %.3fs)\n" "$script_name" "$duration"
    else
        # --- FAILURE ---
        local end_time=$(date +%s.%N)
        FAILED_COUNT=$((FAILED_COUNT + 1))
        local duration=$(echo "$end_time - $start_time" | bc -l)

        # Print failure status immediately
        printf "${RED}FAIL${NC}: %-30s (Time: %.3fs)\n" "$script_name" "$duration"

        # Append detailed failure output to the summary
        FAILURE_OUTPUT+="\n--- FAILURE OUTPUT STDOUT: $script_name ---\n"
        FAILURE_OUTPUT+=$(cat "$temp_stdout")
        FAILURE_OUTPUT+="\n--- FAILURE OUTPUT STDERR: $script_name ---\n"
        FAILURE_OUTPUT+=$(cat "$temp_stderr")
        FAILURE_OUTPUT+="\n--- FAILURE OUTPUT END: $script_name ---\n"
        EXIT_CODE=1
    fi

    # Cleanup temporary files
    rm -f "$temp_stdout" "$temp_stderr"
}

# --- Main Execution ---

echo -e "${BLUE}--- Running logkv Tests ---${NC}"
GLOBAL_START_TIME=$(date +%s.%N)

# 1. Find all test scripts excluding the cleanup script, and store their BASENAMES
TEST_SCRIPT_BASENAMES=$(find "$TEST_DIR" -maxdepth 1 -type f -name "*.sh" ! -name "$CLEAN_SCRIPT" -exec basename {} \; | sort)

# Change directory to tests/ (required for scripts to find their source files)
if [ -d "$TEST_DIR" ]; then
    # pushd changes directory and stores the previous one, popd returns.
    pushd "$TEST_DIR" > /dev/null || exit 1
else
    echo -e "${RED}FATAL: Test directory $TEST_DIR not found.${NC}"
    exit 1
fi

# 2. Run all main test scripts
for script_name in $TEST_SCRIPT_BASENAMES; do
    run_test "$script_name"
done

# 3. Run the cleanup script last from within the tests/ directory
echo
echo -e "${YELLOW}--- Running Cleanup Script ---${NC}"
if [ -f "$CLEAN_SCRIPT" ]; then
    CLEAN_START_TIME=$(date +%s.%N)
    # Run the cleanup script using its basename (since we are in the directory)
    if ("./$CLEAN_SCRIPT"); then
        CLEAN_END_TIME=$(date +%s.%N)
        CLEAN_DURATION=$(echo "$CLEAN_END_TIME - $CLEAN_START_TIME" | bc -l)
        echo -e "${GREEN}PASS${NC}: $CLEAN_SCRIPT (Time: ${CLEAN_DURATION}s)"
    else
        echo -e "${RED}FAIL${NC}: $CLEAN_SCRIPT failed to clean up."
        EXIT_CODE=1
    fi
else
    echo -e "${YELLOW}SKIP${NC}: $CLEAN_SCRIPT not found."
fi

# Change back to the project root
popd > /dev/null

# 4. Calculate total time
GLOBAL_END_TIME=$(date +%s.%N)
TOTAL_TIME=$(echo "$GLOBAL_END_TIME - $GLOBAL_START_TIME" | bc -l)

echo
echo -e "${BLUE}--- Test Summary ---${NC}"

# 5. Print failure output if any
if [ -n "$FAILURE_OUTPUT" ]; then
    echo -e "$FAILURE_OUTPUT"
fi

# 6. Print final tally
echo "Total Time: ${TOTAL_TIME}s"
echo "Total Tests Run: $TOTAL_TESTS"
echo -e "Passed: ${PASSED_COUNT}"
echo -e "Failed: ${FAILED_COUNT}"
echo ""
if [ $FAILED_COUNT -gt 0 ]; then
  echo -e "Result: ${RED}FAILED${NC}"
else
  echo -e "Result: ${GREEN}PASSED${NC}"
fi

exit $EXIT_CODE