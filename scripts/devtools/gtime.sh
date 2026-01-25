#!/bin/bash

# Check if gtime is available
if ! command -v gtime &> /dev/null; then
  echo '{"error": "gtime command not found in PATH"}'
  exit 1
fi

# Check if a path argument was provided
if [ -z "$1" ]; then
  echo '{"error": "No path argument provided"}'
  exit 1
fi

# Run gtime and capture output
GTIME_OUTPUT=$(gtime --verbose cargo run --release --bin gkg index "$1" 2>&1 > /dev/null)

# Parse the gtime output
COMMAND=$(echo "$GTIME_OUTPUT" | grep "Command being timed:" | sed 's/.*Command being timed: "\(.*\)"/\1/')
USER_TIME=$(echo "$GTIME_OUTPUT" | grep "User time" | grep -o '[0-9.]*')
SYSTEM_TIME=$(echo "$GTIME_OUTPUT" | grep "System time" | grep -o '[0-9.]*')
CPU_PERCENT=$(echo "$GTIME_OUTPUT" | grep "Percent of CPU" | grep -o '[0-9]*%' | tr -d '%')
ELAPSED_TIME=$(echo "$GTIME_OUTPUT" | grep "Elapsed (wall clock) time" | grep -o '[0-9:]*' | tail -1)

# Memory metrics
AVG_SHARED_TEXT=$(echo "$GTIME_OUTPUT" | grep "Average shared text size" | grep -o '[0-9]*')
AVG_UNSHARED_DATA=$(echo "$GTIME_OUTPUT" | grep "Average unshared data size" | grep -o '[0-9]*')
AVG_STACK=$(echo "$GTIME_OUTPUT" | grep "Average stack size" | grep -o '[0-9]*')
AVG_TOTAL=$(echo "$GTIME_OUTPUT" | grep "Average total size" | grep -o '[0-9]*')
MAX_RSS_KB=$(echo "$GTIME_OUTPUT" | grep "Maximum resident set size" | grep -o '[0-9]*')
AVG_RSS_KB=$(echo "$GTIME_OUTPUT" | grep "Average resident set size" | grep -o '[0-9]*')

# Page faults
MAJOR_FAULTS=$(echo "$GTIME_OUTPUT" | grep "Major (requiring I/O) page faults:" | grep -o '[0-9]*')
MINOR_FAULTS=$(echo "$GTIME_OUTPUT" | grep "Minor (reclaiming a frame) page faults:" | grep -o '[0-9]*')

# Context switches
VOLUNTARY_CTX=$(echo "$GTIME_OUTPUT" | grep "Voluntary context switches:" | grep -o '[0-9]*')
INVOLUNTARY_CTX=$(echo "$GTIME_OUTPUT" | grep "Involuntary context switches:" | grep -o '[0-9]*')

# I/O and system metrics
SWAPS=$(echo "$GTIME_OUTPUT" | grep "Swaps:" | grep -o '[0-9]*')
FS_INPUTS=$(echo "$GTIME_OUTPUT" | grep "File system inputs:" | grep -o '[0-9]*')
FS_OUTPUTS=$(echo "$GTIME_OUTPUT" | grep "File system outputs:" | grep -o '[0-9]*')
SOCKET_MSG_SENT=$(echo "$GTIME_OUTPUT" | grep "Socket messages sent:" | grep -o '[0-9]*')
SOCKET_MSG_RECV=$(echo "$GTIME_OUTPUT" | grep "Socket messages received:" | grep -o '[0-9]*')
SIGNALS=$(echo "$GTIME_OUTPUT" | grep "Signals delivered:" | grep -o '[0-9]*')
PAGE_SIZE=$(echo "$GTIME_OUTPUT" | grep "Page size" | grep -o '[0-9]*')

# Exit status
EXIT_STATUS=$(echo "$GTIME_OUTPUT" | grep "Exit status:" | grep -o '[0-9]*')

# Convert RSS to MB and GB
MAX_RSS_MB=$(echo "scale=2; $MAX_RSS_KB / 1024" | bc)
MAX_RSS_GB=$(echo "scale=2; $MAX_RSS_KB / 1024 / 1024" | bc)

# Output structured JSON
echo "{
  \"path\": \"$1\",
  \"command\": \"$COMMAND\",
  \"timing\": {
    \"user_time_seconds\": $USER_TIME,
    \"system_time_seconds\": $SYSTEM_TIME,
    \"cpu_percent\": $CPU_PERCENT,
    \"elapsed_time\": \"$ELAPSED_TIME\"
  },
  \"memory\": {
    \"average_shared_text_size_kb\": $AVG_SHARED_TEXT,
    \"average_unshared_data_size_kb\": $AVG_UNSHARED_DATA,
    \"average_stack_size_kb\": $AVG_STACK,
    \"average_total_size_kb\": $AVG_TOTAL,
    \"maximum_resident_set_size_kb\": $MAX_RSS_KB,
    \"maximum_resident_set_size_mb\": $MAX_RSS_MB,
    \"maximum_resident_set_size_gb\": $MAX_RSS_GB,
    \"average_resident_set_size_kb\": $AVG_RSS_KB
  },
  \"page_faults\": {
    \"major\": $MAJOR_FAULTS,
    \"minor\": $MINOR_FAULTS
  },
  \"context_switches\": {
    \"voluntary\": $VOLUNTARY_CTX,
    \"involuntary\": $INVOLUNTARY_CTX
  },
  \"io_and_system\": {
    \"swaps\": $SWAPS,
    \"file_system_inputs\": $FS_INPUTS,
    \"file_system_outputs\": $FS_OUTPUTS,
    \"socket_messages_sent\": $SOCKET_MSG_SENT,
    \"socket_messages_received\": $SOCKET_MSG_RECV,
    \"signals_delivered\": $SIGNALS,
    \"page_size_bytes\": $PAGE_SIZE
  },
  \"exit_status\": $EXIT_STATUS
}"
