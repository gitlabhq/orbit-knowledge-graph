#!/bin/bash

# Parse --v2 flag
V2_FLAG=""
POSITIONAL=()
for arg in "$@"; do
  case $arg in
    --v2) V2_FLAG="--v2"; shift ;;
    *) POSITIONAL+=("$arg"); shift ;;
  esac
done
set -- "${POSITIONAL[@]}"

if [ -z "$1" ]; then
  echo '{"error": "Usage: ./average_rss.sh [--v2] <path> [sampling_interval_seconds]"}'
  echo '{"example": "./average_rss.sh --v2 ../gdk/gitlab 0.025"}'
  exit 1
fi

# Configuration: Sampling interval in seconds (default: 0.025 = 25ms)
SAMPLING_INTERVAL=${2:-0.025}

# Start the process in the background
cargo run --release --bin orbit index $V2_FLAG "$1" > /dev/null 2>&1 &
PROCESS_PID=$!

# Wait a moment for the process to start
sleep 0.1

# Check if the process actually started
if ! kill -0 $PROCESS_PID 2>/dev/null; then
  echo '{"error": "Failed to start gkg process"}'
  exit 1
fi

# Initialize variables
total_rss=0
num_samples=0
max_rss=0
min_rss=-1
samples=()

echo "Monitoring process gkg index process(pid=$PROCESS_PID, path=$1, v2=$V2_FLAG, sampling_interval=$SAMPLING_INTERVAL ms)..." >&2

# Sample RSS while the process is running
while kill -0 $PROCESS_PID 2>/dev/null; do
  # Use ps to get RSS in KB (works on both Mac and Linux)
  rss_kb=$(ps -p $PROCESS_PID -o rss= 2>/dev/null | tr -d ' ')
  
  if [ -n "$rss_kb" ] && [ "$rss_kb" -gt 0 ]; then
    total_rss=$((total_rss + rss_kb))
    num_samples=$((num_samples + 1))
    samples+=($rss_kb)
    
    # Track max
    if [ $rss_kb -gt $max_rss ]; then
      max_rss=$rss_kb
    fi
    
    # Track min
    if [ $min_rss -eq -1 ] || [ $rss_kb -lt $min_rss ]; then
      min_rss=$rss_kb
    fi
  fi
  
  sleep $SAMPLING_INTERVAL
done

# Wait for the process to fully complete
wait $PROCESS_PID
EXIT_CODE=$?

# Calculate average
if [ $num_samples -gt 0 ]; then
  average_rss=$((total_rss / num_samples))
  
  # Convert to MB and GB
  average_rss_mb=$(echo "scale=2; $average_rss / 1024" | bc)
  average_rss_gb=$(echo "scale=2; $average_rss / 1024 / 1024" | bc)
  
  max_rss_mb=$(echo "scale=2; $max_rss / 1024" | bc)
  max_rss_gb=$(echo "scale=2; $max_rss / 1024 / 1024" | bc)
  
  min_rss_mb=$(echo "scale=2; $min_rss / 1024" | bc)
  min_rss_gb=$(echo "scale=2; $min_rss / 1024 / 1024" | bc)
  total_time=$(echo "scale=2; $num_samples*$SAMPLING_INTERVAL" | bc)
  
  # ── ASCII chart ──────────────────────────────────────────
  CHART_WIDTH=60
  CHART_HEIGHT=15

  # Bucket samples into CHART_WIDTH time slots
  samples_per_bucket=$(( (num_samples + CHART_WIDTH - 1) / CHART_WIDTH ))
  buckets=()
  for (( i=0; i<CHART_WIDTH && i*samples_per_bucket<num_samples; i++ )); do
    start=$((i * samples_per_bucket))
    bucket_max=0
    for (( j=start; j<start+samples_per_bucket && j<num_samples; j++ )); do
      if [ ${samples[$j]} -gt $bucket_max ]; then
        bucket_max=${samples[$j]}
      fi
    done
    buckets+=($bucket_max)
  done

  # Render chart
  num_buckets=${#buckets[@]}
  # Round max_rss up to nearest GB for y-axis
  max_gb_ceil=$(echo "($max_rss + 1048575) / 1048576" | bc)
  if [ "$max_gb_ceil" -lt 1 ]; then max_gb_ceil=1; fi
  y_max=$((max_gb_ceil * 1048576))  # back to KB

  echo "" >&2
  echo "RSS over time (${total_time}s, ${num_samples} samples, peak ${max_rss_mb}MB):" >&2

  for (( row=CHART_HEIGHT; row>=1; row-- )); do
    threshold=$(( y_max * row / CHART_HEIGHT ))
    threshold_gb=$(echo "scale=1; $threshold / 1048576" | bc)
    if [ $((row % 3)) -eq 0 ] || [ $row -eq $CHART_HEIGHT ]; then
      printf "%5sGB │" "$threshold_gb" >&2
    else
      printf "       │" >&2
    fi
    for (( col=0; col<num_buckets; col++ )); do
      val_kb=${buckets[$col]}
      if [ $val_kb -ge $threshold ]; then
        printf "█" >&2
      else
        # Check if we're in the lower portion (partial fill)
        lower_threshold=$(( y_max * (row - 1) / CHART_HEIGHT ))
        if [ $val_kb -ge $lower_threshold ] && [ $val_kb -lt $threshold ]; then
          printf "▄" >&2
        else
          printf " " >&2
        fi
      fi
    done
    echo "" >&2
  done

  # X-axis
  printf "       └" >&2
  for (( col=0; col<num_buckets; col++ )); do printf "─" >&2; done
  echo "" >&2

  # Time labels
  printf "        0" >&2
  mid_time=$(echo "scale=1; $total_time / 2" | bc)
  end_time=$total_time
  padding=$(( num_buckets / 2 - 2 ))
  if [ $padding -gt 0 ]; then
    printf "%${padding}s%s" "" "${mid_time}s" >&2
    padding2=$(( num_buckets - num_buckets / 2 - ${#end_time} - 3 ))
    if [ $padding2 -gt 0 ]; then
      printf "%${padding2}s%ss" "" "$end_time" >&2
    fi
  fi
  echo "" >&2
  echo "" >&2

  # Output structured JSON
  echo "{
  \"path\": \"$1\",
  \"v2\": $([ -n "$V2_FLAG" ] && echo "true" || echo "false"),
  \"process_id\": $PROCESS_PID,
  \"exit_code\": $EXIT_CODE,
  \"sampling\": {
    \"num_samples\": $num_samples,
    \"interval_seconds\": $SAMPLING_INTERVAL,
    \"total_time_seconds\": $total_time
  },
  \"rss\": {
    \"average_kb\": $average_rss,
    \"average_mb\": $average_rss_mb,
    \"average_gb\": $average_rss_gb,
    \"max_kb\": $max_rss,
    \"max_mb\": $max_rss_mb,
    \"max_gb\": $max_rss_gb,
    \"min_kb\": $min_rss,
    \"min_mb\": $min_rss_mb,
    \"min_gb\": $min_rss_gb
  }
}"
else
  echo '{"error": "No samples collected", "exit_code": '$EXIT_CODE'}'
  exit 1
fi
