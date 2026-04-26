#!/bin/bash

# Parse --legacy flag (v2 is the production default; pass --legacy to opt
# into the v1 indexer for back-to-back memory comparisons).
LEGACY_FLAG=""
POSITIONAL=()
for arg in "$@"; do
  case $arg in
    --legacy) LEGACY_FLAG="--legacy"; shift ;;
    *) POSITIONAL+=("$arg"); shift ;;
  esac
done
set -- "${POSITIONAL[@]}"

if [ -z "$1" ]; then
  echo '{"error": "Usage: ./average_rss.sh [--legacy] <path> [sampling_interval_seconds]"}'
  echo '{"example": "./average_rss.sh ../gdk/gitlab 0.025"}'
  exit 1
fi

# Configuration: Sampling interval in seconds (default: 0.025 = 25ms)
SAMPLING_INTERVAL=${2:-0.025}

# Build the binary first
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ORBIT_BIN="$REPO_ROOT/target/release/orbit"

echo "Building orbit binary..." >&2
cargo build --bin orbit --release --features duckdb-client/bundled >&2
if [ $? -ne 0 ]; then
  echo '{"error": "Failed to build orbit binary"}' >&2
  exit 1
fi

# Start the process in the background
"$ORBIT_BIN" index $LEGACY_FLAG "$1" > /dev/null 2>&1 &
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
  CHART_HEIGHT=20
  NUM_BANDS=${3:-8}  # configurable via 3rd arg, default 8

  # Bucket samples into NUM_BANDS time bands
  samples_per_band=$(( (num_samples + NUM_BANDS - 1) / NUM_BANDS ))

  # For each band: compute min, avg, max, p95
  band_mins=()
  band_avgs=()
  band_maxs=()
  band_p95s=()
  band_times=()

  for (( b=0; b<NUM_BANDS && b*samples_per_band<num_samples; b++ )); do
    start=$((b * samples_per_band))
    end=$((start + samples_per_band))
    if [ $end -gt $num_samples ]; then end=$num_samples; fi
    count=$((end - start))

    b_min=${samples[$start]}
    b_max=${samples[$start]}
    b_sum=0
    sorted_band=()
    for (( j=start; j<end; j++ )); do
      v=${samples[$j]}
      sorted_band+=($v)
      b_sum=$((b_sum + v))
      if [ $v -gt $b_max ]; then b_max=$v; fi
      if [ $v -lt $b_min ]; then b_min=$v; fi
    done
    b_avg=$((b_sum / count))

    # Sort for p95
    IFS=$'\n' sorted_band=($(sort -n <<< "${sorted_band[*]}")); unset IFS
    p95_idx=$(( count * 95 / 100 ))
    if [ $p95_idx -ge $count ]; then p95_idx=$((count - 1)); fi
    b_p95=${sorted_band[$p95_idx]}

    band_mins+=($b_min)
    band_avgs+=($b_avg)
    band_maxs+=($b_max)
    band_p95s+=($b_p95)
    band_time=$(echo "scale=1; ($start + $count / 2) * $SAMPLING_INTERVAL" | bc)
    band_times+=($band_time)
  done

  actual_bands=${#band_maxs[@]}

  # Y-axis scale: round max up to nearest GB
  max_gb_ceil=$(echo "($max_rss + 1048575) / 1048576" | bc)
  if [ "$max_gb_ceil" -lt 1 ]; then max_gb_ceil=1; fi
  y_max=$((max_gb_ceil * 1048576))

  # Column width per band (total chart ~60 chars)
  col_w=$(( 60 / actual_bands ))
  if [ $col_w -lt 5 ]; then col_w=5; fi
  chart_w=$((col_w * actual_bands))

  echo "" >&2
  echo "RSS over time (${total_time}s, ${num_samples} samples, ${actual_bands} bands):" >&2
  echo "  ▓ = p95-max   █ = avg-p95   ░ = min-avg" >&2
  echo "" >&2

  # Render rows top-down
  for (( row=CHART_HEIGHT; row>=1; row-- )); do
    threshold=$(( y_max * row / CHART_HEIGHT ))
    lower=$(( y_max * (row - 1) / CHART_HEIGHT ))
    threshold_gb=$(echo "scale=1; $threshold / 1048576" | bc)
    if [ $((row % 4)) -eq 0 ] || [ $row -eq $CHART_HEIGHT ]; then
      printf "%5sGB │" "$threshold_gb" >&2
    else
      printf "       │" >&2
    fi

    for (( b=0; b<actual_bands; b++ )); do
      bmin=${band_mins[$b]}
      bavg=${band_avgs[$b]}
      bp95=${band_p95s[$b]}
      bmax=${band_maxs[$b]}

      # For each char in the column
      for (( c=0; c<col_w; c++ )); do
        # Row midpoint in KB
        mid=$(( (threshold + lower) / 2 ))
        if [ $mid -le $bmax ] && [ $mid -ge $bp95 ]; then
          printf "▓" >&2
        elif [ $mid -lt $bp95 ] && [ $mid -ge $bavg ]; then
          printf "█" >&2
        elif [ $mid -lt $bavg ] && [ $mid -ge $bmin ]; then
          printf "░" >&2
        else
          printf " " >&2
        fi
      done
    done
    echo "" >&2
  done

  # X-axis
  printf "       └" >&2
  for (( c=0; c<chart_w; c++ )); do printf "─" >&2; done
  echo "" >&2

  # Time labels
  printf "        " >&2
  for (( b=0; b<actual_bands; b++ )); do
    t=${band_times[$b]}
    label="${t}s"
    padded=$(printf "%-${col_w}s" "$label")
    printf "%s" "$padded" >&2
  done
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
