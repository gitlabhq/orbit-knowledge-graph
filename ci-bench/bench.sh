#!/bin/bash
# Throwaway benchmark for orbit-cli local indexing on a runner. Env: ORBIT_BIN (path), RUNS (default 2)
set -u
cg=/sys/fs/cgroup
echo "== env"; uname -r; grep -m1 'model name' /proc/cpuinfo; echo "cpuinfo processors: $(grep -c ^processor /proc/cpuinfo)"; echo "nproc: $(nproc)"
echo "cgroup fs: $(stat -fc %T $cg)"; cat /proc/self/cgroup
for f in cpu.max cpu.weight memory.max memory.swap.max cpu.cfs_quota_us cpu.cfs_period_us memory.limit_in_bytes; do [ -f $cg/$f ] && echo "$f=$(cat $cg/$f)"; done
free -m | head -2; df -h . | tail -1; mount | grep -E ' / | /builds' | head -3
echo "== cpu speed (single / 2 parallel sha256 of 1GiB; compares per-core speed + SMT siblings)"
t() { local s=$(date +%s.%N); "$@"; echo "$(echo "$(date +%s.%N)-$s"|bc)"; }
one() { head -c 1073741824 /dev/zero | sha256sum >/dev/null; }
echo "single: $(t one)"; echo "par2: $( { s=$(date +%s.%N); one & one & wait; echo "$(date +%s.%N)-$s"|bc; } )"
echo "== fetch corpus"
mkdir -p /tmp/corpus && cd /tmp/corpus && git init -q . && s=$(date +%s)
git fetch -q --depth 1 https://gitlab.com/gitlab-org/gitlab.git 6b457c5ca0c3d2bdf6cfa7ca4d9fb451ef907ef8 && git checkout -q FETCH_HEAD
echo "fetch+checkout_s=$(( $(date +%s)-s )) files=$(git ls-files | wc -l) head=$(git rev-parse HEAD)"
export ORBIT_TELEMETRY_ENABLED=false
for i in $(seq 1 ${RUNS:-2}); do
  export ORBIT_DATA_DIR=/tmp/odata$i; rm -rf $ORBIT_DATA_DIR
  echo "== run $i ($ORBIT_BIN) uptime: $(cat /proc/loadavg)"
  stat0=$(head -1 /proc/stat); cs0=$(cat $cg/cpu.stat 2>/dev/null | tr '\n' ' ')
  s=$(date +%s.%N)
  $ORBIT_BIN index --stats . > /tmp/out.$i 2>&1 &
  pid=$!; maxthr=0; samples=""
  while kill -0 $pid 2>/dev/null; do thr=$(awk '/^Threads/{print $2}' /proc/$pid/status 2>/dev/null); [ -n "$thr" ] && [ "$thr" -gt "$maxthr" ] && maxthr=$thr; hw=$(awk '/^VmHWM/{print $2}' /proc/$pid/status 2>/dev/null); [ -n "$hw" ] && hwm=$hw; sleep 0.25; done
  wait $pid; rc=$?
  e=$(date +%s.%N); stat1=$(head -1 /proc/stat); cs1=$(cat $cg/cpu.stat 2>/dev/null | tr '\n' ' ')
  echo "run=$i rc=$rc wall=$(echo "$e-$s"|bc) max_threads=$maxthr vmhwm_kb=${hwm:-?}"
  echo "/proc/stat cpu before: $stat0"; echo "/proc/stat cpu after:  $stat1  (fields: user nice system idle iowait irq softirq steal)"
  echo "cpu.stat before: $cs0"; echo "cpu.stat after:  $cs1"
  echo "memory.peak=$(cat $cg/memory.peak 2>/dev/null) memory.events: $(tr '\n' ' ' < $cg/memory.events 2>/dev/null)"
  grep -E '"time_seconds"|"files"|errored' /tmp/out.$i | head -5
done
