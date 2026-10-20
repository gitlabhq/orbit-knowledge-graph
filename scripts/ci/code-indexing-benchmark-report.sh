#!/usr/bin/env bash

set -euo pipefail

input_file="${1:-/dev/stdin}"

jq -r '
  def format_time:
    if . < 1 then
      "\((. * 1000 | round)) ms"
    else
      "\(((. * 1000 | round) / 1000)) s"
    end;

  def format_delta($current):
    ((. / $current - 1) * 100 | round) as $percent
    | if $percent > 0 then "+\($percent)%" else "\($percent)%" end;

  .metadata as $metadata
  | [
      .scenario_results[]
      | {
          repository: .scenario_name,
          engine: (.architecture | sub("^linux-x86_64-"; "")),
          mean: .hyperfine_result.mean
        }
    ]
  | group_by(.repository)
  | map({
      repository: .[0].repository,
      current: (map(select(.engine == "code-graph"))[0].mean),
      incremental: (map(select(.engine == "code-graph-incremental"))[0].mean)
    })
  | if all(.current != null and .incremental != null) then . else error("each repository must have both engine results") end
  | sort_by(.repository) as $results
  | ($results | map(.current) | add) as $current_total
  | ($results | map(.incremental) | add) as $incremental_total
  | [
      "### Code indexing benchmark results",
      "",
      "Commit `\($metadata.commit_hash_short)` on `\($metadata.branch)` | [pipeline](\($metadata.pipeline_url)) | [job](\($metadata.job_url))",
      "",
      "Positive deltas mean the incremental engine is slower.",
      "",
      "| Repository | Current `code-graph` | Incremental | Delta |",
      "|:--|--:|--:|--:|",
      (
        $results[]
        | .current as $current
        | "| \(.repository) | \($current | format_time) | \(.incremental | format_time) | \(.incremental | format_delta($current)) |"
      ),
      "| **Total** | **\($current_total | format_time)** | **\($incremental_total | format_time)** | **\($incremental_total | format_delta($current_total))** |"
    ]
  | .[]
' "$input_file"
