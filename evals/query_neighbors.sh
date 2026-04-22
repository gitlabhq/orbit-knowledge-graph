#!/bin/bash

MR_ID=245615
TYPES=(
  "HAS_LABEL|Label"
  "HAS_NOTE|Note"
  "IN_MILESTONE|Milestone"
  "CLOSES|WorkItem"
  "FIXES|Vulnerability"
  "TARGETS|Branch"
  "TRIGGERED|Pipeline"
  "APPROVED|User"
  "ASSIGNED|User"
  "MERGED|User"
  "REVIEWER|User"
)

for relationship in "${TYPES[@]}"; do
  IFS='|' read -r rel_type entity <<< "$relationship"
  
  # Check if it's an incoming or outgoing relationship
  case "$rel_type" in
    APPROVED|MERGED|REVIEWER|ASSIGNED)
      # Incoming relationships (reverse direction)
      python tools/orbit_query.py query << EOF
{
  "query_type": "traversal",
  "nodes": [
    { "id": "other", "entity": "$entity" },
    { "id": "mr", "entity": "MergeRequest", "filters": { "id": $MR_ID } }
  ],
  "relationships": [
    { "from": "other", "to": "mr", "type": "$rel_type" }
  ],
  "limit": 100
}
EOF
      ;;
    *)
      # Outgoing relationships
      python tools/orbit_query.py query << EOF
{
  "query_type": "traversal",
  "nodes": [
    { "id": "mr", "entity": "MergeRequest", "filters": { "id": $MR_ID } },
    { "id": "other", "entity": "$entity" }
  ],
  "relationships": [
    { "from": "mr", "to": "other", "type": "$rel_type" }
  ],
  "limit": 100
}
EOF
      ;;
  esac
  
  echo "---"
done
