SELECT
    traversal_path,
    id,
    toString(uuid) AS uuid,
    nullIf(JSONExtractString(finding_data, 'name'), '') AS name,
    nullIf(JSONExtractString(finding_data, 'description'), '') AS description,
    nullIf(JSONExtractString(finding_data, 'solution'), '') AS solution,
    severity,
    deduplicated,
    toString(overridden_uuid) AS overridden_uuid,
    project_id,
    scanner_id,
    scan_id,
    partition_number,
    {{watermark_column}},
    {{deleted_column}}
FROM siphon_security_findings
WHERE startsWith(traversal_path, {traversal_path:String})
