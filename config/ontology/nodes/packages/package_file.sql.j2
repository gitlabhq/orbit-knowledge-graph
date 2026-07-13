SELECT
  id,
  package_id,
  file_name,
  size,
  if(
    match(file_sha256, '^\\\\x[0-9a-fA-F]{128}$'),
    unhex(substring(file_sha256, 3)),
    NULL
  ) AS file_sha256,
  status,
  created_at,
  updated_at,
  project_id,
  traversal_path,
  {{watermark_column}} AS _version,
  {{deleted_column}} AS _deleted
FROM siphon_packages_package_files
WHERE startsWith(traversal_path, {traversal_path:String}) {{filters}}
ORDER BY traversal_path, id
LIMIT {{batch_size}}
