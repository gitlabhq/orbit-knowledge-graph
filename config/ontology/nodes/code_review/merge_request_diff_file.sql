SELECT
    sipHash64(file.merge_request_diff_id, file.relative_order) AS id,
    diff.merge_request_id AS merge_request_id,
    file.merge_request_diff_id AS merge_request_diff_id,
    diff.project_id AS project_id,
    file.too_large AS too_large,
    file.new_path AS new_path,
    file.old_path AS old_path,
    file.new_file AS new_file,
    file.renamed_file AS renamed_file,
    file.deleted_file AS deleted_file,
    file.binary AS binary,
    file.generated AS generated,
    file.a_mode AS a_mode,
    file.b_mode AS b_mode,
    diff.traversal_path AS traversal_path,
    file.relative_order AS relative_order,
    file.{{watermark_column}} AS {{watermark_column}},
    file.{{deleted_column}} AS {{deleted_column}}
FROM siphon_merge_request_diff_files AS file
INNER JOIN (
    SELECT id, merge_request_id, project_id, traversal_path
    FROM siphon_merge_request_diffs
    WHERE startsWith(traversal_path, {traversal_path:String})
) AS diff ON file.merge_request_diff_id = diff.id
WHERE startsWith(file.traversal_path, {traversal_path:String})
