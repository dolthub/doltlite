WITH
  commits AS MATERIALIZED (SELECT * FROM dolt_log(:revision)),
  refs AS (
    SELECT commit_hash FROM commits
    UNION
    SELECT a.parent_hash FROM commits c
    JOIN dolt_commit_ancestors a ON a.commit_hash=c.commit_hash
    WHERE a.parent_hash IS NOT NULL AND a.parent_index<2
  ),
  versions AS MATERIALIZED (
    SELECT r.commit_hash,
      CASE WHEN EXISTS (
        SELECT 1 FROM dolt_schema_diff('WORKING',r.commit_hash) s
        WHERE s.to_table_name=:table_name
          AND (upper(s.to_create_statement) LIKE 'CREATE TABLE %'
            OR upper(s.to_create_statement) LIKE 'CREATE VIRTUAL TABLE %')
      ) OR (
        EXISTS (SELECT 1 FROM main.sqlite_schema
                WHERE type='table' AND name=:table_name)
        AND NOT EXISTS (
          SELECT 1 FROM dolt_schema_diff('WORKING',r.commit_hash) s
          WHERE s.from_table_name=:table_name
            AND (s.to_table_name IS NOT :table_name
              OR NOT (upper(s.to_create_statement) LIKE 'CREATE TABLE %'
                   OR upper(s.to_create_statement) LIKE 'CREATE VIRTUAL TABLE %'))
        )
      ) THEN dolt_hashof_table(:table_name,r.commit_hash) END AS table_hash
    FROM refs r
  ),
  parents AS MATERIALIZED (
    SELECT c.commit_hash,v.table_hash AS child_hash,p.table_hash AS parent_hash
    FROM commits c
    JOIN versions v ON v.commit_hash=c.commit_hash
    JOIN dolt_commit_ancestors a ON a.commit_hash=c.commit_hash
    JOIN versions p ON p.commit_hash=a.parent_hash
    WHERE a.parent_hash IS NOT NULL AND a.parent_index<2
  )
SELECT c.* FROM commits c
WHERE EXISTS (
  SELECT 1 FROM parents p WHERE p.commit_hash=c.commit_hash
    AND p.parent_hash IS NOT NULL AND p.child_hash IS NOT p.parent_hash
) OR EXISTS (
  SELECT 1 FROM parents p WHERE p.commit_hash=c.commit_hash
  GROUP BY p.commit_hash
  HAVING count(p.parent_hash)=0 AND max(p.child_hash) IS NOT NULL
);
