-- In a spotify_incremental_load SQL file, we have some duplicate data to test our project end-to-end. So for that, we need to drop the primary key constraints from our tables.
-- Query to find the primary key value of the table
SELECT name
FROM sys.key_constraints
WHERE type = 'PK' AND parent_object_id = OBJECT_ID('<table_name>')

-- Query to drop the primary key of the table
ALTER TABLE [dbo].[DimArtist]
DROP CONSTRAINT <primary-key-here>
