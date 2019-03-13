-- Create table to store PRIMARY KEY and FOREIGN KEY constraints
-- before deleting them
CREATE TABLE {schema}.versioning_constraints (
  table_from varchar,
  columns_from varchar[],
  table_to varchar,
  columns_to varchar[]);

-- populate constraints table
INSERT INTO {schema}.versioning_constraints 
SELECT conrelid::regclass AS table_from,
       (select array_agg(att.attname)
	  from (select unnest(conkey) as key) as keys,
	       pg_attribute att where att.attrelid = conrelid and att.attnum = keys.key) as column_from,
       confrelid::regclass AS table_to,
       (select array_agg(att.attname)
	  from (select unnest(confkey) as key) as keys,
	       pg_attribute att where att.attrelid = conrelid and att.attnum = keys.key) as column_to
  FROM   pg_constraint c, pg_namespace n
 WHERE    n.oid = c.connamespace
      AND  contype IN ('f', 'p ')
      AND    n.nspname = '{schema}' ;

-- Drop foreign keys and primary keys 
DO
  $do$
  DECLARE
c record;
BEGIN 
  FOR c IN
    SELECT pgc.conname as name, pgc.conrelid::regclass as table
    FROM   pg_constraint pgc, pg_namespace pgn
    WHERE  pgn.oid = pgc.connamespace
    AND    pgc.contype IN ('f', 'p ')
    AND    pgn.nspname = '{schema}'
    ORDER BY pgc.contype ASC -- foreign keys first
    LOOP
    EXECUTE 'ALTER TABLE ' || c.table || ' DROP CONSTRAINT ' || c.name;
  END LOOP;
END
$do$;

-- Add the versioning_hid primary key
DO
  $$
  DECLARE
rec record;
BEGIN 
  FOR rec IN
    SELECT schemaname, tablename 
    FROM pg_catalog.pg_tables 
    WHERE schemaname = '{schema}' 
    AND tablename != 'versioning_constraints'
    LOOP
    EXECUTE 'ALTER TABLE ' || rec.schemaname || '.' || rec.tablename
      || ' ADD COLUMN versioning_hid SERIAL PRIMARY KEY';
  END LOOP;
END
$$;

-- Create revisions table
CREATE TABLE {schema}.revisions (
  rev serial PRIMARY KEY,
  commit_msg varchar,
  branch varchar DEFAULT 'trunk',
  date timestamp DEFAULT current_timestamp,
  author varchar);

