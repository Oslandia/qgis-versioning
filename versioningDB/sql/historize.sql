-- Create table to store PRIMARY KEY and FOREIGN KEY constraints
-- before deleting them
CREATE TABLE {schema}.versioning_constraints (
  table_from varchar,
  columns_from varchar[],
  defaults_from varchar[],
  table_to varchar,
  columns_to varchar[],
  updtype char,
  deltype char);

-- populate constraints table
INSERT INTO {schema}.versioning_constraints 
SELECT (SELECT relname FROM pg_class WHERE oid = conrelid::regclass) AS table_from,
       
       (SELECT array_agg(att.attname)
	  FROM (SELECT unnest(conkey) AS key) AS keys,
	       pg_attribute att WHERE att.attrelid = conrelid AND att.attnum = keys.key) AS columns_from,

       (SELECT array_agg(adef.adsrc)
	  FROM (SELECT unnest(conkey) AS key) AS keys
		 LEFT JOIN pg_attrdef adef ON adef.adrelid = conrelid
		     AND adef.adnum = keys.key ) AS defaults_from,

       (SELECT relname FROM pg_class WHERE oid = confrelid::regclass) as table_to,
       
       (SELECT array_agg(att.attname)
	  FROM (SELECT unnest(confkey) AS key) AS keys,
	       pg_attribute att WHERE att.attrelid = conrelid AND att.attnum = keys.key) AS columns_to,
       
       c.confupdtype as updtype,
       c.confdeltype as deltype
  FROM   pg_constraint c
	   JOIN pg_namespace n ON n.oid = c.connamespace
 WHERE  contype IN ('f', 'p ')
      AND    n.nspname = '{schema}';

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

