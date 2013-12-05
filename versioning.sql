DROP SCHEMA IF EXISTS versioning CASCADE;
CREATE SCHEMA versioning;

CREATE FUNCTION versioning.init(schema varchar)
    RETURNS void AS
$BODY$
    DECLARE
        schema_versioned varchar;
        table_versioned varchar;
        rec record;
        sql varchar;
    BEGIN
        schema_versioned := schema||'_versioned';
        --RAISE NOTICE 'CREATE SCHEMA %',schema_versioned;
        EXECUTE 'CREATE SCHEMA '||schema_versioned;
        --RAISE NOTICE 'DONE';
        FOR rec IN
            SELECT * FROM information_schema.tables 
            WHERE table_schema = schema
        LOOP
            table_versioned := schema_versioned||'.'||rec.table_name||'_versioned';
            sql := 'CREATE TABLE '||
                    table_versioned||
                    ' AS TABLE '||
                    schema||'.'||rec.table_name;
            --RAISE NOTICE '%', sql;
            EXECUTE sql;
            --RAISE NOTICE 'DONE';
            sql := 'ALTER TABLE '||table_versioned|| 
                   ' ADD COLUMN hid serial NOT NULL';
            --RAISE NOTICE '%', sql;
            EXECUTE sql;
            --RAISE NOTICE 'DONE';

        END LOOP;

        sql := 'CREATE TABLE '||
                    schema_versioned||'.'||'versions'
                    ' (rev serial NOT NULL,
                       msg text NOT NULL,
                       usr varchar NOT NULL,
                       date timestamp DEFAULT current_timestamp)';
        --RAISE NOTICE '%', sql;
        EXECUTE sql;
        --RAISE NOTICE 'DONE';
        sql := 'INSERT INTO '||schema_versioned||'.'||'versions'||
               '(rev,msg,usr) VALUES (1,''versioning initialisation'','||quote_literal(current_user)||')';

        --RAISE NOTICE '%', sql;
        EXECUTE sql;
        --RAISE NOTICE 'DONE';


        --EXCEPTION WHEN others THEN
    END;
$BODY$
LANGUAGE 'plpgsql' VOLATILE
COST 100;  

CREATE FUNCTION versioning.last_revision( schema varchar, 
        "branch" character varying DEFAULT 'trunk' )
    RETURNS integer AS
$BODY$
    DECLARE
        versions CONSTANT varchar DEFAULT schema||'_versioned.versions';
        res integer;
    BEGIN
        EXECUTE 'SELECT max(rev) FROM '||versions INTO res;
        RETURN res;
    END
$BODY$
LANGUAGE 'plpgsql' VOLATILE
COST 100;  

CREATE FUNCTION versioning.checkout( schema varchar, 
        branch varchar DEFAULT 'trunk', 
        revision integer DEFAULT 0 )
    RETURNS void AS
$BODY$
    BEGIN
        IF revision = 0 THEN 
            revision := versioning.last_revision(schema,branch);
        END IF;
        RAISE NOTICE '%', schema;
        RAISE NOTICE '%', branch;
        RAISE NOTICE '%', revision;
    END
$BODY$
LANGUAGE 'plpgsql' VOLATILE
COST 100;  

-- testing

select versioning.init('epanet');
select versioning.last_revision('epanet');
select versioning.checkout('epanet');

