SELECT load_extension('./spatialite_functions.so','vs_init');

CREATE TABLE test( id int, child int );
INSERT INTO test VALUES (1,2);
INSERT INTO test VALUES (2,3);
INSERT INTO test VALUES (3,4);
INSERT INTO test VALUES (4,5);
INSERT INTO test(id) VALUES (5);

INSERT INTO test VALUES (6,7);
INSERT INTO test(id) VALUES (7);

SELECT * FROM test;

-- SELECT vsleaf( id, child ) FROM test WHERE id > 1;
-- SELECT vsleaf( id, child ) FROM test WHERE id = 1;

PRAGMA recursive_triggers = TRUE;

CREATE TEMP TABLE Path (id INTEGER, child INTEGER);

CREATE TRIGGER update_path AFTER UPDATE ON Path 
BEGIN
    UPDATE Path SET id = (SELECT child FROM test WHERE test.id = old.child ) ;
END;

CREATE TRIGGER add_path AFTER INSERT ON Path 
BEGIN
    UPDATE Path SET child = (SELECT child FROM test WHERE test.id = new.id );
END;

SELECT '---Path---';
INSERT INTO Path VALUES (1,2);

SELECT * FROM Path;
