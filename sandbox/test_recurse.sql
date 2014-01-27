-- A method for storing and retrieving hierarchical data in sqlite3
-- by using a trigger and a temporary table.
-- I needed this but had trouble finding information on it.

-- This is for sqlite3, it mostly won't work on anything else, however 
-- most databases have better ways to do this anyway.

PRAGMA recursive_triggers = TRUE; -- This is not possible before 3.6.18

-- When creating the Node table either use a primary key or some other 
-- identifier which the child node can reference.

CREATE TABLE Node (id INTEGER PRIMARY KEY, parent INTEGER, 
    label VARCHAR(16));

INSERT INTO Node (parent, label) VALUES(NULL, "root");
INSERT INTO Node (parent, label) VALUES(1, "a");
INSERT INTO Node (parent, label) VALUES(2, "b");
INSERT INTO Node (parent, label) VALUES(3, "c1");
INSERT INTO Node (parent, label) VALUES(3, "c2");

SELECT * FROM Node;

-- Create the temp table, note that node is not a primary key
-- which insures the order of the results when Node records are
-- inserted out of order

CREATE TEMP TABLE Path (node INTEGER, parent INTEGER, 
    label VARCHAR(16));

CREATE TRIGGER find_path AFTER INSERT ON Path BEGIN
    INSERT INTO Path SELECT Node.* FROM Node WHERE 
        Node.id = new.parent;
END;


-- The flaw here is that label must be unique, so when creating
-- the table there must be a unique reference for selection
-- This insert sets off the trigger find_path

INSERT INTO Path SELECT * FROM Node WHERE label = "c2";

-- Return the hierarchy in order from "root" to "c2"
SELECT 'hierarchy';
SELECT * FROM Path ORDER BY node ASC;

DROP TABLE Path; -- Important if you are staying connected


-- To test this run:
-- sqlite3 -init tree.sql tree.db



