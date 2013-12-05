-- add the constrains lost in translation (postgis->spatilite) such that we can
-- update the new hid and have posgres update the child hid fields accordingly
-- since those field where updated through views, its pretty sure it will work
ALTER TABLE epanet_test.junctions 
ADD CONSTRAINT junctions_trunk_child_fkey FOREIGN KEY(trunk_child) REFERENCES epanet_test.junctions(hid) ON UPDATE CASCADE;

ALTER TABLE epanet_test.pipes 
ADD CONSTRAINT pipes_trunk_child_fkey FOREIGN KEY(trunk_child) REFERENCES epanet_test.pipes(hid) ON UPDATE CASCADE;

/*
first check if commits have been made to the database since the checkout
result must be one to allow commit
*/
SELECT 4 - dest.rev FROM (SELECT MAX(rev) AS rev FROM epanet.revisions) AS dest; 

/*
in case commit occured, we need to update stuff and detect conflicts
*/
BEGIN;

-- where hid already exists, we need to bump hid of added rows
WITH dest_next AS (SELECT MAX(hid) AS hid FROM epanet.pipes),
dest_prev AS ( SELECT MAX(hid) AS hid 
               FROM epanet.pipes 
               WHERE  ( trunk_rev_end IS NULL OR trunk_rev_end >=3 ) AND trunk_rev_begin <= 3)
UPDATE epanet_test.pipes AS src SET hid = src.hid + dest_next.hid - dest_prev.hid 
FROM dest_prev, dest_next
WHERE src.hid > dest_prev.hid AND src.hid != src.hid + dest_next.hid - dest_prev.hid ; 

WITH dest_next AS (SELECT MAX(hid) AS hid FROM epanet.junctions),
dest_prev AS ( SELECT MAX(hid) AS hid 
               FROM epanet.junctions 
               WHERE  ( trunk_rev_end IS NULL OR trunk_rev_end >=3 ) AND trunk_rev_begin <= 3)
UPDATE epanet_test.junctions AS src SET hid = src.hid + dest_next.hid - dest_prev.hid 
FROM dest_prev, dest_next
WHERE src.hid > dest_prev.hid AND src.hid != src.hid + dest_next.hid - dest_prev.hid ; 

SELECT MAX(rev)+1 AS next FROM epanet.revisions;

-- revision number in trunk_parent and trunk_child needs to be updated
WITH dest AS (SELECT MAX(rev)+1 AS next FROM epanet.revisions)
UPDATE epanet_test.junctions SET trunk_rev_end = dest.next-1
FROM dest 
WHERE trunk_rev_end = 3 AND trunk_rev_end != dest.next-1;

WITH dest AS (SELECT MAX(rev)+1 AS next FROM epanet.revisions)
UPDATE epanet_test.junctions SET trunk_rev_begin = dest.next
FROM dest 
WHERE trunk_rev_begin = 4 AND trunk_rev_begin != dest.next;

WITH dest AS (SELECT MAX(rev)+1 AS next FROM epanet.revisions)
UPDATE epanet_test.pipes SET trunk_rev_end = dest.next-1 
FROM dest 
WHERE trunk_rev_end = 3 AND trunk_rev_end != dest.next-1;

WITH dest AS (SELECT MAX(rev)+1 AS next FROM epanet.revisions)
UPDATE epanet_test.pipes SET trunk_rev_begin = dest.next
FROM dest 
WHERE trunk_rev_begin = 4 AND trunk_rev_begin != dest.next;

/*
TODO Detect conflicts
for deleted/updated rows, we check if the row has been modified by someone else (trunk_child is not null)
*/


/*
merge if no conflicts
*/

INSERT INTO epanet.revisions VALUES (4,'merge local','trunk');

-- insert inserted and modified
INSERT INTO epanet.junctions(hid,id, elevation, geom, trunk_rev_begin, trunk_parent)
    SELECT hid,id, elevation, geom, trunk_rev_begin, trunk_parent
    FROM epanet_test.junctions 
    WHERE trunk_rev_begin = 4;

INSERT INTO epanet.pipes(hid, id, start_node, end_node, length, diameter, geom, trunk_rev_begin, trunk_parent)
    SELECT hid, id, start_node, end_node, length, diameter, geom, trunk_rev_begin, trunk_parent
    FROM epanet_test.pipes 
    WHERE trunk_rev_begin = 4;

-- update deleted and modified 
UPDATE epanet.junctions AS dest
SET (trunk_rev_end, trunk_child)=(src.trunk_rev_end, src.trunk_child)
FROM epanet_test.junctions AS src
WHERE dest.hid = src.hid AND src.trunk_rev_end = 3;

UPDATE epanet.pipes AS dest
SET (trunk_rev_end, trunk_child)=(src.trunk_rev_end, src.trunk_child)
FROM epanet_test.pipes AS src
WHERE dest.hid = src.hid AND src.trunk_rev_end = 3;

END;

/*
test in case of no conflict and no commits in between that tables are identical
*/

SELECT * 
FROM (
    SELECT hid, id, elevation, geom, trunk_rev_begin, trunk_rev_end, trunk_parent, trunk_child FROM epanet.junctions 
    EXCEPT 
    SELECT hid, id, elevation, geom, trunk_rev_begin, trunk_rev_end, trunk_parent, trunk_child FROM epanet_test.junctions) AS diff;

SELECT * 
FROM (
    SELECT hid, id, start_node, end_node, length, diameter, geom, trunk_rev_begin, trunk_rev_end, trunk_parent, trunk_child FROM epanet.pipes 
    EXCEPT 
    SELECT hid, id, start_node, end_node, length, diameter, geom, trunk_rev_begin, trunk_rev_end, trunk_parent, trunk_child FROM epanet_test.pipes) AS diff;

/*
first check if commits have been made to the database since the checkout
result must be one to allow commit
*/
SELECT 4 - dest.rev FROM (SELECT MAX(rev) AS rev FROM epanet.revisions) AS dest; 

/*
in case commit occured, we need to update stuff and detect conflicts
*/
BEGIN;

-- where hid already exists, we need to bump hid of added rows
WITH dest_next AS (SELECT MAX(hid) AS hid FROM epanet.pipes),
dest_prev AS ( SELECT MAX(hid) AS hid 
               FROM epanet.pipes 
               WHERE  ( trunk_rev_end IS NULL OR trunk_rev_end >=3 ) AND trunk_rev_begin <= 3)
UPDATE epanet_test.pipes AS src SET hid = src.hid + dest_next.hid - dest_prev.hid 
FROM dest_prev, dest_next
WHERE src.hid > dest_prev.hid AND src.hid != src.hid + dest_next.hid - dest_prev.hid ; 

WITH dest_next AS (SELECT MAX(hid) AS hid FROM epanet.junctions),
dest_prev AS ( SELECT MAX(hid) AS hid 
               FROM epanet.junctions 
               WHERE  ( trunk_rev_end IS NULL OR trunk_rev_end >=3 ) AND trunk_rev_begin <= 3)
UPDATE epanet_test.junctions AS src SET hid = src.hid + dest_next.hid - dest_prev.hid 
FROM dest_prev, dest_next
WHERE src.hid > dest_prev.hid AND src.hid != src.hid + dest_next.hid - dest_prev.hid ; 

SELECT MAX(rev)+1 AS next FROM epanet.revisions;

-- revision number in trunk_parent and trunk_child needs to be updated
WITH dest AS (SELECT MAX(rev)+1 AS next FROM epanet.revisions)
UPDATE epanet_test.junctions SET trunk_rev_end = dest.next-1
FROM dest 
WHERE trunk_rev_end = 3 AND trunk_rev_end != dest.next-1;

WITH dest AS (SELECT MAX(rev)+1 AS next FROM epanet.revisions)
UPDATE epanet_test.junctions SET trunk_rev_begin = dest.next
FROM dest 
WHERE trunk_rev_begin = 4 AND trunk_rev_begin != dest.next;

WITH dest AS (SELECT MAX(rev)+1 AS next FROM epanet.revisions)
UPDATE epanet_test.pipes SET trunk_rev_end = dest.next-1 
FROM dest 
WHERE trunk_rev_end = 3 AND trunk_rev_end != dest.next-1;

WITH dest AS (SELECT MAX(rev)+1 AS next FROM epanet.revisions)
UPDATE epanet_test.pipes SET trunk_rev_begin = dest.next
FROM dest 
WHERE trunk_rev_begin = 4 AND trunk_rev_begin != dest.next;

/*
TODO Detect conflicts
for deleted/updated rows, we check if the row has been modified by someone else (trunk_child is not null)
*/
