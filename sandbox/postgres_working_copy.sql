CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA epanet;

CREATE TABLE epanet.junctions (
    id varchar,
    elevation float, 
    base_demand_flow float, 
    demand_pattern_id varchar, 
    geom geometry('POINT',2154)
);

INSERT INTO epanet.junctions
    (id, elevation, geom)
    VALUES
    ('0',0,ST_GeometryFromText('POINT(1 0)',2154));

INSERT INTO epanet.junctions
    (id, elevation, geom)
    VALUES
    ('1',1,ST_GeometryFromText('POINT(0 1)',2154));

CREATE TABLE epanet.pipes (
    id varchar,
    start_node varchar,
    end_node varchar,
    length float,
    diameter float,
    roughness float,
    minor_loss_coefficient float,
    status varchar,
    geom geometry('LINESTRING',2154)
);

INSERT INTO epanet.pipes
    (id, start_node, end_node, length, diameter, geom) 
    VALUES
    ('0','0','1',1,2,ST_GeometryFromText('LINESTRING(1 0,0 1)',2154));

CREATE TABLE epanet.revisions(
    rev serial PRIMARY KEY,
    commit_msg varchar,
    branch varchar DEFAULT 'trunk',
    date timestamp DEFAULT current_timestamp,
    author varchar);
INSERT INTO epanet.revisions VALUES (1,'initial commit','trunk');

ALTER TABLE epanet.junctions
ADD COLUMN hid serial PRIMARY KEY, 
ADD COLUMN trunk_rev_begin integer REFERENCES epanet.revisions(rev), 
ADD COLUMN trunk_rev_end integer REFERENCES epanet.revisions(rev), 
ADD COLUMN trunk_parent integer REFERENCES epanet.junctions(hid),
ADD COLUMN trunk_child  integer REFERENCES epanet.junctions(hid);

ALTER TABLE epanet.pipes
ADD COLUMN hid serial PRIMARY KEY, 
ADD COLUMN trunk_rev_begin integer REFERENCES epanet.revisions(rev), 
ADD COLUMN trunk_rev_end integer REFERENCES epanet.revisions(rev), 
ADD COLUMN trunk_parent integer REFERENCES epanet.pipes(hid),
ADD COLUMN trunk_child  integer REFERENCES epanet.pipes(hid);

UPDATE epanet.junctions SET trunk_rev_begin = 1;

UPDATE epanet.pipes SET trunk_rev_begin = 1;

CREATE SCHEMA epanet_trunk_rev_head;

CREATE VIEW epanet_trunk_rev_head.junctions
AS SELECT hid, id, elevation, base_demand_flow, demand_pattern_id, geom
   FROM epanet.junctions
   WHERE trunk_rev_end IS NULL AND trunk_rev_begin IS NOT NULL;
  
CREATE VIEW epanet_trunk_rev_head.pipes
AS SELECT  hid, id, start_node, end_node, length, diameter, roughness, minor_loss_coefficient, status, geom
   FROM epanet.pipes
   WHERE trunk_rev_end IS NULL AND trunk_rev_begin IS NOT NULL;



CREATE SCHEMA epanet_working_copy;

CREATE TABLE epanet_working_copy.initial_revision AS SELECT (SELECT MAX(rev) FROM epanet.revisions) AS rev, 'trunk' AS branch, 'epanet' AS table_schema, 'junctions' AS table_name, (SELECT MAX(hid) FROM epanet.junctions) AS max_hid;
;
INSERT INTO epanet_working_copy.initial_revision VALUES ((SELECT MAX(rev) FROM epanet.revisions), 'trunk', 'epanet', 'pipes', (SELECT MAX(hid) FROM epanet.pipes));

CREATE TABLE epanet_working_copy.junctions_diff AS SELECT hid, id, elevation, base_demand_flow, demand_pattern_id, geom, trunk_rev_begin, trunk_rev_end, trunk_parent, trunk_child FROM epanet.junctions WHERE False;

CREATE TABLE epanet_working_copy.pipes_diff AS SELECT hid, id, start_node, end_node, length, diameter, roughness, minor_loss_coefficient, status, geom, trunk_rev_begin, trunk_rev_end, trunk_parent, trunk_child FROM epanet.pipes WHERE False;

INSERT INTO epanet_working_copy.junctions_diff VALUES(1, '1', 2, 0, NULL, ST_GeometryFromText('POINT(0 2)',2154), (SELECT trunk_rev_begin FROM epanet.junctions WHERE hid = 1) ,(SELECT MAX(rev) FROM epanet_working_copy.initial_revision), NULL, 3  );
INSERT INTO epanet_working_copy.junctions_diff VALUES(3, '3', 3, 0, NULL, ST_GeometryFromText('POINT(0 3)',2154), (SELECT MAX(rev)+1 FROM epanet_working_copy.initial_revision), NULL, 1, NULL  );

\a
SELECT hid, trunk_rev_begin, trunk_rev_end, trunk_parent, trunk_child FROM epanet.junctions;
SELECT hid, trunk_rev_begin, trunk_rev_end, trunk_parent, trunk_child FROM epanet_working_copy.junctions_diff;

CREATE VIEW epanet_working_copy.junctions_view AS
SELECT * 
FROM (SELECT DISTINCT ON (hid) * 
    FROM (  SELECT hid, trunk_rev_begin, trunk_rev_end, trunk_parent, trunk_child
                FROM epanet_working_copy.junctions_diff 
            UNION ALL 
            SELECT hid, trunk_rev_begin, trunk_rev_end, trunk_parent, trunk_child 
                FROM epanet.junctions WHERE trunk_rev_begin <= (SELECT MAX(rev) FROM epanet_working_copy.initial_revision) AND (trunk_rev_end IS NULL OR trunk_rev_end >= (SELECT MAX(rev) FROM epanet_working_copy.initial_revision) ) ) AS src 
    ORDER BY hid, trunk_rev_end ASC ) AS merged 
WHERE trunk_rev_end IS NULL;

SELECT * FROM epanet_working_copy.junctions_view;





