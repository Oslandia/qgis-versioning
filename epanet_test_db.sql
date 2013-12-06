/*
To test the versioning we create a network of pipes
the pipes are along x 
the x coordinate of the end of the pipe is the version number
the y coordinate denotes branches (i.e for the trunk y is in [-0.5,0.5])
*/
CREATE EXTENSION postgis;

CREATE SCHEMA epanet;

-- TODO: deal with the UNIQUE constrain of references (point to primary key instead of id)
-- because of history, the UNIQUE constrain may not be satisfied
-- GOTCHA: on merging, if there are relations on primary keys, things will not work nicely
CREATE TABLE epanet.junctions (
    hid serial PRIMARY KEY,
    id varchar, -- UNIQUE, 
    elevation float, 
    base_demand_flow float, 
    demand_pattern_id varchar, 
    geom public.geometry, 
    CONSTRAINT enforce_dims_geom CHECK ((public.st_ndims(geom) = 2)),
    CONSTRAINT enforce_geotype_geom CHECK (((public.geometrytype(geom) = 'POINT'::text) OR (geom IS NULL))),
    CONSTRAINT enforce_srid_geom CHECK ((public.st_srid(geom) = 2154))
);

INSERT INTO epanet.junctions
    (hid,id, elevation, geom)
    VALUES
    (1,'0',0,ST_GeometryFromText('POINT(0 0)',2154));

INSERT INTO epanet.junctions
    (hid,id, elevation, geom)
    VALUES
    (2,'1',1,ST_GeometryFromText('POINT(1 0)',2154));

-- Avoid custom types that will not translate well to spatialite
--CREATE TYPE epanet.pipe_status AS ENUM ('OPEN', 'CLOSED', 'CV');

CREATE TABLE epanet.pipes (
    hid serial PRIMARY KEY,
    id varchar, --UNIQUE, 
    start_node varchar, -- REFERENCES epanet.junctions(id), 
    end_node varchar, -- REFERENCES epanet.junctions(id), 
    length float,
    diameter float,
    roughness float,
    minor_loss_coefficient float,
    status varchar,
    geom public.geometry, 
    CONSTRAINT enforce_dims_geom CHECK ((public.st_ndims(geom) = 2)),
    CONSTRAINT enforce_geotype_geom CHECK (((public.geometrytype(geom) = 'LINESTRING'::text) OR (geom IS NULL))),
    CONSTRAINT enforce_srid_geom CHECK ((public.st_srid(geom) = 2154))
);

INSERT INTO epanet.pipes
    (hid, id, start_node, end_node, length, diameter, geom) 
    VALUES
    (1,'0','0','1',1,2,ST_GeometryFromText('LINESTRING(0 0,1 0)',2154));

/*
Versionning the table by adding 4 columns for each branch (here the trunk)
*/
SELECT 'create first revision' AS msg;
CREATE TABLE epanet.revisions(
    rev serial PRIMARY KEY,
    commit_msg varchar,
    branch varchar DEFAULT 'trunk',
    date timestamp DEFAULT current_timestamp);

ALTER TABLE epanet.junctions
ADD COLUMN trunk_rev_begin integer REFERENCES epanet.revisions(rev), 
ADD COLUMN trunk_rev_end integer REFERENCES epanet.revisions(rev), 
ADD COLUMN trunk_parent integer REFERENCES epanet.junctions(hid),
ADD COLUMN trunk_child  integer REFERENCES epanet.junctions(hid);

ALTER TABLE epanet.pipes
ADD COLUMN trunk_rev_begin integer REFERENCES epanet.revisions(rev), 
ADD COLUMN trunk_rev_end integer REFERENCES epanet.revisions(rev), 
ADD COLUMN trunk_parent integer REFERENCES epanet.pipes(hid),
ADD COLUMN trunk_child  integer REFERENCES epanet.pipes(hid);

INSERT INTO epanet.revisions VALUES (1,'initial commit','trunk');

UPDATE epanet.junctions SET trunk_rev_begin = 1;

UPDATE epanet.pipes SET trunk_rev_begin = 1;

/*
Create a readonly checkout for last revision of trunk
*/
CREATE SCHEMA epanet_trunk_rev_head;

CREATE VIEW epanet_trunk_rev_head.junctions
AS SELECT hid, id, elevation, base_demand_flow, demand_pattern_id, geom
   FROM epanet.junctions
   WHERE trunk_rev_end IS NULL AND trunk_rev_begin IS NOT NULL;
  
CREATE VIEW epanet_trunk_rev_head.pipes
AS SELECT  hid, id, start_node, end_node, length, diameter, roughness, minor_loss_coefficient, status, geom 
   FROM epanet.pipes
   WHERE trunk_rev_end IS NULL AND trunk_rev_begin IS NOT NULL;

/*
Create a readonly checkout for rev 1 of trunk
*/
CREATE SCHEMA epanet_trunk_rev_1;

CREATE VIEW epanet_trunk_rev_1.junctions
AS SELECT hid, id, elevation, base_demand_flow, demand_pattern_id, geom
   FROM epanet.junctions
   WHERE ( trunk_rev_end IS NULL OR trunk_rev_end >=1 ) AND trunk_rev_begin <= 1;
  
CREATE VIEW epanet_trunk_rev_1.pipes
AS SELECT  hid, id, start_node, end_node, length, diameter, roughness, minor_loss_coefficient, status, geom 
   FROM epanet.pipes
   WHERE ( trunk_rev_end IS NULL OR trunk_rev_end >=1 ) AND trunk_rev_begin <= 1;
 
/*
create the second revision
*/
SELECT 'create second revision' AS msg;
INSERT INTO epanet.revisions VALUES (2,'second commit','trunk');

INSERT INTO epanet.junctions
    (hid,id, elevation, geom, trunk_rev_begin)
    VALUES
    (3,'1.5',1.5,ST_GeometryFromText('POINT(1.5 0)',2154), 2);

INSERT INTO epanet.junctions
    (hid,id, elevation, geom, trunk_rev_begin)
    VALUES
    (4,'2',2,ST_GeometryFromText('POINT(2 0)',2154), 2);

INSERT INTO epanet.pipes
    (hid, id, start_node, end_node, length, diameter, geom, trunk_rev_begin) 
    VALUES
    (2,'1','1','1.5',0.5,2,ST_GeometryFromText('LINESTRING(1 0,1.5 0)',2154), 2);

INSERT INTO epanet.pipes
    (hid, id, start_node, end_node, length, diameter, geom, trunk_rev_begin) 
    VALUES
    (3,'2','1.5','2',0.5,2,ST_GeometryFromText('LINESTRING(1.5 0,2 0)',2154), 2);

/*
Create a readonly checkout for rev 2 of trunk
*/
CREATE SCHEMA epanet_trunk_rev_2;

CREATE VIEW epanet_trunk_rev_2.junctions
AS SELECT hid, id, elevation, base_demand_flow, demand_pattern_id, geom
   FROM epanet.junctions
   WHERE ( trunk_rev_end IS NULL OR trunk_rev_end >=2 ) AND trunk_rev_begin <= 2;
  
CREATE VIEW epanet_trunk_rev_2.pipes
AS SELECT  hid, id, start_node, end_node, length, diameter, roughness, minor_loss_coefficient, status, geom 
   FROM epanet.pipes
   WHERE ( trunk_rev_end IS NULL OR trunk_rev_end >=2 ) AND trunk_rev_begin <= 2;
 
/*
create the third revision
*/
SELECT 'create third revision' AS msg;
INSERT INTO epanet.revisions VALUES (3,'third commit','trunk');

-- conceptually remove the first junction added in second commit
UPDATE epanet.junctions SET trunk_rev_end = 2 WHERE hid = 3;
-- conceptually update the second junction added in second commit
INSERT INTO epanet.junctions
    (hid,id, elevation, geom, trunk_rev_begin, trunk_parent)
    VALUES
    (5,'3',1,ST_GeometryFromText('POINT(3 0)',2154), 3, 4);
UPDATE epanet.junctions SET (trunk_rev_end, trunk_child) = (2, 5) WHERE hid = 4;

-- conceptually remove the first pipe added in second commit
UPDATE epanet.pipes SET trunk_rev_end = 2 WHERE hid = 2;
-- conceptually update the second pipe added in second commit
INSERT INTO epanet.pipes
    (hid, id, start_node, end_node, length, diameter, geom, trunk_rev_begin, trunk_parent) 
    VALUES
    (4,'3','1','3',2,2,ST_GeometryFromText('LINESTRING(1 0,3 0)',2154), 3, 3);
UPDATE epanet.pipes SET (trunk_rev_end, trunk_child) = (2, 4) WHERE hid = 3;

/*
Create a readonly checkout for rev 3 of trunk
*/
CREATE SCHEMA epanet_trunk_rev_3;

CREATE VIEW epanet_trunk_rev_3.junctions
AS SELECT hid, id, elevation, base_demand_flow, demand_pattern_id, geom
   FROM epanet.junctions
   WHERE ( trunk_rev_end IS NULL OR trunk_rev_end >=3 ) AND trunk_rev_begin <= 3;
  
CREATE VIEW epanet_trunk_rev_3.pipes
AS SELECT  hid, id, start_node, end_node, length, diameter, roughness, minor_loss_coefficient, status, geom 
   FROM epanet.pipes
   WHERE ( trunk_rev_end IS NULL OR trunk_rev_end >=3 ) AND trunk_rev_begin <= 3;

/*
Test views
*/
SELECT * FROM epanet_trunk_rev_1.junctions;
SELECT * FROM epanet_trunk_rev_2.junctions;
SELECT * FROM epanet_trunk_rev_3.junctions;

/*
Build diff between 1 and 2
*/
SELECT hid AS insert_hid 
FROM epanet.junctions 
WHERE trunk_rev_begin = 2 AND trunk_parent IS NULL;

SELECT hid AS delete_hid 
FROM epanet.junctions 
WHERE trunk_rev_end = 1 AND trunk_child IS NULL;

SELECT old.hid AS old_hid, '->' AS direction, new.hid AS new_hid
FROM epanet.junctions AS old, epanet.junctions AS new 
WHERE old.trunk_rev_end = 1 
      AND new.trunk_rev_begin = 2
      AND old.trunk_child = new.trunk_parent;

/*
Build diff between 2 and 3
*/
SELECT hid AS insert_hid 
FROM epanet.junctions 
WHERE trunk_rev_begin = 3 AND trunk_parent IS NULL;

SELECT hid AS delete_hid 
FROM epanet.junctions 
WHERE trunk_rev_end = 2 AND trunk_child IS NULL;

\timing
-- we don't need the cross join to update, this is for debugging
SELECT old.hid AS old_hid, new.hid AS new_hid
FROM epanet.junctions AS old, epanet.junctions AS new 
WHERE old.trunk_rev_end = 2 AND new.trunk_rev_begin = 3 AND old.trunk_child = new.hid;
-- this is the same thing, without the cross joint
SELECT trunk_parent AS old_hid, hid AS new_hid
FROM epanet.junctions 
WHERE trunk_rev_begin = 3 AND trunk_parent IS NOT NULL;
-- this is the same thing, to show that the info is duplicated
SELECT hid AS old_hid, trunk_child AS new_hid
FROM epanet.junctions 
WHERE trunk_rev_end = 2 AND trunk_child IS NOT NULL;


