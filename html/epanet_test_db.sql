CREATE SCHEMA epanet;

CREATE TABLE epanet.junctions (
    fid serial PRIMARY KEY,
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
    fid serial PRIMARY KEY,
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
ADD COLUMN hid serial UNIQUE, 
ADD COLUMN trunk_rev_begin integer REFERENCES epanet.revisions(rev), 
ADD COLUMN trunk_rev_end integer REFERENCES epanet.revisions(rev), 
ADD COLUMN trunk_parent integer REFERENCES epanet.junctions(hid),
ADD COLUMN trunk_child  integer REFERENCES epanet.junctions(hid);

ALTER TABLE epanet.pipes
ADD COLUMN hid serial UNIQUE, 
ADD COLUMN trunk_rev_begin integer REFERENCES epanet.revisions(rev), 
ADD COLUMN trunk_rev_end integer REFERENCES epanet.revisions(rev), 
ADD COLUMN trunk_parent integer REFERENCES epanet.pipes(hid),
ADD COLUMN trunk_child  integer REFERENCES epanet.pipes(hid);

UPDATE epanet.junctions SET trunk_rev_begin = 1;

UPDATE epanet.pipes SET trunk_rev_begin = 1;

CREATE SCHEMA epanet_trunk_rev_head;

CREATE VIEW epanet_trunk_rev_head.junctions
AS SELECT hid, id, elevation, base_demand_flow, demand_pattern_id, geom::geometry('POINT',2154)
   FROM epanet.junctions
   WHERE trunk_rev_end IS NULL AND trunk_rev_begin IS NOT NULL;
  
CREATE VIEW epanet_trunk_rev_head.pipes
AS SELECT  hid, id, start_node, end_node, length, diameter, roughness, minor_loss_coefficient, status, geom::geometry('LINESTRING',2154)
   FROM epanet.pipes
   WHERE trunk_rev_end IS NULL AND trunk_rev_begin IS NOT NULL;

