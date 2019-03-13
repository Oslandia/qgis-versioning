CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA epanet;

CREATE TABLE epanet.junctions (
    id serial PRIMARY KEY,
    elevation float, 
    base_demand_flow float, 
    demand_pattern_id varchar, 
    geom geometry('POINT',2154)
);

INSERT INTO epanet.junctions
    (elevation, geom)
    VALUES
    (0,ST_GeometryFromText('POINT(1 0)',2154));

INSERT INTO epanet.junctions
    (elevation, geom)
    VALUES
    (1,ST_GeometryFromText('POINT(0 1)',2154));

CREATE TABLE epanet.pipes (
    id serial PRIMARY KEY,
    start_node integer references epanet.junctions(id),
    end_node integer references epanet.junctions(id),
    start_node integer references e,
    end_node varchar,
    length float,
    diameter float,
    roughness float,
    minor_loss_coefficient float,
    status varchar,
    geom geometry('LINESTRING',2154)
);

INSERT INTO epanet.pipes
    (start_node, end_node, length, diameter, geom) 
    VALUES
    (1,2,1,2,ST_GeometryFromText('LINESTRING(1 0,0 1)',2154));

CREATE TABLE epanet.revisions(
    rev serial PRIMARY KEY,
    commit_msg varchar,
    branch varchar DEFAULT 'trunk',
    date timestamp DEFAULT current_timestamp,
    author varchar);
INSERT INTO epanet.revisions VALUES (1,'initial commit','trunk');

ALTER TABLE epanet.junctions
ADD COLUMN trunk_rev_begin integer REFERENCES epanet.revisions(rev), 
ADD COLUMN trunk_rev_end integer REFERENCES epanet.revisions(rev), 
ADD COLUMN trunk_parent integer REFERENCES epanet.junctions(jid),
ADD COLUMN trunk_child  integer REFERENCES epanet.junctions(jid);

ALTER TABLE epanet.pipes
ADD COLUMN trunk_rev_begin integer REFERENCES epanet.revisions(rev), 
ADD COLUMN trunk_rev_end integer REFERENCES epanet.revisions(rev), 
ADD COLUMN trunk_parent integer REFERENCES epanet.pipes(pid),
ADD COLUMN trunk_child  integer REFERENCES epanet.pipes(pid);

UPDATE epanet.junctions SET trunk_rev_begin = 1;

UPDATE epanet.pipes SET trunk_rev_begin = 1;

CREATE SCHEMA epanet_trunk_rev_head;

CREATE VIEW epanet_trunk_rev_head.junctions
AS SELECT jid, id, elevation, base_demand_flow, demand_pattern_id, geom
   FROM epanet.junctions
   WHERE trunk_rev_end IS NULL AND trunk_rev_begin IS NOT NULL;
  
CREATE VIEW epanet_trunk_rev_head.pipes
AS SELECT  pid, id, start_node, end_node, length, diameter, roughness, minor_loss_coefficient, status, geom
   FROM epanet.pipes
   WHERE trunk_rev_end IS NULL AND trunk_rev_begin IS NOT NULL;

