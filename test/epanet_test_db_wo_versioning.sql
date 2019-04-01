CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA epanet;

CREATE TABLE epanet.junctions (
    jid serial PRIMARY KEY,
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
    pid serial PRIMARY KEY,
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

CREATE TABLE epanet.areas (
    id serial PRIMARY KEY,
    name varchar,
    geom geometry('POLYGON',2154)
);

INSERT INTO epanet.areas
    (name, geom) 
    VALUES
    ('test',ST_GeometryFromText('POLYGON((0 0,0 1,1 0,0 0))',2154));
