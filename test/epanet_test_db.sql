CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA epanet;

CREATE TABLE epanet.junctions (
    id serial PRIMARY KEY,
    elevation float, 
    base_demand_flow float, 
    demand_pattern_id varchar, 
    geom geometry('POINT',2154)
);

CREATE TABLE epanet.pipes (
    id serial PRIMARY KEY,
    start_node integer references epanet.junctions(id),
    end_node integer references epanet.junctions(id),
    length float,
    diameter float,
    roughness float,
    minor_loss_coefficient float,
    status varchar,
    geom geometry('LINESTRING',2154)
);

-- INSERT DATA (Use to identify the data insertion block in test, do not remove this line!!!)

INSERT INTO epanet.junctions
    (elevation, geom)
    VALUES
    (0,ST_GeometryFromText('POINT(1 0)',2154));

INSERT INTO epanet.junctions
    (elevation, geom)
    VALUES
    (1,ST_GeometryFromText('POINT(0 1)',2154));


INSERT INTO epanet.pipes
    (start_node, end_node, length, diameter, geom) 
    VALUES
    (1,2,1,2,ST_GeometryFromText('LINESTRING(1 0,0 1)',2154));
