CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA myschema;
CREATE TABLE myschema.referenced (
id1 integer,
id2 integer,
name varchar,
geom geometry('POINT', 2154),
PRIMARY KEY (id1, id2)
);

CREATE TABLE myschema.referencing (
id integer PRIMARY KEY,
fkid1 integer,
fkid2 integer,
name varchar,
geom geometry('POINT', 2154),
FOREIGN KEY (fkid1, fkid2) REFERENCES myschema.referenced (id1, id2)
);

INSERT INTO myschema.referenced (id1, id2, name, geom) VALUES (1,18, 'toto', ST_GeometryFromText('POINT(0 0)',2154));
INSERT INTO myschema.referenced (id1, id2, name, geom) VALUES (42,4, 'titi', ST_GeometryFromText('POINT(0 0)',2154));

INSERT INTO myschema.referencing (id, fkid1, fkid2, name, geom)
VALUES (16, 1,18, 'fk_toto', ST_GeometryFromText('POINT(0 0)',2154));
