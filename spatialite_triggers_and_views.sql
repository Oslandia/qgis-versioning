/*
Replace tables by views for current revision of trunk and create triggers
*/

-- NOTE do not do that, or reference in spatial tables, otherwise ogr2ogr fails to export
--ALTER TABLE junctions_view RENAME TO junctions;
--ALTER TABLE pipes_view RENAME TO pipes;

CREATE VIEW junctions_view
AS SELECT ROWID AS ROWID, ogc_fid, id, elevation, base_demand_flow, demand_pattern_id, geometry
   FROM junctions
   WHERE ( trunk_rev_end IS NULL OR trunk_rev_end >=4 ) AND trunk_rev_begin <= 4;
INSERT INTO views_geometry_columns
        (view_name, view_geometry, view_rowid, f_table_name, f_geometry_column)
VALUES 
        ('junctions_view', 'GEOMETRY', 'OGC_FID', 'junctions', 'GEOMETRY');  

CREATE VIEW pipes_view
AS SELECT  ROWID AS ROWID, ogc_fid, id, start_node, end_node, length, diameter, roughness, minor_loss_coefficient, status, geometry
   FROM pipes
   WHERE ( trunk_rev_end IS NULL OR trunk_rev_end >=4 ) AND trunk_rev_begin <= 4;
INSERT INTO views_geometry_columns
        (view_name, view_geometry, view_rowid, f_table_name, f_geometry_column)
VALUES 
        ('pipes_view', 'GEOMETRY', 'ROWID', 'pipes', 'GEOMETRY');  


CREATE TRIGGER update_junctions INSTEAD OF UPDATE ON junctions_view
  BEGIN
    INSERT INTO junctions 
    (ogc_fid, id, elevation, base_demand_flow, demand_pattern_id, geometry, trunk_rev_begin, trunk_parent)
    VALUES
    ((SELECT MAX(ogc_fid) + 1 FROM junctions), new.id, new.elevation, new.base_demand_flow, new.demand_pattern_id, new.geometry, 4, old.ogc_fid);
    UPDATE junctions SET trunk_rev_end = 3, trunk_child = (SELECT MAX(ogc_fid) FROM junctions) WHERE ogc_fid = old.ogc_fid;
  END;

CREATE TRIGGER insert_junctions INSTEAD OF INSERT ON junctions_view
  BEGIN
    INSERT INTO junctions 
    (ogc_fid, id, elevation, base_demand_flow, demand_pattern_id, geometry, trunk_rev_begin)
    VALUES
    ((SELECT MAX(ogc_fid) + 1 FROM junctions), new.id, new.elevation, new.base_demand_flow, new.demand_pattern_id, new.geometry, 4);
  END;

CREATE TRIGGER delete_junctions INSTEAD OF DELETE ON junctions_view
  BEGIN
    UPDATE junctions SET trunk_rev_end = 3 WHERE ogc_fid = old.ogc_fid;
  END;

CREATE TRIGGER update_pipes INSTEAD OF UPDATE ON pipes_view 
  BEGIN
    INSERT INTO pipes 
    (ogc_fid, id, start_node, end_node, length, diameter, roughness, minor_loss_coefficient, status, geometry, trunk_rev_begin, trunk_parent)
    VALUES
    ((SELECT MAX(ogc_fid) + 1 FROM pipes), new.id, new.start_node, new.end_node, new.length, new.diameter, new.roughness, new.minor_loss_coefficient, new.status, new.geometry, 4, old.ogc_fid);
    UPDATE pipes SET trunk_rev_end = 3, trunk_child = (SELECT MAX(ogc_fid) FROM pipes) WHERE ogc_fid = old.ogc_fid;
  END;

CREATE TRIGGER insert_pipes INSTEAD OF INSERT ON pipes_view 
  BEGIN
    INSERT INTO pipes 
    (ogc_fid, id, start_node, end_node, length, diameter, roughness, minor_loss_coefficient, status, geometry, trunk_rev_begin)
    VALUES
    ((SELECT MAX(ogc_fid) + 1 FROM pipes), new.id, new.start_node, new.end_node, new.length, new.diameter, new.roughness, new.minor_loss_coefficient, new.status, new.geometry, 4);
  END;

CREATE TRIGGER delete_pipes INSTEAD OF DELETE ON pipes_view 
  BEGIN
    UPDATE pipes SET trunk_rev_end = 3 WHERE ogc_fid = old.ogc_fid;
  END;


/*
Test
*/

INSERT INTO junctions_view
    (id, elevation, geometry)
    VALUES
    ('4',2,GeometryFromText('POINT(4 0)',2154));

INSERT INTO pipes_view
    (id, start_node, end_node, length, diameter, geometry) 
    VALUES
    ('4','3','4',1,2,ST_GeometryFromText('LINESTRING(3 0,4 0)',2154));

UPDATE junctions_view SET elevation=-1 WHERE id='0';

/*
Show diff
*/
SELECT 'inserted';
SELECT ogc_fid AS insert_hid 
FROM junctions
WHERE trunk_rev_begin = 4 AND trunk_parent IS NULL;

SELECT 'deleted';
SELECT ogc_fid AS delete_hid 
FROM junctions
WHERE trunk_rev_end = 3 AND trunk_child IS NULL;

SELECT 'updated';
SELECT ogc_fid AS old_hid, trunk_child AS new_hid
FROM junctions 
WHERE trunk_rev_end = 3 AND trunk_child IS NOT NULL;

--DROP VIEW pipes_view;
--DROP VIEW junctions_view;
