/* == global example of setting up a transparent history table with rules == */

drop schema if exists refgeo cascade;
create schema refgeo;

drop table if exists refgeo.hzone cascade;
create table refgeo.hzone (
	hid serial				-- historical id
	, id serial				-- normal id
	, name varchar				-- zone name
	, funcid varchar			-- functional identifier
	, geom geometry				-- geometry for the zone
	, sdate timestamp	default current_timestamp	-- start date : data is valid from that date on
	, edate timestamp	default null	-- end date : data is valid until that date
);

-- the view to current data
create or replace view 
	refgeo.zone as 
select
	id
	, name
	, funcid
	, geom
from
	refgeo.hzone
where
	edate is null;

-- make this view updatable
-- this mechanism will be easier with PG >= 9.3

-- insert
create rule 
	refgeo_zone_ins as
on insert to 
	refgeo.zone 
do instead
insert into 
	refgeo.hzone (id, name, funcid, geom) 
values 
	(NEW.id, NEW.name, NEW.funcid, NEW.geom);

-- update
create rule 
	refgeo_zone_up as
on update to 
	refgeo.zone 
do instead
(
-- insert a new row to the table with new values
-- warning : the view filter gets propagated to these queries. Therefore we have to do the insert before
-- 		or the update will set the edate and the insert won't be executed as the view filter would
--		not let any result pass.
insert into 
	refgeo.hzone (id, name, funcid, geom) 
values 
	(NEW.id, NEW.name, NEW.funcid, NEW.geom);
-- now update the second most recent entry (ie not the one we just inserted) for this id and set its end date
update 
	refgeo.hzone 
set 
	edate = current_timestamp 
where 
	id = OLD.id 
	and edate is null 
	and hid = (
		select 
			nth_value(hid, 2) over (partition by id order by sdate asc RANGE BETWEEN UNBOUNDED PRECEDING AND unbounded following ) 
		from 
			refgeo.hzone 
		where 
			id = OLD.id 
		limit 1
	)
);

-- datsup current value instead of delete
create rule 
	refgeo_zone_del as
on delete to 
	refgeo.zone 
do instead
update 
	refgeo.hzone 
set 
	edate = current_timestamp 
where 
	id = OLD.id 
	and edate is null;

/* == end of setup == */

/* == test it == */
truncate refgeo.hzone;

-- insert old and new data
insert into 
	refgeo.hzone (id, name, funcid, geom, sdate, edate)
select 
	n as id
	, 'Point ' || n::text as name
	, n as funcid
	, st_makepoint(random() * 100, random() * 100)
	, current_timestamp - interval '1 month' as sdate
	, current_timestamp as edate
from 
	generate_series(1, 1000) as n
union
select 
	n as id
	, 'Point ' || n::text as name	
	, n + 1000 as funcid
	, st_makepoint(random() * 100, random() * 100)
	, current_timestamp as sdate
	, null as edate
from 
	generate_series(1, 1000) as n;

-- get all data
select * from refgeo.hzone;

-- get current data
select * from refgeo.zone;

-- insert new data
insert into refgeo.zone (id, name, funcid, geom) values (3500, 'Point 3500', 3500, 'POINT(33 33)'::geometry);

-- see new data
select * from refgeo.zone order by id desc limit 10;
select * from refgeo.hzone order by id desc limit 10;

-- delete our point
delete from refgeo.zone where id = 3500;
select * from refgeo.zone order by id desc limit 10;
select * from refgeo.hzone order by id desc limit 10;

-- insert new data
insert into refgeo.zone (id, name, funcid, geom) values (3500, 'Point 3500 bis', 3500, 'POINT(42 42)'::geometry);
select * from refgeo.zone order by id desc limit 10;
select * from refgeo.hzone order by id desc limit 10;

-- update our point
update refgeo.zone set geom = 'POINT(42 42)'::geometry, name = 'Point 43 (was 3500)' where id = 3500;
select * from refgeo.zone order by id desc limit 10;
select * from refgeo.hzone order by id desc limit 10;

