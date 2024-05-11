create or replace view std.trains as
	select distinct on (timestamp, id)
	date_bin('10 seconds', timestamp, TIMESTAMP '2001-01-01') as timestamp, rit_id as id,
	st_x(st_transform(location, 28992)) as x, st_y(st_transform(location, 28992)) as y,
	snelheid as speed, richting as direction, horizontale_nauwkeurigheid as accuracy,
	type as type, bron as source
	from raw.ns_trains
	order by timestamp asc, rit_id asc
;
