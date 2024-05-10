create view std.trains as
	        select time_bucket('10 seconds', timestamp, '-5 seconds'::INTERVAL) + '5 seconds' as timestamp, rit_id as id,
            avg(st_x(st_transform(location, 28992))) as x, avg(st_y(st_transform(location, 28992))) as y,
            avg(snelheid) as speed, avg(richting) as direction, avg(horizontale_nauwkeurigheid) as accuracy,
            string_agg(type, ';') as type, string_agg(bron, ';') as source
        from raw.ns_trains
        group by timestamp, rit_id
        order by timestamp asc, rit_id asc
;