-- be sure to set your schema first
drop table if exists ns_trains;
create table if not exists ns_trains (
	timestamp timestamptz not null,
	rit_id int not null,
	snelheid float,
	richting float,
	horizontale_nauwkeurigheid float,
	type text,
	bron text
);

select addgeometrycolumn('ns_trains', 'location', 4326, 'POINT', 2);

select create_hypertable('ns_trains', by_range('timestamp', INTERVAL '1 day'));
create index on	ns_trains(rit_id, timestamp desc);