CREATE TABLE events3 (
 account_id UInt64,
 device_id UInt64,
 event_type Enum8(
 'Login' = 1,
 'Logout' = 2),
 country String,
 time_ns Int64,
 date_time DateTime MATERIALIZED toDateTime(time_ns / 1000000000)
)
ENGINE = MergeTree()
ORDER BY (account_id);

insert into events3 values(1, 2, 'Login', 'USA', 1614612171033000000);
insert into events3 values(1, 2, 'Login', 'USA', 1954612171033000000);
insert into events3 values(1, 3, 'Logout', 'USA', 1614612171053000000);

select partition, name, part_type, partition_id
from system.parts
where table='events3';

optimize table events3 final;

create table users (
	user_id UInt64,
	name String,
	country String,
	age UInt8,
	sign Int8
) Engine = CollapsingMergeTree(sign)
ORDER BY (user_id);

insert into users values(1, 'Alex', 'Russia', 30, 1);
insert into users values(2, 'Artem', 'Russia', 25, 1);
insert into users values(2, 'Artem', 'Russia', 25, -1);
insert into users values(2, 'Artem', 'Russia', 26, 1);

insert into users values(2, 'Alex2', 'Russia', 30, 1);
insert into users values(2, 'Artem2', 'Russia', 25, 1);
insert into users values(2, 'Artem2', 'Russia', 25, -1);

create table stats (
	user_id UInt64,
	views UInt8,
	duration UInt8,
	version UInt8,
	sign Int8
) Engine = VersionedCollapsingMergeTree(sign, version)
ORDER BY (user_id);

insert into stats values(1, 5, 100, 1, 1);
insert into stats values(1, 5, 100, 1, -1),(1, 5, 150, 2, 1)

insert into stats values(2, 5, 100, 1, 1);
insert into stats values(2, 5, 150, 2, 1)

create table stats2 (
	user_id UInt64,
	views UInt8,
	duration UInt8,
	version UInt8
) Engine = ReplacingMergeTree(version)
ORDER BY (user_id);


insert into stats2 values(1, 5, 100, 1);
insert into stats2 values(1, 5, 100, 1),(1, 5, 150, 2);

create table stats3 (
	user_id UInt64,
	views UInt8,
	duration UInt8,
	version UInt8
) Engine = ReplacingMergeTree
ORDER BY (user_id);

insert into stats3 values(1, 5, 100, 2);
insert into stats3 values(1, 5, 100, 1);


create table summing_ex (
	id UInt32,
	val1 UInt32,
	val3 UInt32,
	val2 String
) Engine = SummingMergeTree(val1) ORDER BY id;

insert into summing_ex values (1, 1, '1')  (2, 2, '1')  (3, 3, '1');
insert into summing_ex values (1, 2, '2')  (2, 3, '3')  (3, 4, '4');

create table visits (
	date_in DateTime,
	user_id UInt64,
	views UInt8,
	duration UInt8
) Engine = MergeTree
ORDER BY (date_in, user_id);

insert into visits values('2024-05-30 10:00:00', 1, 5, 100) ('2024-05-30 10:00:00', 2, 10, 150) ;
insert into visits values('2024-05-30 10:00:00', 1, 7, 200) ('2024-05-30 10:00:00', 1, 10, 150) ;

create table visits_aggregation (
	date_in DateTime,
	user_id UInt64,
	sum_views AggregateFunction(sum, UInt8),
	avg_duration AggregateFunction(avg, UInt8)
) Engine = AggregatingMergeTree()
ORDER BY (date_in, user_id);

insert into visits_aggregation select date_in, user_id, sumState(views) as sum_views, avgState(duration) as avg_duration from visits group by date_in, user_id;


create materialized view mv_visits_aggr TO visits_aggregation
as select date_in, user_id, sumState(views) as sum_views, avgState(duration) as avg_duration from visits
group by date_in, user_id;


select date_in, user_id, sumMerge(sum_views) as sum_views, avgMerge(avg_duration) as avg_duration from mv_visits_aggr
group by date_in, user_id;

