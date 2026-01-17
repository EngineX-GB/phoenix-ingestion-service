drop database if exists db_phoenix;

create database if not exists db_phoenix;

use db_phoenix;


create table tbl_changes_tracker_temp (
	user_id text,
	telephone text,
	location text,
	username text,
	rating integer,
	nationality text
);


create table if not exists tbl_new_clients (
	oid integer not null auto_increment,
	user_id text,
	load_datetime datetime,
	primary key (oid)
);





create table if not exists tbl_audit_log (
	oid integer not null auto_increment,
	class_name text,
	method_name text,
	date_time_invoked datetime,
	parameters text,
	action text,
	primary key (oid)
);


create table if not exists tbl_client (
	oid integer not null auto_increment,
	username text,
	nationality text,
	location text,
	rating integer,
	age integer,
	rate_15_min integer,
	rate_30_min integer,
	rate_45_min integer,
	rate_1_hour integer,
	rate_1_50_hour integer,
	rate_2_hour integer,
	rate_2_50_hour integer,
	rate_3_hour integer,
	rate_3_50_hour integer,
	rate_4_hour integer,
	rate_overnight integer,
	telephone text,
	url_page text,
	refresh_time datetime,
	user_id varchar(20),
	image_available boolean,
	region text,
	gender text,
	member_since date,
	height decimal(3,2),
	dress_size integer,
	hair_colour text,
	eye_colour text,
	verified boolean,
	email varchar(255),
	preference_list text,
	ethnicity varchar(20),
	record_source varchar(15),
	primary key (oid)
);


create table if not exists tbl_client_history (
	oid integer not null auto_increment,
	client_oid integer,
	username text,
	nationality text,
	location text,
	rating integer,
	age integer,
	rate_15_min integer,
	rate_30_min integer,
	rate_45_min integer,
	rate_1_hour integer,
	rate_1_50_hour integer,
	rate_2_hour integer,
	rate_2_50_hour integer,
	rate_3_hour integer,
	rate_3_50_hour integer,
	rate_4_hour integer,
	rate_overnight integer,
	telephone text,
	url_page text,
	refresh_time datetime,
	user_id varchar(20),
	image_available boolean,
	region text,
	gender text,
	member_since date,
	height decimal(3,2),
	dress_size integer,
	hair_colour text,
	eye_colour text,
	verified boolean,
	email varchar(255),
	preference_list text,
	ethnicity varchar(20),
	record_source varchar(15),
	primary key (oid)
);


create or replace view vw_view_new_clients as
	select * from tbl_client where user_id in (select user_id from tbl_new_clients) order by rating desc;



create table if not exists tbl_client_temp (
	oid integer not null auto_increment,
	username text,
	nationality text,
	location text,
	rating integer,
	age integer,
	rate_15_min integer,
	rate_30_min integer,
	rate_45_min integer,
	rate_1_hour integer,
	rate_1_50_hour integer,
	rate_2_hour integer,
	rate_2_50_hour integer,
	rate_3_hour integer,
	rate_3_50_hour integer,
	rate_4_hour integer,
	rate_overnight integer,
	telephone text,
	url_page text,
	refresh_time datetime  DEFAULT CURRENT_TIMESTAMP,
	user_id varchar(20),
	image_available boolean,
	region text,
	gender text,
	member_since date,
	height decimal(3,2),
	dress_size integer,
	hair_colour text,
	eye_colour text,
	verified boolean,
	email varchar(255),
	preference_list text,
	ethnicity varchar(20),
	record_source varchar(15),
	primary key (oid)
);

create index idx_client_user_id on tbl_client(user_id);
create index idx_refresh_time on tbl_client(refresh_time);
create index idx_client_temp_user_id on tbl_client_temp(user_id);

create table if not exists tbl_client_price_time_series_tracking (
	oid integer not null auto_increment,
	client_oid integer not null,
	rate_15_min integer,
	rate_30_min integer,
	rate_45_min integer,
	rate_1_hour integer,
	rate_1_50_hour integer,
	rate_2_hour integer,
	rate_2_50_hour integer,
	rate_3_hour integer,
	rate_3_50_hour integer,
	rate_4_hour integer,
	rate_overnight integer,
	record_datetime datetime,
	primary key (oid),
	foreign key (client_oid) references tbl_client (oid)
);




create table if not exists tbl_client_changes_tracker (
	oid integer not null auto_increment,
	user_id text not null,
	field_value varchar(50),
	old_value varchar(255),
	new_value varchar(255),
	record_datetime datetime,
	primary key (oid)
);

create index idx_client_changes_tracker_client_oid on tbl_client_changes_tracker(oid);
create index idx_client_changes_tracker_field_value on tbl_client_changes_tracker(field_value);
create index idx_client_changes_tracker_old_value on tbl_client_changes_tracker(old_value);


create table if not exists tbl_feedback (
	oid bigint not null auto_increment,
	service_provider varchar(3),
	user_id varchar(20),
	ukp_user_id varchar(10),
	unique_identifier varchar(200),
	rating varchar(20),
	comment text,
	feedback_date date,
	primary key (oid),
	unique (unique_identifier)
);

create index idx_feedback_user_id on tbl_feedback(user_id);
create index idx_feedback_ukp_user_id on tbl_feedback(ukp_user_id);


-- 2026-01-02: used for storing the new feedback data from the api

CREATE TABLE IF NOT EXISTS tbl_feedback_v2 (
	id bigint,
	user_id varchar(20),
	username varchar(100),
	by_user_id varchar(20),
	by_username varchar(100),
	by_user_total_rating bigint,
	rating_date datetime,
	rating varchar(20),
	disputed boolean,
	feedback text,
	feedback_response text,
	rating_type varchar(100),
	user_type varchar(100),
	user_active boolean,
	PRIMARY KEY (id)
);

create index idx_feedback_v2_user_id on tbl_feedback_v2(user_id);

create table if not exists tbl_service_report_v2 (
	id bigint,
	user_id varchar(20),
	username varchar(100),
	candidate_description text,
	candidate_score int,
	by_username varchar(100),
	comments text,
	comments_score int,
	create_date text,
	exclude_affiliate bool,
	price varchar(10),	 -- fee
	location text,
	meet_date text,--
	meet_duration varchar(10),
	on_call bool, -- check
	personality text,
	personality_score int,
	rating_total bigint,
	recommend bool,
	rejected bool, -- check
	report_rating varchar(100),
	score int, -- check
	services text,
	services_score int, -- check
	venue_description text,
	venue_score int, -- check
	visit_again bool,
	primary key (id)
);


create index idx_service_report_v2_user_id on tbl_service_report_v2(user_id);

CREATE TABLE IF NOT EXISTS tbl_client_bulk_staging AS SELECT * FROM tbl_client_temp WHERE 1 = 0;











-- 04-12-21: adding time series proc cleanup:


create table if not exists tbl_time_series_data (
	oid integer auto_increment not null,
	user_id varchar(20) not null,
	day_of_week varchar(9),
	refresh_date date not null,
	load_datetime datetime not null,
	rate_15_min integer,
	rate_30_min integer,
	rate_45_min integer,
	rate_1_hour integer,
	rate_1_50_hour integer,
	rate_2_hour integer,
	rate_2_50_hour integer,
	rate_3_hour integer,
	rate_3_50_hour integer,
	rate_4_hour integer,
	rate_overnight integer,
	region varchar(200),
	primary key (oid)
);

DELIMITER //
CREATE PROCEDURE CheckMaxDateAndAct()
BEGIN
    -- Perform your action here
    insert into tbl_client_history (client_Oid, username, location, nationality, age, rating, rate_15_min, rate_30_min, rate_45_min, rate_1_hour,
    rate_1_50_hour, rate_2_hour, rate_2_50_hour, rate_3_hour, rate_3_50_hour, rate_4_hour, rate_overnight, telephone,
    url_page, refresh_time, user_id, image_available, region, gender, member_since, height, dress_size, hair_colour, eye_colour, verified,
    email, preference_list, ethnicity, record_source)
    (select oid, username, location, nationality, age, rating,
   		rate_15_min, rate_30_min, rate_45_min, rate_1_hour,
    	rate_1_50_hour, rate_2_hour, rate_2_50_hour, rate_3_hour, rate_3_50_hour, rate_4_hour, rate_overnight, telephone,
    	url_page, refresh_time, user_id, image_available, region, gender, member_since, height, dress_size, hair_colour, eye_colour, verified,
    	email, preference_list, ethnicity, record_source from tbl_client_temp);
end; //




-- TODO: 2026-01-05 investigate why this is taking longer. Do we need this still?
delimiter //
create procedure prc_clean_up_time_series_data()
begin
	DECLARE user_id_result VARCHAR(20);
	declare max_load_time datetime;
	declare finished int default 0;
DECLARE user_id_cursor CURSOR FOR select user_id, max(load_datetime) from tbl_time_series_data where refresh_date = date(now()) group by user_id having count(user_id) > 1;
declare continue handler for not found set finished=1;
	OPEN user_id_cursor;
	get_flagged: LOOP
		FETCH user_id_cursor INTO user_id_result, max_load_time;
			if finished then
			leave get_flagged;
		end if;
		delete from tbl_time_series_data where user_id = user_id_result and refresh_date = date(now()) and load_datetime < max_load_time;
	END LOOP get_flagged;
	close user_id_cursor;
end;//

create index idx_user_id_max_load_time on tbl_time_series_data(user_id, load_datetime);

-- ----------------------------------


drop procedure if exists prc_clean_up_data;

create procedure prc_clean_up_data ()
begin
	set @duplicates_deleted = 0;
	set @new_records = 0;
	set @delete_temp_records = 0;
	set @updated_records = 0;
	set @tracked_changes = 0;
	set @price_time_series_records = 0;

	-- remove duplicates in the temp table

	delete from tbl_client_temp where oid in (select oid from tbl_client_temp group by user_id having count(user_id) > 1 order by user_id);
	set @duplicates_deleted = ROW_COUNT();


	call prc_run_temp_staging_data();

	-- update the records that exist in the tbl_client table

update tbl_client d2 inner join (select * from tbl_client_temp where user_id in (select user_id from tbl_client)) d1
on d2.user_id = d1.user_id
set d2.username = d1.username,
	d2.nationality = d1.nationality,
	d2.location = d1.location,
	d2.rating = d1.rating,
	d2.age = d1.age,
	d2.rate_15_min = d1.rate_15_min,
	d2.rate_30_min = d1.rate_30_min,
	d2.rate_45_min = d1_rate_45_min,
	d2.rate_1_hour = d1.rate_1_hour,
	d2.rate_1_50_hour = d1.rate_1_50_hour,
	d2.rate_2_hour = d1.rate_2_hour,
	d2.rate_2_50_hour = d1.rate_2_50_hour,
	d2.rate_3_hour = d1.rate_3_hour,
	d2.rate_3_50_hour = d1.rate_3_50_hour,
	d2.rate_4_hour = d1.rate_4_hour,
	d2.rate_overnight = d1.rate_overnight,
	d2.telephone = d1.telephone,
	d2.url_page = d1.url_page,
	d2.refresh_time = d1.refresh_time,
	d2.region = d1.region,
	d2.gender = d1.gender,
	d2.member_since = d1.member_since,
	d2.height = d1.height,
	d2.dress_size = d1.dress_size,
	d2.hair_colour = d1.hair_colour,
	d2.eye_colour = d1.eye_colour,
    d2.verified = d1.verified,
    d2.email = d1.email,
    d2.preference_list = d1.preference_list,
    d2.ethnicity = d1.ethnicity,
    d2.record_source = d1.record_source;
	set @updated_records = ROW_COUNT();

	-- Create time series pricing data records for all clients

	insert into tbl_time_series_data (user_id, day_of_week, refresh_date, load_datetime, region, rate_15_min, rate_30_min, rate_45_min, rate_1_hour, rate_1_50_hour,
	rate_2_hour, rate_2_50_hour, rate_3_hour, rate_3_50_hour, rate_4_hour, rate_overnight)
	(select user_id, UPPER(dayname(refresh_time)), date(refresh_time), now(), region, rate_15_min, rate_30_min, rate_45_min, rate_1_hour, rate_1_50_hour,
	rate_2_hour, rate_2_50_hour, rate_3_hour, rate_3_50_hour, rate_4_hour, rate_overnight from tbl_client_temp);

	set @price_time_series_records = ROW_COUNT();

	-- run procedure to clean up duplicates (as a result of multiple loads if any)

	call prc_clean_up_time_series_data();

	-- insert new records from temp to master table

	insert into tbl_client (username, nationality, location, rating, age, rate_15_min, rate_30_min, rate_45_min, rate_1_hour, rate_1_50_hour, rate_2_hour, rate_2_50_hour, rate_3_hour, rate_3_50_hour, rate_4_hour, rate_overnight, telephone, url_page, refresh_time, user_id, image_available, region, gender, member_since, height, dress_size, hair_colour, eye_colour, verified, email, preference_list, ethnicity, record_source)
			(select username, nationality, location, rating, age, rate_15_min, rate_30_min, rate_45_min, rate_1_hour, rate_1_50_hour, rate_2_hour, rate_2_50_hour, rate_3_hour, rate_3_50_hour, rate_4_hour, rate_overnight, telephone, url_page, refresh_time, user_id, image_available, region, gender, member_since, height, dress_size, hair_colour, eye_colour, verified, email, preference_list, ethnicity, record_source from tbl_client_temp where user_id not in (select user_id from tbl_client));
	set @new_records = ROW_COUNT();

	-- flush all records in the temp table

	delete from tbl_client_temp;
	set @delete_temp_records = ROW_COUNT();

insert into tbl_client_changes_tracker (user_id, field_value, old_value, new_value, record_datetime)
	(select c.user_Id as user_id, "telephone" as fieldname, t.telephone as old_value, c.telephone as new_value, c.refresh_time as act_time from (select * from tbl_changes_tracker_temp where user_id in (select user_id from tbl_client)) t
	inner join tbl_client c on t.user_id = c.user_id where c.telephone <> t.telephone)
	union
	(select c.user_Id as user_id, "location" as fieldname, t.location as old_value, c.location as new_value, c.refresh_time as act_time from (select * from tbl_changes_tracker_temp where user_id in (select user_id from tbl_client)) t
	inner join tbl_client c on t.user_id = c.user_Id where c.location <> t.location)
	union
	(select c.user_Id as user_id, "username" as fieldname, t.username as old_value, c.username as new_value, c.refresh_time as act_time from (select * from tbl_changes_tracker_temp where user_id in (select user_id from tbl_client)) t
	inner join tbl_client c on t.user_id = c.user_id where c.username <> t.username)
	union
	(select c.user_Id as user_id, "rating" as fieldname, t.rating as old_value, c.rating as new_value, c.refresh_time as act_time from (select * from tbl_changes_tracker_temp where user_id in (select user_id from tbl_client)) t
	inner join tbl_client c on t.user_id = c.user_id where c.rating <> t.rating)
	union
	(select c.user_Id as user_id, "nationality" as fieldname, t.nationality as old_value, c.nationality as new_value, c.refresh_time as act_time from (select * from tbl_changes_tracker_temp where user_id in (select user_id from tbl_client)) t
	inner join tbl_client c on t.user_id = c.user_id where c.nationality <> t.nationality);


delete from tbl_changes_tracker_temp;


	select @duplicates_deleted, @new_records, @delete_temp_records, @updated_records, @tracked_changes, @price_time_series_records;
end;//



create procedure prc_run_temp_staging_data()
begin
delete from tbl_changes_tracker_temp;

insert into tbl_changes_tracker_temp (user_id, telephone, location, username, rating, nationality)
(select c.user_id, c.telephone, c.location, c.username, c.rating, c.nationality from tbl_client c inner join tbl_client_temp t on c.user_id = t.user_id);
end;



create table if not exists tbl_data_load_log (
	oid integer not null auto_increment,
	job_name text,
	stream text,
	provider text,
	parameters text,
	load_datetime datetime,
	status text,
	message text,
	primary key (oid)
);


create table if not exists tbl_client_image (
	oid integer not null auto_increment,
	client_oid integer,
	image_path text,
	primary key (oid),
	foreign key (client_oid) references tbl_client (oid)
);


create table if not exists tbl_client_watch_list (
	oid integer not null auto_increment,
	client_oid integer not null,
	primary key (oid),
	foreign key (client_oid) references tbl_client(oid)
);


create procedure proc_delete_duplicates()
begin
	delete from tbl_client where oid in (select oid from tbl_client group by user_id having count(user_id) > 1 order by user_id);
end;


create view vw_display_duplicates as
select user_id, oid, count(user_id) from tbl_client group by user_id having count(user_id) > 1 order by user_id;


create or replace view vw_find_updated_client_entries as select * from tbl_client where refresh_time > CURRENT_DATE();

create or replace view vw_view_client_changes as
select c.oid, c.username, d.user_id, d.field_value, d.old_value, d.new_value, d.record_datetime
from tbl_client c inner join tbl_client_changes_tracker d on c.user_id = d.user_id;


create table if not exists tbl_appointment_log (
	oid integer not null,
	client_oid integer,
	appointment_date_time datetime,
	location text,
	region text,
	duration text,
	price decimal,
	notes text,
	primary key (oid),
	foreign key (client_oid) references tbl_client(oid)
);


create procedure prc_get_total_metrics()
begin
	select (select count(oid) from tbl_client) as total_clients,
			(select count(oid) from tbl_client_watch_list) as total_watch_list,
			(select count(oid) from tbl_client_changes_tracker where date(record_datetime) = date(now())) as new_tracked_changes,
			-- get the total number of clients available (system wide)
			(select count(oid) from tbl_client where date(refresh_time) = date(now())) as total_available_clients,

			-- get the total number of clients that are not available today (system wide)
			(select count(oid) from tbl_client where date(refresh_time) < date(now())) as total_not_available_clients,

			-- new client count
			(select count(oid) from tbl_new_clients where date(load_datetime) = date(now())) as total_new_clients,

			-- get the total number of clients in the watch list
			(select count(oid) from tbl_client_watch_list) as total_client_watchlist,

			-- get the total number of approved appointments bookings
			(select count(oid) from tbl_appointment_log) as total_appointment_bookings,

			-- get the number of stale client records

			(select count(oid) client_oid from tbl_client where (month(now()) - month(refresh_time)) >= 4) as total_stale_client_records;


end;

create table tbl_client_availability_tracker (
	oid integer not null auto_increment,
	client_oid integer,
	date_of_availability date,
	frequency integer,
	primary key (oid)
);


create index idx_client_oid_date_of_availability on tbl_client_availability_tracker (client_oid, date_of_availability);


create index idx_client_availability_tracker_client_oid on tbl_client_availability_tracker(client_oid);
create index idx_client_availability_tracker_date on tbl_client_availability_tracker(date_of_availability);


create procedure prc_update_client_availabiliy_tracker()
begin

-- delete any existing tracker statements for today, as these will be added again with new records for the next load
delete from tbl_client_availability_tracker where date_of_availability = date(now());


insert into tbl_client_availability_tracker(client_oid, date_of_availability, frequency)
(select c.oid, date(now()), 1 from tbl_client c where date(refresh_time) = date(now()));

end;

-- 08-12-21 - As of this date, AW has now masked telephone numbers on client pages, unless you register and log into their site.
-- this table is a way to backup the phone numbers before they are overwritten with the **** masking value on each load.

create table if not exists tbl_client_contact_list (
oid integer auto_increment,
client_oid integer,
user_id text,
username text,
telephone text,
nationality text,
age integer,
url_page text,
telephone_as_of date,
details_last_saved_at date,
primary key (oid)
);


create procedure prc_new_clean_up_data ()
begin
	set sql_mode = '';
	set @duplicates_deleted = 0;
	set @new_records = 0;
	set @delete_temp_records = 0;
	set @updated_records = 0;
	set @tracked_changes = 0;
	set @price_time_series_records = 0;

	-- remove duplicates in the temp table

	create temporary table temp_table1 (oid int);
	insert into temp_table1 (oid) (select oid from tbl_client_temp group by user_id having count(user_id) > 1 order by user_id);
	delete from tbl_client_temp where oid in (select oid from temp_table1);
	set @duplicates_deleted = ROW_COUNT();
	drop temporary table temp_table1;

	call prc_run_temp_staging_data();

	-- update the records that exist in the tbl_client table

	create temporary table temp_table2 (oid integer,
		username text,
		nationality text,
		location text,
		rating integer,
		age integer,
		rate_15_min integer,
		rate_30_min integer,
		rate_45_min integer,
		rate_1_hour integer,
		rate_1_50_hour integer,
		rate_2_hour integer,
		rate_2_50_hour integer,
		rate_3_hour integer,
		rate_3_50_hour integer,
		rate_4_hour integer,
		rate_overnight integer,
		telephone text,
		url_page text,
		refresh_time datetime,
		user_id varchar(20),
		image_available boolean,
		region text,
		gender text,
		member_since date,
		height decimal(3,2),
		dress_size integer,
		hair_colour text,
		eye_colour text,
		verified boolean,
	    email varchar(255),
	    preference_list text,
	    ethnicity varchar(20),
	    record_source varchar(15)
		);

	insert into temp_table2 (select * from tbl_client_temp where user_id in (select user_id from tbl_client));

	update tbl_client d2 inner join (select * from temp_table2) d1
		on d2.user_id = d1.user_id
		set d2.username = d1.username,
			d2.nationality = d1.nationality,
			d2.location = d1.location,
			d2.rating = d1.rating,
			d2.age = d1.age,
			d2.rate_15_min = d1.rate_15_min,
			d2.rate_30_min = d1.rate_30_min,
			d2.rate_45_min = d1.rate_45_min,
			d2.rate_1_hour = d1.rate_1_hour,
			d2.rate_1_50_hour = d1.rate_1_50_hour,
			d2.rate_2_hour = d1.rate_2_hour,
			d2.rate_2_50_hour = d1.rate_2_50_hour,
			d2.rate_3_hour = d1.rate_3_hour,
			d2.rate_3_50_hour = d1.rate_3_50_hour,
			d2.rate_4_hour = d1.rate_4_hour,
			d2.rate_overnight = d1.rate_overnight,
			d2.telephone = d1.telephone,
			d2.url_page = d1.url_page,
			d2.refresh_time = d1.refresh_time,
			d2.region = d1.region,
			d2.gender = d1.gender,
			d2.member_since = d1.member_since,
			d2.height = d1.height,
			d2.dress_size = d1.dress_size,
			d2.hair_colour = d1.hair_colour,
			d2.eye_colour = d1.eye_colour,
			d2.verified = d1.verified,
			d2.email = d1.email,
			d2.preference_list = d1.preference_list,
			d2.ethnicity = d1.ethnicity,
			d2.record_source = d1.record_source;

			set @updated_records = ROW_COUNT();

		drop temporary table temp_table2;

	-- Create time series pricing data records for all clients

	insert into tbl_time_series_data (user_id, day_of_week, refresh_date, load_datetime, region, rate_15_min, rate_30_min, rate_45_min, rate_1_hour, rate_1_50_hour,
	rate_2_hour, rate_2_50_hour, rate_3_hour, rate_3_50_hour, rate_4_hour, rate_overnight)
	(select user_id, UPPER(dayname(refresh_time)), date(refresh_time), now(), region, rate_15_min, rate_30_min, rate_45_min, rate_1_hour, rate_1_50_hour,
	rate_2_hour, rate_2_50_hour, rate_3_hour, rate_3_50_hour, rate_4_hour, rate_overnight from tbl_client_temp);

	set @price_time_series_records = ROW_COUNT();

	-- run procedure to clean up duplicates (as a result of multiple loads if any)
	-- TODO: 2026-01-05 - Remove the prc_clean_up_time_series_data(). Needs to be analysed as it is consuming load time. Do we need this?
	-- call prc_clean_up_time_series_data();

	-- insert new records from temp to master table

	insert into tbl_client (username, nationality, location, rating, age, rate_15_min, rate_30_min, rate_45_min, rate_1_hour, rate_1_50_hour, rate_2_hour, rate_2_50_hour, rate_3_hour, rate_3_50_hour, rate_4_hour, rate_overnight, telephone, url_page, refresh_time, user_id, image_available, region, gender, member_since, height, dress_size, hair_colour, eye_colour, verified, email, preference_list, ethnicity, record_source)
			(select username, nationality, location, rating, age, rate_15_min, rate_30_min, rate_45_min, rate_1_hour, rate_1_50_hour, rate_2_hour, rate_2_50_hour, rate_3_hour, rate_3_50_hour, rate_4_hour, rate_overnight, telephone, url_page, refresh_time, user_id, image_available, region, gender, member_since, height, dress_size, hair_colour, eye_colour, verified, email, preference_list, ethnicity, record_source from tbl_client_temp where user_id not in (select user_id from tbl_client));
	set @new_records = ROW_COUNT();


    -- add records from the temp table into the tbl_client_history table for audit purposes.
    call CheckMaxDateAndAct();

	-- flush all records in the temp table
	delete from tbl_client_temp;
	set @delete_temp_records = ROW_COUNT();

insert into tbl_client_changes_tracker (user_id, field_value, old_value, new_value, record_datetime)
	(select c.user_Id as user_id, "telephone" as fieldname, t.telephone as old_value, c.telephone as new_value, c.refresh_time as act_time from (select * from tbl_changes_tracker_temp where user_id in (select user_id from tbl_client)) t
	inner join tbl_client c on t.user_id = c.user_id where c.telephone <> t.telephone)
	union
	(select c.user_Id as user_id, "location" as fieldname, t.location as old_value, c.location as new_value, c.refresh_time as act_time from (select * from tbl_changes_tracker_temp where user_id in (select user_id from tbl_client)) t
	inner join tbl_client c on t.user_id = c.user_Id where c.location <> t.location)
	union
	(select c.user_Id as user_id, "username" as fieldname, t.username as old_value, c.username as new_value, c.refresh_time as act_time from (select * from tbl_changes_tracker_temp where user_id in (select user_id from tbl_client)) t
	inner join tbl_client c on t.user_id = c.user_id where c.username <> t.username)
	union
	(select c.user_Id as user_id, "rating" as fieldname, t.rating as old_value, c.rating as new_value, c.refresh_time as act_time from (select * from tbl_changes_tracker_temp where user_id in (select user_id from tbl_client)) t
	inner join tbl_client c on t.user_id = c.user_id where c.rating <> t.rating)
	union
	(select c.user_Id as user_id, "nationality" as fieldname, t.nationality as old_value, c.nationality as new_value, c.refresh_time as act_time from (select * from tbl_changes_tracker_temp where user_id in (select user_id from tbl_client)) t
	inner join tbl_client c on t.user_id = c.user_id where c.nationality <> t.nationality);


delete from tbl_changes_tracker_temp;


	select @duplicates_deleted, @new_records, @delete_temp_records, @updated_records, @tracked_changes, @price_time_series_records;
end;


-- these alter statements must be added to handle the encoding of awkward strings in the location field

ALTER TABLE db_phoenix.tbl_client_temp CONVERT TO CHARACTER SET utf8mb4;
ALTER TABLE db_phoenix.tbl_client CONVERT TO CHARACTER SET utf8mb4;

ALTER TABLE db_phoenix.tbl_client_temp MODIFY COLUMN location text CHARACTER SET utf8mb4;
ALTER TABLE db_phoenix.tbl_client MODIFY COLUMN location text CHARACTER SET utf8mb4;



-- =======================================================================

-- analytics tables

create table if not exists tbl_client_analytics (
	oid bigint not null auto_increment,
	user_id varchar(20),
	member_since date,
	first_observed datetime,
	last_observed datetime,
	minimum_charge integer,
	maximum_charge integer,
	spread integer,
	number_of_days_collecting_data bigint,
	number_of_days_in_service bigint,
	percentage_available decimal,
	total_regions_travelled integer,
	previously_serviced_bb bool,
	record_time datetime,
	primary key (oid)
);

create index idx_client_analytics_user_id on tbl_client_analytics(user_id);