class AnalyticsQueries:

    def __init__(self):
        pass

    @staticmethod
    def get_first_and_last_load_dates_query():
        return "select min(date(refresh_time)), max(date(refresh_time)) from tbl_client"

    @staticmethod
    def flush_analytics_data(user_id_list):
        return f"""
            delete from tbl_client_analytics where user_id in ({user_id_list})
        """

    @staticmethod
    def create_links_from_feedback_reports(user_id):
        return f"""
		insert ignore into tbl_link (user_id_1, user_id_2, input_type, source, relationship, notes) 
	    select user_id, by_user_id, "AUTOMATED" as input_type, "ANALYTICS_BUILT_FROM_FEEDBACK" as source, "SP_TO_P" as relationship, concat("SP -> P | ", username, "->", by_username) as notes
	  	from tbl_feedback_v2 
	    where user_id = '{user_id}' 
	    and rating_type = 'Booking' 
	    group by username, by_username, by_user_id
		union
	    select by_user_id,user_id, "AUTOMATED" as input_type, "ANALYTICS_BUILT_FROM_FEEDBACK" as source, "P_TO_SP" as relationship, concat("P -> SP | ", by_username, "->", username) as notes
	  	from tbl_feedback_v2 
	    where user_id = '{user_id}' 
	    and rating_type = 'Booking' 
	    group by username, by_username, by_user_id;			
        """

    @staticmethod
    def get_attendance_statistics_query(first_load_date_minus_one_day, latest_load_date, user_id_list):
        return f"""
        insert into tbl_client_analytics(user_id, member_since, first_observed, last_observed, minimum_charge, maximum_charge, spread,
	    number_of_days_collecting_data, number_of_days_in_service, percentage_available, total_regions_travelled, previously_serviced_bb,record_time)
        select user_id, member_since, MIN(refresh_time) as first_observed, MAX(refresh_time) as last_observed, 
		min(rate_1_hour) as minimum_charge, 
		max(rate_1_hour) as maximum_charge,
	    (max(rate_1_hour) - min(rate_1_hour)) as spread,
	    case when member_since > '{first_load_date_minus_one_day}' 
	    then (DATEDIFF('{latest_load_date}', member_since)) 
	    else (DATEDIFF('{latest_load_date}', '{first_load_date_minus_one_day}')) 
	    end as number_of_days_collecting_data,
	    COUNT(refresh_time) as number_of_days_in_service,
		case when member_since > '{first_load_date_minus_one_day}' 
	    then (COUNT(refresh_time) / DATEDIFF('{latest_load_date}', member_since) * 100) 
	    else (COUNT(refresh_time) / DATEDIFF('{latest_load_date}', '2022-01-21') * 100) 
	    end as percentage_available,
		count(distinct(region)) as total_regions_travelled,
		(SUM(CASE WHEN preference_list LIKE '%' THEN 1 ELSE 0 END) > 0) AS previously_serviced_bb,
		now()
		from tbl_client_history where user_id in ({user_id_list})
	    group by user_id, member_since order by percentage_available desc;
        """
