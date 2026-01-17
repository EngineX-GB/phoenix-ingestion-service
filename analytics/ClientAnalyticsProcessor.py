import mysql
from mysql.connector import DataError

from analytics.AnalyticsQueries import AnalyticsQueries
from analytics.AnalyticsUtil import AnalyticsUtil


class ClientAnalyticsProcessor:

    def __init__(self, property_manager):
        self.property_manager = property_manager
        pass

    def get_load_dates(self, cursor):
        query = AnalyticsQueries.get_first_and_last_load_dates_query()
        cursor.execute(query)
        return cursor.fetchone()
    def load_availability_statistics(self, first_load_date, latest_load_date, user_id_list, cursor):
        deleteQuery = AnalyticsQueries.flush_analytics_data(user_id_list)
        cursor.execute(deleteQuery)
        print("[INFO] Flushed Analytics Data. Number of rows impacted : " + str(cursor.rowcount))
        query = AnalyticsQueries.get_attendance_statistics_query(first_load_date, latest_load_date, user_id_list)
        cursor.execute(query)
        print("[INFO] Filled Analytics Data. Number of rows inserted : " + str(cursor.rowcount))
        return   # it's inserting data. Not returning anything






    def check_red_flag_data(self):
        mydb = mysql.connector.connect(
            host=self.property_manager.get_datasource_url(),
            user=self.property_manager.get_datasource_username(),
            password=self.property_manager.get_datasource_password(),
            database=self.property_manager.get_datasource_name()
        )

        user_id_list = []
        mysql_cursor = mydb.cursor()
        query = "SELECT user_id FROM tbl_client WHERE date(refresh_time) = '2025-12-16'"
        mysql_cursor.execute(query)
        result_set = mysql_cursor.fetchall()
        for (user_id,) in result_set:
            user_id_list.append(user_id)

        row = self.get_load_dates(mysql_cursor)
        if row:
            first_load_date = row[0]
            last_load_date = row[1]
            self.load_availability_statistics(first_load_date, last_load_date,
                                              AnalyticsUtil.join_strings_from_list(user_id_list), mysql_cursor)
        else:
            raise Exception("An error occurred when trying to get load dates for preparing analytics.")

        mysql_cursor.close()
        mydb.close()
