import mysql
from mysql.connector import DataError
from _mysql_connector import MySQLInterfaceError
from controller.DataIngestionImpl import DataIngestionImpl


class ServiceReportsV2DataIngestionImpl(DataIngestionImpl):

    def __init__(self, property_manager):
        super().__init__(property_manager)

    def parse_int_value(self, value):
        if value == "None":
            return None
        if value == "":
            return None
        return int(value)
    def populate_staging_data(self, csv_row: list, feed_file: str):

        mydb = mysql.connector.connect(
            host=self.property_manager.get_datasource_url(),
            user=self.property_manager.get_datasource_username(),
            password=self.property_manager.get_datasource_password(),
            database=self.property_manager.get_datasource_name()
        )

        mysqlcursor = mydb.cursor()

        # insert data into staging table
        insert_client_row_statement = (
            "INSERT IGNORE INTO tbl_service_report_v2 (id, user_id, username, candidate_description, candidate_score,"
            "by_username,"
            "comments, comments_score, create_date,"
            "exclude_affiliate, price,"
            "location, meet_date, meet_duration, on_call, personality, personality_score, rating_total, recommend,"
            "rejected, report_rating, score, services, services_score, venue_description, venue_score, visit_again) "
            "VALUES "
            "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        for row in csv_row:
            try:
                value_client_row_ = (row.__getitem__(0),  # id
                                     row.__getitem__(1),  # user_id
                                     row.__getitem__(2),  # username
                                     row.__getitem__(3),  # candidate_description
                                     self.parse_int_value(row.__getitem__(4)),  # candidate_score
                                     row.__getitem__(5),  # by_username,
                                     row.__getitem__(6),  # comments,
                                     row.__getitem__(7),  # comments_score,
                                     row.__getitem__(8),  # create_date,
                                     bool(row.__getitem__(9)),  # exclude_affiliate,
                                     row.__getitem__(10),  # price,
                                     row.__getitem__(11),  # location,
                                     row.__getitem__(12),  # meet_date,
                                     row.__getitem__(13),  # meet_duration,
                                     bool(row.__getitem__(14)),  # on_call,
                                     row.__getitem__(15),  # personality,
                                     self.parse_int_value(row.__getitem__(16)),  # personality_score,
                                     self.parse_int_value(row.__getitem__(17)),  # rating_total,
                                     bool(row.__getitem__(18)),  # recommended,
                                     bool(row.__getitem__(19)),  # rejected,
                                     row.__getitem__(20).upper(),  # report_rating,
                                     self.parse_int_value(row.__getitem__(23)),  # score,
                                     row.__getitem__(24),  # services,
                                     self.parse_int_value(row.__getitem__(25)),  # score_services,
                                     row.__getitem__(26),  # venue_description,
                                     self.parse_int_value(row.__getitem__(27)),  # venue_score,
                                     bool(row.__getitem__(28))  # visit_again,
                                     )
                mysqlcursor.execute(insert_client_row_statement, value_client_row_)
            except (ValueError, IndexError, DataError, MySQLInterfaceError) as e:
                print("[WARN] Unable to correctly parse row in file[" + feed_file + "] row=[" + str(
                    row) + "] exception = " + str(e))

        mydb.commit()
        mysqlcursor.close()
