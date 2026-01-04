import re
from datetime import datetime

import mysql
from mysql.connector import DataError
from _mysql_connector import MySQLInterfaceError

from controller.DataIngestionImpl import DataIngestionImpl


class FeedbackV2DataIngestionImpl(DataIngestionImpl):

    def __init__(self, property_manager):
        super().__init__(property_manager)

    @staticmethod
    def parse_rating(is_positive, is_negative, is_neutral):
        if is_positive == "True":
            return "POSITIVE"
        elif is_negative == "True":
            return "NEGATIVE"
        elif is_neutral == "True":
            return "NEUTRAL"
        else:
            return "UNKNOWN"

    @staticmethod
    def extract_user_id_from_file_name(filename):
        pattern = re.compile("feeds-feedbackv2-phoenix_(\\d+)_(.*)\\.txt$")
        result = re.search(pattern, filename)
        if result:
            return result.group(1)
        return None

    @staticmethod
    def parse_date(value):
        try:
            return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")


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
            "INSERT IGNORE INTO tbl_feedback_v2 (id, user_id, username, by_user_id, by_username,"
            "by_user_total_rating,"
            "rating_date, rating, disputed,"
            "feedback, feedback_response,"
            "rating_type, user_type, user_active) "
            "VALUES "
            "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        for row in csv_row:
            try:
                value_client_row_ = (row.__getitem__(0),  # id
                                     FeedbackV2DataIngestionImpl.extract_user_id_from_file_name(feed_file),  # user_id
                                     row.__getitem__(1),  # username
                                     row.__getitem__(2),  # by_user_id
                                     row.__getitem__(3),  # by_username
                                     row.__getitem__(4),  # by_user_total_rating
                                     FeedbackV2DataIngestionImpl.parse_date(row.__getitem__(5)),  # rating_date,
                                     FeedbackV2DataIngestionImpl.parse_rating(row.__getitem__(6),
                                                                              row.__getitem__(7),
                                                                              row.__getitem__(8)),
                                     bool(row.__getitem__(9)),  # disputed,
                                     row.__getitem__(10),  # feedback,
                                     row.__getitem__(11),  # feedback_response,
                                     row.__getitem__(12),  # rating_type,
                                     row.__getitem__(13),  # user_type,
                                     "true" if row.__getitem__(14) == "True" else "False"  # user_active,
                                     )
                mysqlcursor.execute(insert_client_row_statement, value_client_row_)
            except (ValueError, IndexError, DataError, MySQLInterfaceError) as e:
                print("[WARN] Unable to correctly parse row in file[" + feed_file + "] row=[" + str(
                    row) + "] exception = " + str(e))

        mydb.commit()
        mysqlcursor.close()

