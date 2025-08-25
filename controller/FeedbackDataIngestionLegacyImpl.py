from datetime import datetime

import re
import mysql.connector

from controller.DataIngestionImpl import DataIngestionImpl
from mysql.connector import DataError
from _mysql_connector import MySQLInterfaceError


class FeedbackDataIngestionLegacyImpl(DataIngestionImpl):

    def __init__(self, property_manager):
        super().__init__(property_manager)

    def extract_user_id_from_file_name(self, filename):
        pattern = re.compile("feeds-feedback-phoenix_(\\d+)_(.*)\\.txt$")
        result = re.search(pattern, filename)
        if result:
            return result.group(1)
        return None

    def populate_staging_data(self, csv_row: list, feed_file: str):

        # extract the user id from the filename (temporary solution for this type of feed format)
        user_id = self.extract_user_id_from_file_name(feed_file)

        mydb = mysql.connector.connect(
            host=self.property_manager.get_datasource_url(),
            user=self.property_manager.get_datasource_username(),
            password=self.property_manager.get_datasource_password(),
            database=self.property_manager.get_datasource_name()
        )

        mysqlcursor = mydb.cursor()

        # insert data into staging table
        insert_client_row_statement = ("INSERT IGNORE INTO tbl_feedback (unique_identifier, service_provider, user_id, "
                                       "ukp_user_id,"
                                       "rating, comment, feedback_date)"
                                       "VALUES "
                                       "(%s, %s, %s, %s, %s, %s, %s)")
        for row in csv_row:
            try:
                value_client_row_ = (row.__getitem__(0),  # unique_identifier
                                     "AW",  # service_provider
                                     user_id,  # user_id
                                     None,
                                     "FEEDBACK_ONLY" if row.__getitem__(1) == "FeedbackOnly" else row.__getitem__(1).upper(),  # rating
                                     row.__getitem__(6),  # comment
                                     datetime.strptime(row.__getitem__(4), '%d/%m/%Y %H:%M').strftime('%Y-%m-%d'))
                mysqlcursor.execute(insert_client_row_statement, value_client_row_)
            except (ValueError, IndexError, DataError, MySQLInterfaceError) as e:
                print("[WARN] Unable to correctly parse row in file[" + feed_file + "] row=[" + str(
                    row) + "] exception = " + str(e))

        mydb.commit()
        mysqlcursor.close()
