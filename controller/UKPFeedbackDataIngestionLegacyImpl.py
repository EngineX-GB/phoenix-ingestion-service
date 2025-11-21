from datetime import datetime

import re
import mysql.connector

from controller.DataIngestionImpl import DataIngestionImpl
from mysql.connector import DataError
from _mysql_connector import MySQLInterfaceError


class UKPFeedbackDataIngestionLegacyImpl(DataIngestionImpl):

    def __init__(self, property_manager):
        super().__init__(property_manager)

    def populate_staging_data(self, csv_row: list, feed_file: str):

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
                value_client_row_ = (row.__getitem__(0) + "-" + row.__getitem__(1),  # unique_identifier
                                     "UKP",  # service_provider
                                     row.__getitem__(2),  # user_id
                                     row.__getitem__(1),  # ukpId
                                     row.__getitem__(3).upper(),  # rating
                                     row.__getitem__(4),  # comment
                                     row.__getitem__(5)  # feedback date
                                     )
                mysqlcursor.execute(insert_client_row_statement, value_client_row_)
            except (ValueError, IndexError, DataError, MySQLInterfaceError) as e:
                print("[WARN] Unable to correctly parse row in file[" + feed_file + "] row=[" + str(
                    row) + "] exception = " + str(e))

        mydb.commit()
        mysqlcursor.close()
