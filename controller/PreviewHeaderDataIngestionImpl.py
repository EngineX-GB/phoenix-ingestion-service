import mysql.connector

from controller.DataIngestionImpl import DataIngestionImpl
from mysql.connector import DataError
from _mysql_connector import MySQLInterfaceError


class PreviewHeaderDataIngestionImpl(DataIngestionImpl):
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

        insert_row_statement = ("INSERT IGNORE INTO tbl_preview_header (user_id, title, details, "
                                "start_date,"
                                "end_date, is_active, business_key)"
                                "VALUES "
                                "(%s, %s, %s, %s, %s, %s, %s)")
        for row in csv_row:
            try:
                value_row = (row.__getitem__(0),  # user_id
                             row.__getitem__(1),  # title
                             row.__getitem__(2),  # details
                             row.__getitem__(3),  # start_date
                             row.__getitem__(4),  # end_date
                             bool(row.__getitem__(5)),  # is_active
                             row.__getitem__(6)  # business_key
                             )
                mysqlcursor.execute(insert_row_statement, value_row)
            except (ValueError, IndexError, DataError, MySQLInterfaceError) as e:
                print("[WARN] Unable to correctly parse row in file[" + feed_file + "] row=[" + str(
                    row) + "] exception = " + str(e))

        mydb.commit()
        mysqlcursor.close()
