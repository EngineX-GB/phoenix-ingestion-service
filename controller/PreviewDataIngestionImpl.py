import mysql.connector

from controller.DataIngestionImpl import DataIngestionImpl
from mysql.connector import DataError
from _mysql_connector import MySQLInterfaceError


class PreviewDataIngestionImpl(DataIngestionImpl):
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

        insert_row_statement = ("INSERT IGNORE INTO tbl_preview_data (user_id, country, region, county,"
                                "town, postcode, details, availability_date, business_key)"
                                "VALUES "
                                "(%s, %s, %s, %s, %s, %s, %s, %s, %s)")
        for row in csv_row:
            try:
                value_row = (row.__getitem__(0),  # user_id
                             row.__getitem__(1),  # country
                             row.__getitem__(2),  # region
                             row.__getitem__(3),  # county
                             row.__getitem__(4),  # town
                             row.__getitem__(5),  # postcode
                             row.__getitem__(6),  # details
                             row.__getitem__(7),  # availability_date
                             row.__getitem__(8)  # business_key

                )
                mysqlcursor.execute(insert_row_statement, value_row)
            except (ValueError, IndexError, DataError, MySQLInterfaceError) as e:
                print("[WARN] Unable to correctly parse row in file[" + feed_file + "] row=[" + str(
                    row) + "] exception = " + str(e))

        mydb.commit()
        mysqlcursor.close()
