from IDataAccess import IDataAccess
import csv
import mysql.connector


class DataAccessImpl(IDataAccess):

    def __init__(self):
        pass

    @staticmethod
    def get_csv_rows(filename):
        datarows = []
        with open(filename) as file:
            rows = csv.reader(file, delimiter='|')
            for row in rows:
                datarows.append(row)
        print("[INFO] Returning " + str(len(datarows)) + " records from " + filename)
        return datarows

    @staticmethod
    def parse_not_specified_value(value: str):
        if value == "Not Specified":
            return 0
        return value


    # load the feed file into the staging table and
    # call out the stored proc to formalise the data
    # in the main table

    def load_feed_data(self, feed_files: list):
        for feed_file in feed_files:
            print("[INFO] Loading data file : " + feed_file)
            csv_read_rows = DataAccessImpl.get_csv_rows(feed_file)
            self.populate_staging_data(csv_read_rows)


    def populate_staging_data(self, csv_row: list):
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",
            database="db_phoenix"
        )

        print("[INFO] Connected to data source : mysql")

        mysqlcursor = mydb.cursor()

        # insert data into staging table
        insert_client_row_statement = ("INSERT INTO tbl_client_temp ("
                                       "username, nationality, location, rating, age, rate_15_min, rate_30_min, rate_1_hour, "
                                       "rate_1_50_hour, rate_2_hour, rate_2_50_hour, rate_3_hour, rate_3_50_hour, rate_4_hour, "
                                       "rate_overnight, telephone, url_page, user_id, region, hair_colour, eye_colour"
                                       ") "
                                       "VALUES "
                                       "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        for row in csv_row:
            value_client_row_ = (row.__getitem__(0),  # username
                                 row.__getitem__(1),  # nationality
                                 row.__getitem__(2),  # location
                                 int(row.__getitem__(3)),  # rating
                                 int(DataAccessImpl.parse_not_specified_value(row.__getitem__(4))),  # age
                                 int(row.__getitem__(5)),  # 15
                                 int(row.__getitem__(6)),  # 30
                                 int(row.__getitem__(8)),  # 1
                                 int(row.__getitem__(9)),  # 1.5
                                 int(row.__getitem__(10)),  # 2
                                 int(row.__getitem__(11)),  # 2.5
                                 int(row.__getitem__(12)),  # 3
                                 int(row.__getitem__(13)),  # 3.5
                                 int(row.__getitem__(14)),  # 4
                                 int(row.__getitem__(15)),  # ov
                                 row.__getitem__(16),  # tel
                                 row.__getitem__(17),  # url
                                 row.__getitem__(19),  # userid
                                 row.__getitem__(21),  # region
                                 row.__getitem__(26),  # haircol
                                 row.__getitem__(27)  # eyecol
                                 )
            mysqlcursor.execute(insert_client_row_statement, value_client_row_)

        mydb.commit()
        # after the loads on the temp table, run the store proc to put it in the main table
        print("[INFO] Running store proc to move data from staging to production table")
        mysqlcursor.callproc("prc_new_clean_up_data")
        mydb.commit()
        mysqlcursor.close()
        print("[INFO] Disconnected from data source : mysql")