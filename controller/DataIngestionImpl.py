import io
import os
from datetime import datetime

from mysql.connector import DataError
from _mysql_connector import MySQLInterfaceError

from controller.IDataIngestion import IDataIngestion
import mysql.connector
import re

from util.IngestionUtil import IngestionUtil


class DataIngestionImpl(IDataIngestion):

    def __init__(self, property_manager):
        self.property_manager = property_manager
        pass

    # load the feed file into the staging table and
    # call out the stored proc to formalise the data
    # in the main table

    def load_feed_data(self, feed_files: list):
        for feed_file in feed_files:
            print("[INFO] Loading data file : " + feed_file)
            try:
                csv_read_rows = IngestionUtil.get_csv_rows(feed_file)
                self.populate_staging_data(csv_read_rows, feed_file)
            except UnicodeDecodeError:
                print(
                    "[ERROR] A UnicodeDecodeError exception has occurred when processing the csv file [ " + feed_file + " ]")

    def load_feed_data_via_text_wrapper(self, feed_files: list[io.TextIOWrapper]):
        for feed_file in feed_files:
            try:
                csv_read_rows = IngestionUtil.get_csv_rows_via_text_wrapper(feed_file)
                self.populate_staging_data(csv_read_rows, "")   # todo
            except UnicodeDecodeError:
                print(
                    "[ERROR] A UnicodeDecodeError exception has occurred when processing the csv file in text wrapper")

    def load_feed_data_by_directory(self, directory_path: str):
        pattern = re.compile("^\\d{4}-\\d{2}-\\d{2}$")
        valid_subdirectories = []
        if not os.path.exists(directory_path):
            raise Exception("Directory " + directory_path + " does not exist")

        contains_subdirectories = IngestionUtil.check_for_subdirectories(directory_path)

        if contains_subdirectories:
            # check to see if the directory has dated subdirectories in it:
            # e.g. feeds
            #       |_> 2025-01-01
            #       |_> 2025-01-02

            file_entries = os.listdir(directory_path)
            for f in file_entries:
                subfolder_path = os.path.join(directory_path, os.path.basename(f))
                if os.path.isdir(subfolder_path):
                    # check the folder name to see if it has the date format?
                    folder_name = os.path.basename(f)
                    if re.match(pattern, folder_name):
                        valid_subdirectories.append(subfolder_path)

            # now process the feeds in these dated subdirectories
            valid_subdirectories.sort()

            for subdirectory in valid_subdirectories:
                file_entries = os.listdir(subdirectory)
                feed_file_paths = []
                for file in file_entries:
                    file_name = os.path.abspath(os.path.join(subdirectory, file))
                    feed_file_paths.append(file_name)
                # now load the feed files (for each dated subdirectory) in the data store
                feed_file_paths.sort()
                self.load_feed_data(feed_file_paths)
        else:
            # assume that feed files (txt) are already in the specified directory
            # and start to process them

            file_entries = os.listdir(directory_path)
            files = [
                os.path.abspath(os.path.join(directory_path, f))
                for f in file_entries if os.path.isfile(os.path.join(directory_path, f))]
            files.sort()
            self.load_feed_data(files)

    def populate_staging_data(self, csv_row: list, feed_file: str):
        mydb = mysql.connector.connect(
            host = self.property_manager.get_datasource_url(),
            user = self.property_manager.get_datasource_username(),
            password = self.property_manager.get_datasource_password(),
            database = self.property_manager.get_datasource_name()
        )

        # print("[INFO] Connected to data source : mysql")

        mysqlcursor = mydb.cursor()

        # insert data into staging table
        insert_client_row_statement = ("INSERT INTO tbl_client_temp ("
                                       "username, nationality, location, rating, age, rate_15_min, rate_30_min, "
                                       "rate_45_min, rate_1_hour, "
                                       "rate_1_50_hour, rate_2_hour, rate_2_50_hour, rate_3_hour, rate_3_50_hour, rate_4_hour, "
                                       "rate_overnight, telephone, url_page, refresh_time, user_id, image_available, region, gender, member_since, "
                                       "height, dress_size, hair_colour, eye_colour, verified, "
                                       "email, preference_list, record_source"
                                       ") "
                                       "VALUES "
                                       "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        for row in csv_row:
            try:
                value_client_row_ = (IngestionUtil.fix_encoded_string(row.__getitem__(0)),  # username
                                     row.__getitem__(1),  # nationality
                                     row.__getitem__(2),  # location
                                     int(row.__getitem__(3)),  # rating
                                     int(IngestionUtil.parse_not_specified_value(row.__getitem__(4))),  # age
                                     int(row.__getitem__(5)),  # 15
                                     int(row.__getitem__(6)),  # 30
                                     int(row.__getitem__(7)),  # 45
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
                                     row.__getitem__(18),  # refresh_time
                                     row.__getitem__(19),  # userid
                                     bool(row.__getitem__(20)),  # imageAvailable
                                     row.__getitem__(21),  # region
                                     IngestionUtil.convert_none(row.__getitem__(22)),  # gender
                                     datetime.strptime(row.__getitem__(23), '%d/%m/%Y').strftime('%Y-%m-%d'),
                                     # member since (needs to be formatted)
                                     IngestionUtil.convert_none(row.__getitem__(24)),  # height (needs to be converted)
                                     IngestionUtil.convert_none_by_type(row.__getitem__(25), "int"),
                                     # dress size (needs to be converted)
                                     IngestionUtil.convert_none(row.__getitem__(26)),  # haircol
                                     IngestionUtil.convert_none(row.__getitem__(27)),  # eyecol
                                     bool(row.__getitem__(28)),  # verified
                                     IngestionUtil.convert_none(row.__getitem__(29)),  # email
                                     row.__getitem__(30),  # preference_list
                                     "FEED_FILE")
                mysqlcursor.execute(insert_client_row_statement, value_client_row_)
            except (ValueError, IndexError, DataError, MySQLInterfaceError) as e:
                print("[WARN] Unable to correctly parse row in file[" + feed_file + "] row=[" + str(row) + "] exception = " + str(e))

        mydb.commit()
        # after the loads on the temp table, run the store proc to put it in the main table
        # print("[INFO] Running store proc to move data from staging to production table")
        mysqlcursor.callproc("prc_new_clean_up_data")
        mydb.commit()
        mysqlcursor.close()
        # print("[INFO] Disconnected from data source : mysql")
