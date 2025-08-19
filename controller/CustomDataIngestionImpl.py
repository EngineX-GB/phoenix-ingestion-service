from datetime import datetime

import mysql.connector
import json

from _mysql_connector import MySQLInterfaceError
from mysql.connector import DataError

from model.Column import Column
from controller.DataIngestionImpl import DataIngestionImpl
from util.IngestionUtil import IngestionUtil


class CustomDataIngestionImpl(DataIngestionImpl):

    def __init__(self, column_configfile_list, property_manager):
        super().__init__(property_manager)
        self.column_configfile_list = column_configfile_list
        self.dict = self.build_column_count_to_config_dictionary(self.column_configfile_list)
        self.debug = False

    def build_column_count_to_config_dictionary(self, column_configfile_list):
        column_count_dictionary = {}
        for config_file in column_configfile_list:
            with open(config_file) as f:
                config_data = json.load(f)
                columns = config_data["columns"]
                column_count_dictionary[str(len(columns))] = config_file
                f.close()
        return column_count_dictionary

    def populate_staging_data(self, csv_row: list, feed_file):

        mydb = mysql.connector.connect(
            host = self.property_manager.get_datasource_url(),
            user = self.property_manager.get_datasource_username(),
            password = self.property_manager.get_datasource_password(),
            database = self.property_manager.get_datasource_name()
        )

        # print("[INFO] Connected to data source : mysql")

        mysqlcursor = mydb.cursor()

        sql_column_names_string = ""
        sql_values_string = ""
        column_metadata_list = []
        parameter_object_list = []
        is_number_of_columns_counted = False

        for row in csv_row:
            try:
                if not is_number_of_columns_counted:
                    number_of_columns = len(row)  # number of columns in the feed file being read
                    is_number_of_columns_counted = True
                    ingestion_config_file = self.dict.get(str(number_of_columns))
                    if ingestion_config_file is not None:
                        with open(ingestion_config_file) as f:
                            config = json.load(f)
                            columns = config["columns"]
                            for column in columns:
                                sql_column_names_string += column["name"] + ","
                                sql_values_string += "%s, "
                                column_metadata_list.append(Column(column["name"], column["type"],
                                                                   column["nullable"], column["position"],
                                                                   column["replaceNonSpecifiedValue"],
                                                                   column["convertNullToNone"],
                                                                   column["convertUKDateToSqlDate"],
                                                                   column["convertLatinCharsetToUtf"]))
                    else:
                        raise RuntimeError("No ingestion config file that can handle parsing "
                                           + str(number_of_columns) + " columns")

                insert_client_row_statement = "INSERT INTO tbl_client_temp (" + sql_column_names_string[
                                                                                :-1] + ") VALUES (" + sql_values_string[
                                                                                                      :-2] + ")"

                if self.debug:
                    print("[DEBUG] " + insert_client_row_statement)

                for column in column_metadata_list:
                    if column.type == "int":
                        if column.replace_non_specified_value:
                            parameter_object_list.append(
                                int(IngestionUtil.parse_not_specified_value(row.__getitem__(column.position)))),  # age
                        elif column.convert_none_to_null:
                            parameter_object_list.append(
                                (IngestionUtil.convert_none_by_type(row.__getitem__(column.position), "int")))
                        else:
                            parameter_object_list.append(int(row.__getitem__(column.position)))
                    elif column.type == "string":
                        if column.convert_none_to_null:
                            parameter_object_list.append(IngestionUtil.convert_none(row.__getitem__(column.position)))
                        elif column.convert_uk_date_to_sql_date:
                            parameter_object_list.append(
                                datetime.strptime(row.__getitem__(column.position), '%d/%m/%Y').strftime('%Y-%m-%d'))
                        elif column.convert_latin_charset_to_utf:
                            parameter_object_list.append(IngestionUtil.fix_encoded_string(row.__getitem__(column.position)))
                        else:
                            parameter_object_list.append(row.__getitem__(column.position))
                    elif column.type == "boolean":
                        parameter_object_list.append(bool(row.__getitem__(column.position)))

                parameter_object_tuple = tuple(parameter_object_list)
                mysqlcursor.execute(insert_client_row_statement, parameter_object_tuple)
                # reset the parameter object list
                parameter_object_list.clear()
            except (ValueError, IndexError, DataError, MySQLInterfaceError) as e:
                print("[WARN] Unable to correctly parse row in file [ " + feed_file + "] row=[" + str(row) + "] exception=[" + str(e) + "]")
                # try and delete the previously added object in the parameter_object_tuple
                parameter_object_list.clear()   # to clear the corrupted entry. Important!!
                continue
        #
        mydb.commit()
        # # after the loads on the temp table, run the store proc to put it in the main table
        # print("[INFO] Running store proc to move data from staging to production table")
        mysqlcursor.callproc("prc_new_clean_up_data")
        mydb.commit()
        mysqlcursor.close()
        # print("[INFO] Disconnected from data source : mysql")
