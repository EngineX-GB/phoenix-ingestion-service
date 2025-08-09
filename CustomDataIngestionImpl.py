import mysql.connector
import json

from Column import Column
from DataIngestionImpl import DataIngestionImpl
from IngestionUtil import IngestionUtil


class CustomDataIngestionImpl(DataIngestionImpl):


    def load_feed_data_by_directory(self, directory_path : str):

        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",
            database="db_phoenix"
        )

        print("[INFO] Connected to data source : mysql")

        mysqlcursor = mydb.cursor()

        sql_column_names_string = ""
        sql_values_string = ""
        column_metadata_list = []
        parameter_object_list = []
        with open('ingestion-config.json') as f:
            config = json.load(f)
            columns = config["columns"]
            for column in columns:
                sql_column_names_string += column["name"] + ","
                sql_values_string += "%s, "
                column_metadata_list.append(Column(column["name"], column["type"],
                                                   column["nullable"], column["position"],
                                                   column["replaceNonSpecifiedValue"],
                                                   column["convertNullToNone"]))

        insert_client_row_statement = "INSERT INTO tbl_client_temp (" + sql_column_names_string[:-1] + ") VALUES (" + sql_values_string[:-2] + ")"
        rows = IngestionUtil.get_csv_rows("C:/Users/Dell/PycharmProjects/data-ingestion/feeds/2025-08-08/clients_2025-08-08_123505.txt")
        print(insert_client_row_statement)

        for row in rows:
            for column in column_metadata_list:
                if column.type == "int":
                    if column.replace_non_specified_value:
                        parameter_object_list.append(int(IngestionUtil.parse_not_specified_value(row.__getitem__(column.position)))),  # age
                    else:
                        parameter_object_list.append(int(row.__getitem__(column.position)))
                elif column.type == "string":
                    if column.convert_none_to_null:
                        parameter_object_list.append(IngestionUtil.convert_none(row.__getitem__(column.position))),
                    else:
                        parameter_object_list.append(row.__getitem__(column.position))
                elif column.type == "boolean":
                    parameter_object_list.append(bool(row.__getitem__(column.position)))

            parameter_object_tuple = tuple(parameter_object_list)
            mysqlcursor.execute(insert_client_row_statement, parameter_object_tuple)
            # reset the parameter object list
            parameter_object_list.clear()
        #
        mydb.commit()
        # # after the loads on the temp table, run the store proc to put it in the main table
        print("[INFO] Running store proc to move data from staging to production table")
        mysqlcursor.callproc("prc_new_clean_up_data")
        mydb.commit()
        mysqlcursor.close()
        print("[INFO] Disconnected from data source : mysql")

    def load_feed_data(self, feed_files: list):
        pass
    def populate_staging_data(self, csv_row: list):
        pass