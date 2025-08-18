import csv
import io
import json
import os
import sys

import mysql.connector
class IngestionUtil:

    def get_csv_rows_via_text_wrapper(text_wrapper:io.TextIOWrapper):
        datarows = []
        reader = csv.reader(text_wrapper, delimiter='|')
        for row in reader:
            datarows.append(row)
        return datarows

    @staticmethod
    def fix_encoded_string(text):
        return text.encode('latin1').decode('utf-8')

    @staticmethod
    def resource_path(relative_path):
        """ Get absolute path to resource, works for dev and for PyInstaller """
        base_path = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))
        return os.path.join(base_path, relative_path)

    @staticmethod
    def app_version():
        with open(IngestionUtil.resource_path("version.json")) as file:
            app_data = json.load(file)
            return app_data["version"]

    @staticmethod
    def check_latest_entry_in_datastore(property_manager):
        mydb = mysql.connector.connect(
            host = property_manager.get_datasource_url(),
            user = property_manager.get_datasource_username(),
            password = property_manager.get_datasource_password(),
            database = property_manager.get_datasource_name()
        )
        cursor = mydb.cursor()
        query = "select max(date(refresh_time)) from tbl_client;"
        cursor.execute(query)
        result = cursor.fetchone()

        max_date = result[0] if result[0] is not None else None
        cursor.close()
        mydb.close()
        return max_date


    @staticmethod
    def get_csv_rows(filename):
        datarows = []
        with open(filename) as file:
            rows = csv.reader(file, delimiter='|')
            for row in rows:
                datarows.append(row)
        return datarows

    @staticmethod
    def parse_not_specified_value(value: str):
        if value == "Not Specified":
            return 0
        return value

    @staticmethod
    def check_for_subdirectories(directory):
        objects = os.listdir(directory)
        for o in objects:
            if os.path.isdir(os.path.join(directory, o)):
                return True
        return False

    @staticmethod
    def convert_none(value : str):
        if value == "None":
            return None
        return value

    @staticmethod
    def convert_none_by_type(value : str, type : str):
        if value == "None":
            return None
        if type == "int":
            value = value.replace("+", "")
            return int(value)