import csv
import os


class IngestionUtil:

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