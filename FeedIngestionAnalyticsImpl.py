import os
import re

from DataIngestionImpl import DataIngestionImpl
from IngestionUtil import IngestionUtil


class FeedIngestionAnalyticsImpl(DataIngestionImpl):

    def load_feed_data(self, feed_files: list):
        for feed_file in feed_files:
            try:
                csv_read_rows = IngestionUtil.get_csv_rows(feed_file)
                print("[INFO] [" + feed_file+ "] columns=" + str(self.determine_number_of_columns(csv_read_rows)) + "   rows=" + str(len(csv_read_rows)))
            except UnicodeDecodeError:
                print("[ERROR] [" + feed_file + "] A UnicodeDecodeError exception has occurred when processing the csv file")

    def determine_number_of_columns(self, csv_read_rows):
        for row in csv_read_rows:
            return len(row)