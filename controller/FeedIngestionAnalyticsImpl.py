from controller.DataIngestionImpl import DataIngestionImpl
from util.IngestionUtil import IngestionUtil


class FeedIngestionAnalyticsImpl(DataIngestionImpl):

    def load_feed_data(self, feed_files: list):
        dict_column_count = {}
        current_column_count = 0
        for feed_file in feed_files:
            try:
                csv_read_rows = IngestionUtil.get_csv_rows(feed_file)
                num_of_columns = self.determine_number_of_columns(csv_read_rows)
                if current_column_count != num_of_columns:
                    # log the file with the count difference here
                    dict_column_count.update({feed_file: num_of_columns})
                    current_column_count = num_of_columns
                # print("[INFO] [" + feed_file + "] columns=" + str(
                    # self.determine_number_of_columns(csv_read_rows)) + "   rows=" + str(len(csv_read_rows)))
            except UnicodeDecodeError:
                pass
                #print(
                    #"[ERROR] [" + feed_file + "] A UnicodeDecodeError exception has occurred when processing the csv file")
        # print out the dictionary of differences
        for k in dict_column_count.keys():
            print(k + " --> " + str(dict_column_count.get(k)))

    def determine_number_of_columns(self, csv_read_rows):
        for row in csv_read_rows:
            return len(row)
