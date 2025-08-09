from CustomDataIngestionImpl import CustomDataIngestionImpl
from FeedIngestionAnalyticsImpl import FeedIngestionAnalyticsImpl
from DataIngestionImpl import DataIngestionImpl
import sys

if __name__ == "__main__":
    dataAccess = DataIngestionImpl()
    feedAnalyser = FeedIngestionAnalyticsImpl()
    dynamicLoad = CustomDataIngestionImpl()

    if len(sys.argv) == 1:
        print("[ERROR] Invalid number of parameters. Type --help for more information")
    if sys.argv[1] == "--help":
        print("[INFO] Help Manual added here")
    if sys.argv[1] == "--service":
        print("[INFO] Running Ingestion Service....")
        print("[WARN] This feature is not supported yet.")
    if sys.argv[1] == "--cmd":
        print("[INFO] Running Ingestion Service....")
        if sys.argv[2] == "--batch":
            folder_path = sys.argv[3]
            dataAccess.load_feed_data_by_directory(folder_path)
        elif sys.argv[2] == "--custom-load":
            feed_file_list_string = sys.argv[3]
            feed_file_list = feed_file_list_string.split(",")
            dataAccess.load_feed_data(feed_file_list)
        elif sys.argv[2] == "--analyse":
            folder_path = sys.argv[3]
            feedAnalyser.load_feed_data_by_directory(folder_path)
        elif sys.argv[2] == "--dynamic":
            folder_path = sys.argv[3]
            dynamicLoad.load_feed_data_by_directory(folder_path)
