from CustomDataIngestionImpl import CustomDataIngestionImpl
from FeedIngestionAnalyticsImpl import FeedIngestionAnalyticsImpl
from DataIngestionImpl import DataIngestionImpl
import multiprocessing

import sys
import os
import uvicorn

if __name__ == "__main__":
    dataAccess = DataIngestionImpl()
    feedAnalyser = FeedIngestionAnalyticsImpl()

    internal_flags = ["--multiprocessing-fork"]
    if any(flag in sys.argv for flag in internal_flags):
        # Let uvicorn handle this internally
        pass
    else:
        if len(sys.argv) == 1:
            print("[ERROR] Invalid number of parameters. Type --help for more information")
        elif sys.argv[1] == "--help":
            print("[INFO] Help Manual added here")
        elif sys.argv[1] == "--service":
            print("[INFO] Running Ingestion Service....")
            print("[WARN] This feature is not supported yet.")
        elif sys.argv[1] == "--api":
            print("[INFO] Starting FastAPI server...")
            multiprocessing.freeze_support()
            uvicorn.run("main:app", host="127.0.0.1", port=8000)
        elif sys.argv[1] == "--cmd":
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
                config_mapping_files_list_string = sys.argv[4]
                config_mappers = config_mapping_files_list_string.split(",")
                for mapper in config_mappers:
                    if not os.path.exists(mapper):
                        raise RuntimeError("Mapper file : " + mapper + " does not exist")
                folder_path = sys.argv[3]
                dynamicLoad = CustomDataIngestionImpl(config_mappers)
                dynamicLoad.load_feed_data_by_directory(folder_path)
            else:
                print("[ERROR] Unknown cmd flag " + sys.argv[2])
        else:
            print("[ERROR] Unknown flag " + sys.argv[1])
