from DataAccessImpl import DataAccessImpl
import sys

if __name__ == "__main__":
    dataAccess = DataAccessImpl()

    if len(sys.argv) == 1:
        print("[ERROR] Invalid number of parameters. Type --help for more information")
        exit(1)
    if sys.argv[1] == "--help":
        print("[INFO] Help Manual added here")
        exit(0)
    if sys.argv[1] == "--service":
        print("[INFO] Running Ingestion Service....")
        print("[WARN] This feature is not supported yet.")
        exit(0)
    if sys.argv[1] == "--cmd":
        print("[INFO] Running Ingestion Service....")
        if sys.argv[2] == "--batch":
            folder_path = sys.argv[3]
            dataAccess.load_feed_data_by_directory(folder_path)
        elif sys.argv[2] == "--custom-load":
            feed_file_list_string = sys.argv[3]
            feed_file_list = feed_file_list_string.split(",")
            dataAccess.load_feed_data(feed_file_list)
