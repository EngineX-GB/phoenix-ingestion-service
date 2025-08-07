from DataAccessImpl import DataAccessImpl

if __name__ == "__main__":
    dataAccess = DataAccessImpl()
    feed_files_list = ['feeds/clients_2025-08-07_130951.txt',
                       'feeds/clients_2025-08-07_131023.txt',
                       'feeds/clients_2025-08-07_131058.txt',
                       'feeds/clients_2025-08-07_131129.txt',
                       'feeds/clients_2025-08-07_131158.txt',
                       'feeds/clients_2025-08-07_131228.txt',
                       'feeds/clients_2025-08-07_131256.txt',
                       'feeds/clients_2025-08-07_131326.txt',
                       'feeds/clients_2025-08-07_131355.txt',
                       'feeds/clients_2025-08-07_131422.txt',
                       'feeds/clients_2025-08-07_131449.txt',
                       'feeds/clients_2025-08-07_131514.txt',
                       'feeds/clients_2025-08-07_131535.txt',
                       'feeds/clients_2025-08-07_131602.txt',
                       'feeds/clients_2025-08-07_131618.txt'
                       ]
    dataAccess.load_feed_data(feed_files_list)
