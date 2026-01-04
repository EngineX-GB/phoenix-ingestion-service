import datetime
import warnings

import mysql
import mysql.connector

from controller.PropertyManager import PropertyManager


class BulkLoadStagingProcessor:

    def __init__(self, property_manager):
        self.property_manager = property_manager
        pass

    def clear_staging(self):
        mydb = mysql.connector.connect(
            host=self.property_manager.get_datasource_url(),
            user=self.property_manager.get_datasource_username(),
            password=self.property_manager.get_datasource_password(),
            database=self.property_manager.get_datasource_name()
        )
        cursor = mydb.cursor()

        query = "delete from tbl_client_bulk_staging"
        cursor.execute(query)
        mydb.commit()
        print("Delete staging data : [" + str(cursor.rowcount) + " rows].")
        cursor.close()
        mydb.close()

    def process_bulk_staging_data(self):
        distinct_dates = []

        query = "select distinct date(refresh_time) from tbl_client_bulk_staging order by date(refresh_time)"

        max_date_loaded_query = "select date(max(refresh_time)) from tbl_client"

        mydb = mysql.connector.connect(
            host=self.property_manager.get_datasource_url(),
            user=self.property_manager.get_datasource_username(),
            password=self.property_manager.get_datasource_password(),
            database=self.property_manager.get_datasource_name()
        )

        cursor = mydb.cursor()

        # get the max date from the database:

        cursor.execute(max_date_loaded_query)
        row = cursor.fetchone()
        if row:
            max_date = row[0]
            if max_date is None:
                print("[INFO] No records pre-exist in the database. Loading feeds.")
            else:
                print("The Max Date in database is : " + max_date.strftime('%Y-%m-%d'))
        else:
            max_date = None

        # Get the distinct dates
        cursor.execute(query)
        results = cursor.fetchall()
        for (date_value,) in results:
            distinct_dates.append(date_value)

        # for each date, copy the dataset into the tbl_client_temp table

        for _feed_date in distinct_dates:
            # check if the date to be loaded has not been loaded already or superseded by a previous load:
            if max_date is not None and max_date > _feed_date:
                # don't proceed the feed file. In this case, continue along
                print("[WARN] Feed dated:" + _feed_date.strftime('%Y-%m-%d') + " has already been processed. Ignoring.")
                continue

            formatted_date_value = _feed_date.strftime('%Y-%m-%d')

            insert_query = "INSERT INTO tbl_client_temp (SELECT * FROM tbl_client_bulk_staging WHERE date(refresh_time) = '" + formatted_date_value + "')"
            cursor.execute(insert_query)
            cursor.callproc("prc_new_clean_up_data")

            # Note: the mysql-connector-python driver may need to be updated to support cursor.stored_results
            # rather than cursor.stored_results() which has deprecated.
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                stored_proc_result = next(cursor.stored_results())
                row = stored_proc_result.fetchone()

                if row:
                    print("[INFO] Load statistics: Date : " + formatted_date_value + ", duplicates deleted: " + str(
                        row[0]) + ", New records : " + str(row[1]) +
                          ", deleted temp records : " + str(row[2]) + ", updated records : " + str(row[3]))
            mydb.commit()

        # run the stored proc to process the day's worth of data.

        cursor.close()
        mydb.close()