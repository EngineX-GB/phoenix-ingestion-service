import os
class PropertyManager:


    def __init__(self):
        self.datasource_url = "localhost" if (
                os.getenv("DATASOURCE_URL") is None) else str(os.getenv("DATASOURCE_URL"))
        self.datasource_username = "root" if (
            os.getenv("DATASOURCE_USERNAME") is None) else str(os.getenv("DATASOURCE_USERNAME"))
        self.datasource_password = "root" if (
                os.getenv("DATASOURCE_PASSWORD") is None) else str(os.getenv("DATASOURCE_PASSWORD"))
        self.datasource_name = "db_phoenix" if (
                os.getenv("DATASOURCE_NAME") is None) else str(os.getenv("DATASOURCE_NAME"))

    def get_datasource_url(self):
        return self.datasource_url

    def get_datasource_username(self):
        return self.datasource_username

    def get_datasource_password(self):
        return self.datasource_password

    def get_datasource_name(self):
        return self.datasource_name