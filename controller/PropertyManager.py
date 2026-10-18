import os


class PropertyManager:

    def __init__(self):
        self.datasource_url = "localhost" if (
                os.getenv("DATASOURCE_URL") is None) else str(os.getenv("DATASOURCE_URL"
                                                                        ""))
        self.datasource_username = "root" if (
                os.getenv("DATASOURCE_USERNAME") is None) else str(os.getenv("DATASOURCE_USERNAME"))
        self.datasource_password = "root" if (
                os.getenv("DATASOURCE_PASSWORD") is None) else str(os.getenv("DATASOURCE_PASSWORD"))
        self.datasource_name = "db_phoenix" if (
                os.getenv("DATASOURCE_NAME") is None) else str(os.getenv("DATASOURCE_NAME"))
        self.client_service_url = "http://localhost:8081" if (
                os.getenv("CLIENT_SERVICE_URL") is None) else str(os.getenv("CLIENT_SERVICE_URL"))

        self.environment = "PROD" if (os.getenv("ENV") is None) else str(os.getenv("ENV"))
        self.service_port = 8000 if (os.getenv("SERVICE_PORT") is None) else int(os.getenv("SERVICE_PORT"))
        self.service_host = "127.0.0.1" if (os.getenv("SERVICE_HOST") is None) else str(os.getenv("SERVICE_HOST"))

    def get_datasource_url(self):
        return self.datasource_url

    def get_datasource_username(self):
        return self.datasource_username

    def get_datasource_password(self):
        return self.datasource_password

    def get_datasource_name(self):
        return self.datasource_name

    def get_client_service_url(self):
        return self.client_service_url

    def getServicePort(self):
        return self.service_port

    def getServiceHost(self):
        return self.service_host

    def getEnvironment(self):
        return self.environment

    def get_list_of_configs(self):
        return {
            "environment" : self.environment,
            "datasource_url" : self.datasource_url,
            "datasource_username" : self.datasource_username,
            "datasource_password" : self.datasource_password,
            "datasource_name" : self.datasource_name,
            "client_service_url" : self.client_service_url
        }