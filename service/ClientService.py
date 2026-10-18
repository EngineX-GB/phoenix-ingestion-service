import requests


class ClientService:

    def __init__(self, property_manager):
        self.property_manager = property_manager


    def send_notification(self,notification_type, operation, status, detail):
        payload = {
            "notificationType" : notification_type,
            "content" : {
                "operation" : operation,
                "serviceName" : "data-ingestion",
                "status" : status,
                "detail" : detail
            }
        }
        response = requests.post(f"{self.property_manager.get_client_service_url()}/notifications/publish", json=payload)
        if response.status_code != 201:
            print(f'[ERROR] An error has occurred when sending a notification to the phoenix client service : {response.status_code}')

