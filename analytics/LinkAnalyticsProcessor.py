class LinkAnalyticsProcessor:

    def __init__(self):
        pass

    def analyse_link(self, start_user_id, number_of_iterations, cursor):
        query = f"select by_user_id from tbl_link where user_id = {start_user_id}"
