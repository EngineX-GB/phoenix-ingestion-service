from analytics.AnalyticsUtil import AnalyticsUtil


class LinkAnalyticsProcessor:

    def __init__(self):
        pass


    def get_user_2_list(self, user_id_list: list[str], starting_iteration_number, number_of_iterations, cursor):
        query1 = f"select user_id_2 from tbl_link where user_id_1 in ({AnalyticsUtil.join_strings_from_list(user_id_list)})"
        cursor.execute(query1)
        user_id_list.clear()
        resultset_userids = cursor.fetchall()
        for (user_id,) in resultset_userids:
           user_id_list.append(user_id)
        starting_iteration_number = starting_iteration_number + 1
        if starting_iteration_number < number_of_iterations:
            self.get_user_2_list(user_id_list, starting_iteration_number, number_of_iterations, cursor)
        else:
            # don't iterate and check the status of the clients to see if they have any red flags associated with them.
            red_flag_query = f"SELECT user_id from tbl_client where user_id in ({AnalyticsUtil.join_strings_from_list(user_id_list)}) and preference_list like '%BB%'"
            cursor.execute(red_flag_query)
            resultset = cursor.fetchall()
            for (bad_user,) in resultset:
                print("BadUser : " + bad_user)