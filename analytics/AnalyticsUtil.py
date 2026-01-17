class AnalyticsUtil:

    def __init__(self):
        pass

    @staticmethod
    def join_strings_from_list(string_list):
        return ",".join(f"'{x}'" for x in string_list)