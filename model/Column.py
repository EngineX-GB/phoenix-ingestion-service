class Column:

    def __init__(self, name, type, nullable, position, replace_non_specified_value, convert_none_to_null,
                 convert_uk_date_to_sql_date, convert_charset_to_utf):
        self.name = name
        self.type = type
        self.nullable = nullable
        self.position = position
        self.replace_non_specified_value = replace_non_specified_value
        self.convert_none_to_null = convert_none_to_null
        self.convert_uk_date_to_sql_date = convert_uk_date_to_sql_date
        self.convert_charset_to_utf = convert_charset_to_utf
