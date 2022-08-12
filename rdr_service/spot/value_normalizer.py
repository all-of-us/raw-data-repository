# Module for normalizing data values


class ValueNormalizer:
    def __init__(self):
        # map rules to methods; 'rule_name': rule_method
        self.rule_map = {
            "boolean_to_upper_string": self.rule_boolean_to_upper_string,
            "upper": self.rule_upper,
            "percent_to_ratio": self.rule_percent_to_ratio,
            "clean_null": self.rule_clean_null,
        }

    @staticmethod
    def rule_boolean_to_upper_string(value):
        print('performing boolean to upper string')
        if value is None:
            return value
        if value.lower() in ['1', 'true', 'y']:
            return "TRUE"
        elif value.lower() in ['0', 'false', 'n']:
            return "FALSE"
        else:
            return value

    @staticmethod
    def rule_upper(value):
        print('performing upper')
        if value is None:
            return value
        return value.upper()

    @staticmethod
    def rule_percent_to_ratio(value):
        print('performing percent to ratio')
        if value is None:
            return value
        try:
            value = float(value)

        except ValueError:
            return value

        # Assuming values over 1 are pct and values under 1 are ratios.
        if value > 1:
            value = value / 100

        if value > 0.001:
            return str(value)
        else:
            return "{:e}".format(value)  # returns scientific notation

    @staticmethod
    def rule_clean_null(value):
        print('performing clean null')
        if value is None:
            return value
        if value.lower() in ['null', '']:
            return None
        else:
            return value
