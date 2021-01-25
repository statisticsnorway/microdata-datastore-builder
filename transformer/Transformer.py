from datetime import datetime, timedelta


class Transformer:

    def __init__(self):
        pass

    @staticmethod
    def get_norwegian_text(texts: list) -> str:
        return [element for element in texts if element['languageCode'] == 'no'][0]['value']

    @staticmethod
    def days_since_epoch(date_string: str) -> int:
        epoch = datetime.utcfromtimestamp(0)
        date_obj = Transformer.to_date(date_string)
        return (date_obj - epoch).days

    @staticmethod
    def to_date(date_string):
        return datetime.strptime(date_string, '%Y-%m-%d')

    @staticmethod
    def calculate_valid_period(time_period: list) -> dict:
        valid_period = {"start": time_period[0]}
        if 2 == len(time_period):
            valid_period["stop"] = time_period[1]
        return valid_period

    def transform_identifier(self, dataset: dict) -> dict:
        if input is None:
            return {}
        identifier = dataset['identifier'][0]
        return {
            'name': identifier['name'],
            'label': self.get_norwegian_text(identifier['title']),
            'dataType': 'Long',
            'representedVariables': [
                {
                    'validPeriod': {
                        'start': self.days_since_epoch(dataset['dataRevision']['temporalCoverageStart']),
                        'stop': self.days_since_epoch(dataset['dataRevision']['temporalCoverageLatest'])
                    },
                    'valueDomain': self.transform_valuedomain(identifier['valueDomain']),
                    'description': self.get_norwegian_text(identifier['description'])
                }
            ],
            'keyType': self.transform_name_title_description(identifier['unitType']),
            'format': identifier['format'],
            'variableRole': 'Identifier'
        }

    def transform_name_title_description(self, input: dict) -> dict:
        if input is None:
            return {}
        return {
            'name': input['name'],
            'label': self.get_norwegian_text(input['title']),
            'description': self.get_norwegian_text(input['description'])
        }

    def transform_valuedomain(self, valuedomain: dict) -> dict:
        transformed = {}

        # Dette må kalles for å plassere disse feltene til datoointaerval elementene i listen

        if 'description' in valuedomain.keys():
            transformed['description'] = self.get_norwegian_text(valuedomain['description'])
        elif 'measurementUnitDescription' in valuedomain.keys():
            transformed['description'] = self.get_norwegian_text(valuedomain['measurementUnitDescription'])

        if 'measurementUnitDescription' in valuedomain.keys():
            transformed['unitOfMeasure'] = self.get_norwegian_text(valuedomain['measurementUnitDescription'])

        if 'codeList' in valuedomain.keys():
            print('ok')
        else:
            print('not ok')

        return transformed

    def transform_represented_variables(self, valuedomain: dict) -> list:
        transformed = []

        dates_from_all_code_items = []
        for code_item in valuedomain['codeList']['topLevelCodeItems']:
            if 'validityPeriodStart' in code_item:
                dates_from_all_code_items.append(code_item['validityPeriodStart'])
            if 'validityPeriodStop' in code_item:
                dates_from_all_code_items.append(code_item['validityPeriodStop'])

        time_periods = self.calculate_time_periods(dates_from_all_code_items)

        for time_period in time_periods:
            represented_variable = {}
            if self.calculate_description_from_value_domain(valuedomain) is not None:
                represented_variable["description"] = self.calculate_description_from_value_domain(valuedomain)
            if self.calculate_mesurement_unit_description_from_value_domain(valuedomain) is not None:
                represented_variable["unitOfMeasure"] = self.calculate_mesurement_unit_description_from_value_domain(valuedomain)
            represented_variable["validPeriod"] = self.calculate_valid_period(time_period)
            transformed.append(represented_variable)

        return transformed

    def calculate_description_from_value_domain(self, valuedomain: dict) -> str:
        if 'description' in valuedomain.keys():
            return self.get_norwegian_text(valuedomain['description'])
        elif 'measurementUnitDescription' in valuedomain.keys():
            return self.get_norwegian_text(valuedomain['measurementUnitDescription'])
        else:
            return None

    def calculate_mesurement_unit_description_from_value_domain(self, valuedomain: dict) -> str:
        if 'measurementUnitDescription' in valuedomain.keys():
            return self.get_norwegian_text(valuedomain['measurementUnitDescription'])
        else:
            return None

    def calculate_time_periods(self, dates: list) -> list:
        unique_dates = set(dates)
        string_list = list(unique_dates)
        string_list.sort()
        date_list = [self.to_date(date_string) for date_string in string_list]

        one_day: timedelta = timedelta(days=1)
        time_periods = []
        for i, date in enumerate(date_list):
            if i + 1 < len(date_list):
                time_periods.append([date_list[i], date_list[i + 1] - one_day])
            else:
                time_periods.append([date_list[i], None])

        return time_periods
