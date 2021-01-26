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
    def transform_dataset(dataset: dict) -> dict:
        return {
            "attributeVariables": "[TODO]",
            "identifierVariables": "[TODO]",
            "measureVariable": "TODO",
            'name': dataset['name'],
            "populationDescription": Transformer.get_norwegian_text(dataset['populationDescription']),
            "temporality":  dataset['temporalityType'],
            "temporalCoverage": Transformer.get_temporal_coverage(dataset['dataRevision']),
            "subjectFields": Transformer.get_subject_fields(dataset['measure']['subjectField']),
            "languageCode": "no"
        }

    @staticmethod
    def get_temporal_coverage(data_revision: dict) -> dict:
        period = {
            "start": Transformer.days_since_epoch(data_revision['temporalCoverageStart'])
        }
        if data_revision['temporalCoverageLatest'] is not None:
            period["stop"] = Transformer.days_since_epoch(data_revision['temporalCoverageLatest'])

        return period

    @staticmethod
    def get_subject_fields(subject_fields: dict) -> list:
        return [Transformer.get_norwegian_text(subject_field['title']) for subject_field in subject_fields]

    """ 
    We need to refactor valuedomain transformation, make new method transform_value_domain.
    This must transform value domains with or without code lists.
    
    Code example for description (Elvis operator in python?)
        self.get_norwegian_text(identifier['valueDomain']['measurementUnitDescription']),
        if (identifier['valueDomain']['description'] is not None)
        else self.get_norwegian_text(identifier['valueDomain']['measurementUnitDescription'])
    
    """

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

        if 'description' in valuedomain.keys():
            transformed['description'] = self.get_norwegian_text(valuedomain['description'])
        elif 'measurementUnitDescription' in valuedomain.keys():
            transformed['description'] = self.get_norwegian_text(valuedomain['measurementUnitDescription'])

        if 'measurementUnitDescription' in valuedomain.keys():
            transformed['unitOfMeasure'] = self.get_norwegian_text(valuedomain['measurementUnitDescription'])

        if 'codeList' in valuedomain.keys():
            print ('ok')
        else:
            print('not ok')

        return transformed

    def calculate_time_periods(self, dates: list) -> list:
        unique_dates = set(dates)
        string_list = list(unique_dates)
        string_list.sort()
        date_list = [self.to_date(date_string) for date_string in string_list]

        one_day: timedelta = timedelta(days=1)
        time_periods = []
        for i, date in enumerate(date_list):
            if i+1 < len(date_list):
                time_periods.append([date_list[i], date_list[i+1] - one_day])
            else:
                time_periods.append([date_list[i], None])

        return time_periods

