import datetime


class Transformer:

    def __init__(self):
        pass

    @staticmethod
    def get_norwegian_text(texts: list) -> str:
        return [element for element in texts if element['languageCode'] == 'no'][0]['value']

    @staticmethod
    def days_since_epoch(date: str) -> int:
        epoch = datetime.datetime.utcfromtimestamp(0)
        date_obj = datetime.datetime.strptime(date, '%Y-%m-%d')
        return (date_obj - epoch).days

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
