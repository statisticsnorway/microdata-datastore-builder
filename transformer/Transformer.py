
class Transformer:

    def __init__(self):
        pass

    def transform_name_title_description(self, input: dict) -> dict:
        if input is None:
            return {}

        return {
            'name': input['name'],
            'label': self.get_norwegian_text(input['title']),
            'description': self.get_norwegian_text(input['description'])
        }

    def transform_identifier(self, dataset: dict) -> dict:
        if input is None:
            return {}

        identifier = dataset['identifier'][0]
        return {
            'name': identifier['name'],
            'label': self.get_norwegian_text(identifier['title']),
            'dataType': 'Long',
            'format': identifier['format']
        }

    @staticmethod
    def get_norwegian_text(texts: list) -> str:
        return [element for element in texts if element['languageCode'] == 'no'][0]['value']

