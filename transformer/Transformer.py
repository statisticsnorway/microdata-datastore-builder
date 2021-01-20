
class Transformer:

    def __init__(self):
        pass

    def transform_unit_type(self, unit_type) -> dict:
        if unit_type is None:
            return {}

        return {
            'name': unit_type['name'],
            'label': self.get_norwegian_text(unit_type['title']),
            'description': self.get_norwegian_text(unit_type['description'])
        }

    @staticmethod
    def get_norwegian_text(texts: list) -> str:
        return [element for element in texts if element['languageCode'] == 'no'][0]['value']

