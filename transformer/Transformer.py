
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

    @staticmethod
    def get_norwegian_text(texts: list) -> str:
        return [element for element in texts if element['languageCode'] == 'no'][0]['value']

