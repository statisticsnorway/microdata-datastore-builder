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
            "attributeVariables": Transformer.get_attribute_variables(dataset['attribute']),
            "identifierVariables": "[TODO]",
            "measureVariable": Transformer.transform_measure(dataset['measure']),
            'name': dataset['name'],
            "populationDescription": Transformer.get_norwegian_text(dataset['populationDescription']),
            "temporality": dataset['temporalityType'],
            "temporalCoverage": Transformer.get_temporal_coverage(dataset['dataRevision']),
            "subjectFields": Transformer.get_subject_fields(dataset['measure']['subjectField']),
            "languageCode": "no"
        }

    @staticmethod
    def transform_measure(measure: dict) -> dict:
        return {
            'name': measure['name'],
            'label': Transformer.get_norwegian_text(measure['title']),
            'dataType': Transformer.transform_data_type(measure['dataType']),
            'representedVariables': "TODO",
            'keyType': Transformer.transform_unit_type(measure["unitType"]),
            'format': measure['format'],
            'variableRole': "Measure"
        }

    @staticmethod
    def get_attribute_variables(attributes: list) -> list:
        result = []
        for attribute in attributes:
            attr = {
                "name": attribute["name"],
                "label": Transformer.get_norwegian_text(attribute["title"]),
                "representedVariables": "TODO",
                "dataType": Transformer.transform_data_type(attribute['dataType']),
                "variableRole": Transformer.get_variable_role(attribute['attributeType'])
            }
            if "unitType" in attribute:
                attr['keyType'] = Transformer.transform_unit_type(attribute["unitType"])
            if "format" in attribute:
                attr['format'] = attribute["format"]

            result.append(attr)

        return result

    @staticmethod
    def get_variable_role(attribute_type: str) -> str:
        variable_roles_mapping = {
            "stop": "Stop",
            "start": "Start",
            "source": "Source"
        }

        return variable_roles_mapping.get(attribute_type.lower(), attribute_type)

    @staticmethod
    def transform_data_type(data_type: str) -> str:
        data_types_mapping = {
            "STRING": "String",
            "LONG": "Long",
            "DOUBLE": "Double",
            "DATE": "Instant"
        }

        return data_types_mapping.get(data_type, data_type)

    @staticmethod
    def transform_unit_type(unit_type: dict) -> dict:
        if not unit_type:
            return {}

        return {
            'name': unit_type["name"],
            'label': Transformer.get_norwegian_text(unit_type["title"]),
            'description': Transformer.get_norwegian_text(unit_type["description"])
        }

    @staticmethod
    def get_temporal_coverage(data_revision: dict) -> dict:
        period = {
            "start": Transformer.days_since_epoch(data_revision['temporalCoverageStart'])
        }
        if data_revision['temporalCoverageLatest']:
            period["stop"] = Transformer.days_since_epoch(data_revision['temporalCoverageLatest'])

        return period

    @staticmethod
    def get_subject_fields(subject_fields: dict) -> list:
        return [Transformer.get_norwegian_text(subject_field['title']) for subject_field in subject_fields]

    @staticmethod
    def calculate_valid_period(time_period: list) -> dict:
        valid_period = {"start": time_period[0]}
        if 2 == len(time_period) and time_period[1] is not None:
            valid_period["stop"] = time_period[1]
        return valid_period

    def transform_identifier(self, dataset: dict) -> dict:
        if input is None:
            return {}
        identifier = dataset['identifier'][0]
        start = dataset['dataRevision']['temporalCoverageStart']
        stop = dataset['dataRevision']['temporalCoverageLatest']
        transformed = {
            'name': identifier['name'],
            'label': self.get_norwegian_text(identifier['title']),
            'dataType': 'Long',
            'representedVariables': self.transform_represented_variables(identifier, start, stop),
            'keyType': self.transform_name_title_description(identifier['unitType']),
            'format': identifier['format'],
            'variableRole': 'Identifier'
        }
        return transformed

    def transform_name_title_description(self, input: dict) -> dict:
        if not input:
            return {}
        return {
            'name': input['name'],
            'label': self.get_norwegian_text(input['title']),
            'description': self.get_norwegian_text(input['description'])
        }

    def transform_represented_variables(self, entity: dict, start: str, stop: str) -> list:
        value_domain = entity['valueDomain']
        description = self.get_norwegian_text(entity['description'])
        if 'codeList' in value_domain.keys():
            return self.transform_value_domain_with_codelist(value_domain, description)
        else:
            return self.transform_value_domain_without_codelist(value_domain, description, start, stop)

    def transform_value_domain_without_codelist(self, value_domain: dict, description: str, start: str, stop: str) -> list:
        transformed = []
        represented_variable = {}
        time_period = [ self.to_date(start), self.to_date(stop)]
        represented_variable["validPeriod"] = self.calculate_valid_period(time_period)
        represented_variable["description"] = description
        value_domain_out = {}
        if self.create_description_from_value_domain(value_domain) is not None:
            value_domain_out["description"] = self.create_description_from_value_domain(value_domain)
        if self.create_mesurement_unit_description_from_value_domain(value_domain) is not None:
            value_domain_out["unitOfMeasure"] = self.create_mesurement_unit_description_from_value_domain(value_domain)
        represented_variable["valueDomain"] = value_domain_out
        transformed.append(represented_variable)
        return transformed

    def transform_value_domain_with_codelist(self, value_domain: dict, description: str) -> list:
        transformed = []

        dates_from_all_code_items = []
        for code_item in value_domain['codeList']['topLevelCodeItems']:
            if 'validityPeriodStart' in code_item:
                dates_from_all_code_items.append(code_item['validityPeriodStart'])
            if 'validityPeriodStop' in code_item:
                dates_from_all_code_items.append(code_item['validityPeriodStop'])

        time_periods = self.calculate_time_periods(dates_from_all_code_items)

        for time_period in time_periods:
            represented_variable = {"description": description,
                                    "validPeriod": self.calculate_valid_period(time_period)}

            if 'codeList' in value_domain.keys():
                code_list_out = []
                for code_item in value_domain['codeList']['topLevelCodeItems']:
                    self.select_code_item(code_item, code_list_out, time_period)

                value_domain_out = {
                    "codeList": code_list_out,
                    "missingValues": []
                }
                represented_variable["valueDomain"] = value_domain_out
            transformed.append(represented_variable)
        return transformed

    def select_code_item(self, code_item, code_list_out, time_period):
        time_period_start = time_period[0]
        time_period_stop = None if time_period[1] is None else time_period[1]

        validity_period_start = datetime.strptime(code_item['validityPeriodStart'], '%Y-%m-%d')
        validity_period_stop = datetime.strptime(code_item['validityPeriodStop'], '%Y-%m-%d') \
            if 'validityPeriodStop' in code_item.keys() else None

        if time_period_stop is None:
            if validity_period_start <= time_period_start and validity_period_stop is None:
                self.append_code_item_to_list(code_item, code_list_out)
        else:
            if validity_period_stop is None:
                if validity_period_start <= time_period_start:
                    self.append_code_item_to_list(code_item, code_list_out)
            else:
                if validity_period_start <= time_period_start and validity_period_stop > time_period_stop:
                    self.append_code_item_to_list(code_item, code_list_out)

    def append_code_item_to_list(self, code_item:dict, code_list:list):
        code_list.append({
            "category": self.get_norwegian_text(code_item['categoryTitle']),
            "code": code_item['code']
        })

    def create_description_from_value_domain(self, valuedomain: dict) -> str:
        if 'description' in valuedomain.keys():
            return self.get_norwegian_text(valuedomain['description'])
        elif 'measurementUnitDescription' in valuedomain.keys():
            return self.get_norwegian_text(valuedomain['measurementUnitDescription'])
        else:
            return None

    def create_mesurement_unit_description_from_value_domain(self, valuedomain: dict) -> str:
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
