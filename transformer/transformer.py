from datetime import datetime


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
            'name': unit_type["shortName"],
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
    def transform_subject_fields(subject_fields: dict) -> list:
        return [Transformer.get_norwegian_text(subject_field['title']) for subject_field in subject_fields]

    @staticmethod
    def calculate_valid_period(time_period: list) -> dict:
        valid_period = {"start": time_period[0]}
        if 2 == len(time_period) and time_period[1] is not None:
            valid_period["stop"] = time_period[1]
        return valid_period

    @staticmethod
    def transform_represented_variables(entity: dict, start: str, stop: str) -> list:
        value_domain = entity['valueDomain']
        description = Transformer.get_norwegian_text(entity['description']) if 'description' in entity else None

        if 'codeList' in value_domain.keys():
            represented_variables = Transformer.transform_enumerated_value_domain(value_domain, description)
            if 'sentinelAndMissingValues' in entity.keys():
                missing_codes = Transformer.transform_missing_values(entity['sentinelAndMissingValues'])
                for represented_variable in represented_variables:
                    represented_variable['valueDomain']['missingValues'] = missing_codes
            return represented_variables
        else:
            return Transformer.transform_described_value_domain(value_domain, description, start, stop)

    @staticmethod
    def transform_missing_values(missing_values: list) -> list:
        codes = []
        for code_element in missing_values:
            codes.append(code_element['code'])
        return codes

    @staticmethod
    def transform_described_value_domain(value_domain: dict, description: str, start: str,
                                         stop: str) -> list:
        transformed = []
        represented_variable = {}
        time_period = [Transformer.days_since_epoch(start), Transformer.days_since_epoch(stop)]
        represented_variable["validPeriod"] = Transformer.calculate_valid_period(time_period)
        represented_variable["description"] = description
        value_domain_out = {}
        if Transformer.create_description_from_value_domain(value_domain) is not None:
            value_domain_out["description"] = Transformer.create_description_from_value_domain(value_domain)
        if Transformer.create_mesurement_unit_description_from_value_domain(value_domain) is not None:
            value_domain_out["unitOfMeasure"] = Transformer.create_mesurement_unit_description_from_value_domain(
                value_domain)

        if value_domain_out:
            represented_variable["valueDomain"] = value_domain_out
        transformed.append(represented_variable)
        return transformed

    @staticmethod
    def transform_enumerated_value_domain(value_domain: dict, description: str) -> list:
        transformed = []

        dates_from_all_code_items = []
        for code_item in value_domain['codeList']['codeItems']:
            if 'validFrom' in code_item:
                dates_from_all_code_items.append(Transformer.days_since_epoch(code_item['validFrom']))
            if 'validUntil' in code_item:
                dates_from_all_code_items.append(Transformer.days_since_epoch(code_item['validUntil']))

        time_periods = Transformer.calculate_time_periods(dates_from_all_code_items)

        for time_period in time_periods:
            represented_variable = {"description": description,
                                    "validPeriod": Transformer.calculate_valid_period(time_period)}

            if 'codeList' in value_domain.keys():
                code_list_out = []
                for code_item in value_domain['codeList']['codeItems']:
                    Transformer.select_code_item(code_item, code_list_out, time_period)

                value_domain_out = {
                    "codeList": code_list_out
                }
                represented_variable["valueDomain"] = value_domain_out
            transformed.append(represented_variable)
        return transformed

    @staticmethod
    def select_code_item(code_item, code_list_out, time_period):
        time_period_start = time_period[0]
        time_period_stop = None if time_period[1] is None else time_period[1]

        valid_from = Transformer.days_since_epoch(code_item['validFrom'])
        valid_until = Transformer.days_since_epoch(code_item['validUntil']) \
            if 'validUntil' in code_item.keys() else None

        if time_period_stop is None:
            if valid_from <= time_period_start and valid_until is None:
                Transformer.append_code_item_to_list(code_item, code_list_out)
        else:
            if valid_until is None:
                if valid_from <= time_period_start:
                    Transformer.append_code_item_to_list(code_item, code_list_out)
            else:
                if valid_from <= time_period_start and valid_until > time_period_stop:
                    Transformer.append_code_item_to_list(code_item, code_list_out)

    @staticmethod
    def append_code_item_to_list(code_item: dict, code_list: list):
        code_list.append({
            "category": Transformer.get_norwegian_text(code_item['categoryTitle']),
            "code": code_item['code']
        })

    @staticmethod
    def create_description_from_value_domain(valuedomain: dict) -> str:
        if 'description' in valuedomain.keys():
            return Transformer.get_norwegian_text(valuedomain['description'])
        elif 'measurementUnitDescription' in valuedomain.keys():
            return Transformer.get_norwegian_text(valuedomain['measurementUnitDescription'])
        elif 'title' in valuedomain.keys():
            return Transformer.get_norwegian_text(valuedomain['title'])
        else:
            raise Exception('Cannot create description from value domain ' + valuedomain['name'])

    @staticmethod
    def create_mesurement_unit_description_from_value_domain(valuedomain: dict) -> str:
        if 'measurementUnitDescription' in valuedomain.keys():
            return Transformer.get_norwegian_text(valuedomain['measurementUnitDescription'])
        else:
            return None

    @staticmethod
    def calculate_time_periods(dates: list) -> list:
        unique_dates = set(dates)
        days_since_epoch_list = list(unique_dates)
        days_since_epoch_list.sort()

        one_day = 1
        time_periods = []
        for i, date in enumerate(days_since_epoch_list):
            if i + 1 < len(days_since_epoch_list):
                time_periods.append([days_since_epoch_list[i], days_since_epoch_list[i + 1] - one_day])
            else:
                time_periods.append([days_since_epoch_list[i], None])

        return time_periods

    @staticmethod
    def get_attribute_variable(attribute: dict, start: str, stop: str) -> list:
        attr = {
            "name": attribute["shortName"],
            "label": Transformer.get_norwegian_text(attribute["title"]),
            "dataType": Transformer.transform_data_type(attribute['dataType']),
            "variableRole": Transformer.get_variable_role(attribute['variableRole'])
        }
        if "valueDomain" in attribute:
            attr['representedVariables'] = Transformer.transform_represented_variables(attribute, start, stop)
        if "unitType" in attribute:
            attr['keyType'] = Transformer.transform_unit_type(attribute["unitType"])
        if "format" in attribute:
            attr['format'] = attribute["format"]
        return attr

    @staticmethod
    def transform_dataset(dataset: dict) -> dict:
        start = dataset['dataRevision']['temporalCoverageStart']
        stop = dataset['dataRevision']['temporalCoverageLatest']

        identifierVariables = Transformer.transform_identifiers(dataset, start, stop)
        dataset_measure, measureVariable = Transformer.transform_measure(dataset, start, stop)
        subjectFields = Transformer.transform_subject_fields(dataset_measure['subjectFields'])
        attributeVariables = Transformer.transform_attributes(dataset, start, stop)

        transformed = {'name': dataset['shortName'],
                       'populationDescription': Transformer.get_norwegian_text(dataset['populationDescription']),
                       'temporalCoverage': Transformer.get_temporal_coverage(dataset['dataRevision']),
                       'temporality': dataset['temporalityType'],
                       'identifierVariables': identifierVariables,
                       'measureVariable': measureVariable,
                       'subjectFields': subjectFields,
                       'languageCode': 'no'}
        if attributeVariables:
            transformed['attributeVariables'] = attributeVariables
        return transformed

    @staticmethod
    def transform_attributes(dataset, start, stop):
        attributeVariables = []
        if any(element['variableRole'] == 'START_TIME' for element in dataset["variables"]):
            attribute_start = \
                [variable for variable in dataset["variables"] if variable['variableRole'] == 'START_TIME'][0]
            attributeVariables.append(Transformer.get_attribute_variable(attribute_start, start, stop))
        if any(element['variableRole'] == 'STOP_TIME' for element in dataset["variables"]):
            attribute_stop = [variable for variable in dataset["variables"] if variable['variableRole'] == 'STOP_TIME'][
                0]
            attributeVariables.append(Transformer.get_attribute_variable(attribute_stop, start, stop))
        return attributeVariables

    @staticmethod
    def transform_measure(dataset, start, stop):
        dataset_measure = [variable for variable in dataset["variables"] if variable['variableRole'] == 'MEASURE'][0]
        measureVariable = {'name': dataset_measure['shortName'],
                           'label': Transformer.get_norwegian_text(dataset_measure['title']),
                           'dataType': dataset_measure['dataType'],
                           'representedVariables': Transformer.transform_represented_variables(dataset_measure, start, stop)}
        for representedVariable in measureVariable['representedVariables']:
            if representedVariable['description'] is None:
                representedVariable['description'] = Transformer.get_norwegian_text(dataset['description'])
        if 'unitType' in dataset.keys():
            measureVariable['keyType'] = Transformer.transform_unit_type(dataset["unitType"])
        return dataset_measure, measureVariable

    @staticmethod
    def transform_identifiers(dataset, start, stop):
        identifierVariables = []
        identifiers = [variable for variable in dataset["variables"] if variable['variableRole'] == 'IDENTIFIER']
        for identifier_instance in identifiers:
            identifierVariable = {
                'variableRole': 'Identifier',
                'name': identifier_instance['shortName'],
                'label': Transformer.get_norwegian_text(identifier_instance['title']),
                'dataType': identifier_instance['dataType'],
                'format': identifier_instance['format'],
                'representedVariables': Transformer.transform_represented_variables(identifier_instance, start, stop)
            }
            if 'unitType' in identifier_instance.keys():
                identifierVariable['keyType'] = Transformer.transform_unit_type(identifier_instance["unitType"])
            identifierVariables.append(identifierVariable)
        return identifierVariables
