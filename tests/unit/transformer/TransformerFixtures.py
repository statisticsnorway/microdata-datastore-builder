from datetime import datetime

name_title_description = {
    "name": "PERSON",
    "title": [
        {
            "languageCode": "no",
            "value": "Menneske"
        },
        {
            "languageCode": "en",
            "value": "Person"
        }
    ],
    "description": [
        {
            "languageCode": "no",
            "value": "Statistisk enhet er person (individ, enkeltmenneske)."
        },
        {
            "languageCode": "en",
            "value": "Statistical unit is person"
        }
    ]
}

expected_name_title_description = {
    "name": "PERSON",
    "label": "Menneske",
    "description": "Statistisk enhet er person (individ, enkeltmenneske)."
}

dataset = {
    "name": "TEST_PERSON_INCOME",
    "title": [
        {"languageCode": "no", "value": "Inntekt"},
        {"languageCode": "en", "value": "Income"}
    ],
    "description": [
        {"languageCode": "no", "value": "Personinntekt."},
        {"languageCode": "en", "value": "Personal income."}
    ],
    "populationDescription": [
        {"languageCode": "no", "value": "Alle personer med inntekt."},
        {"languageCode": "en", "value": "All persons with income."}
    ],
    "temporalityType": "ACCUMULATED",
    "spatialCoverageDescription": [
        {"languageCode": "no", "value": "Norge"},
        {"languageCode": "en", "value": "Norway"}
    ],
    "dataRevision": {
        "version": "1.0.0",
        "releaseStatus": "RELEASED",
        "operationType": "ADD",
        "releaseDate": "2020-03-25",
        "description": [
            {"languageCode": "no", "value": "Første versjon av datasettet."},
            {"languageCode": "en", "value": "The initial version of this dataset."}
        ],
        "temporalCoverageStart": "2016-01-01",
        "temporalEndOfSeries": False,
        "temporalCoverageLatest": "2019-12-31"
    },
    "measure": {
        "name": "TEST_PERSON_INCOME",
        "uriDefinition": ["https://data.skatteetaten.no/begrep/personinntekt%20fra%20l%C3%B8nnsinntekt"],
        "dataType": "LONG",
        "subjectField": [
            {
                "name": "INCOME_PROPERTY_TAX",
                "title": [
                    {"languageCode": "no", "value": "Inntekt"},
                    {"languageCode": "en", "value": "Income"}
                ],
                "description": [
                    {"languageCode": "no", "value": "Innekt, formue, skatt"},
                    {"languageCode": "en", "value": "Income, property, taxes"}
                ]
            }
        ],
        "valueDomain": {
            "name": "INCOME",
            "title": [
                {"languageCode": "no", "value": "Personinntekt i norske kroner (NOK)."},
                {"languageCode": "en", "value": "Personal income in Norwegian kroner (NOK)."}
            ],
            "uriDefinition": [None]
        }
    },
    "identifier": [
        {
            "name": "PERSON_ID_1",
            "title": [
                {"languageCode": "no", "value": "Personidentifikator"},
                {"languageCode": "en", "value": "National identity number"}
            ],
            "description": [
                {"languageCode": "no", "value": "Identifikator for person"},
                {"languageCode": "en", "value": "Persons national identity number"}
            ],
            "format": "RandomUInt48",
            "unitType": {
                "name": "PERSON",
                "title": [
                    {"languageCode": "no", "value": "Person"},
                    {"languageCode": "en", "value": "Person"}
                ],
                "description": [
                    {"languageCode": "no", "value": "Statistisk enhet er person (individ, enkeltmenneske)."},
                    {"languageCode": "en", "value": "Statistical unit is person"}
                ]
            },
            "valueDomain": {
                "name": "FOEDSELSNUMMER",
                "title": [
                    {"languageCode": "no", "value": "Pseudonymisert fødselsnummer"},
                    {"languageCode": "en", "value": "Pseudonym for persons national identity number"}
                ],
                "uriDefinition": [
                    "http://www.ssb.no/a/metadata/conceptvariable/vardok/26/nb",
                    "https://www.ssb.no/a/metadata/conceptvariable/vardok/26/en"
                ],
                "measurementUnitDescription": [
                    {"languageCode": "no", "value": "N/A"},
                    {"languageCode": "en", "value": "N/A"}
                ]
            }
        }
    ],
    "attribute": [
        {
            "name": "START",
            "title": [
                {"languageCode": "no", "value": "Startdato"},
                {"languageCode": "en", "value": "Start date"}
            ],
            "description": [
                {"languageCode": "no", "value": "Startdato/måletidspunktet for hendelsen"},
                {"languageCode": "en", "value": "Event start date"}
            ],
            "dataType": "DATE",
            "attributeType": "START",
            "valueDomain": {
                "name": "START_DATE_EVENT",
                "title": [
                    {"languageCode": "no", "value": "Startdato"},
                    {"languageCode": "en", "value": "Start date"}
                ],
                "uriDefinition": [None],
                "measurementUnitDescription": [
                    {"languageCode": "no", "value": "N/A"},
                    {"languageCode": "en", "value": "N/A"}
                ]
            }
        },
        {
            "name": "STOP",
            "title": [
                {"languageCode": "no", "value": "Stoppdato"},
                {"languageCode": "en", "value": "Stop date"}
            ],
            "description": [
                {"languageCode": "no", "value": "Stoppdato/sluttdato for hendelsen"},
                {"languageCode": "en", "value": "Event stop/end date"}
            ],
            "dataType": "DATE",
            "attributeType": "STOP",
            "valueDomain": {
                "name": "STOP_DATE_EVENT",
                "title": [
                    {"languageCode": "no", "value": "Stoppdato/sluttdato"},
                    {"languageCode": "en", "value": "Stop/end date"}
                ],
                "uriDefinition": [None],
                "measurementUnitDescription": [
                    {"languageCode": "no", "value": "N/A"},
                    {"languageCode": "en", "value": "N/A"}
                ]
            }
        }
    ]
}

expected_identifier = {
    "name": "PERSON_ID_1",
    "label": "Personidentifikator",
    "dataType": "Long",
    "representedVariables": [
        {
            "validPeriod": {
                "start": 16801,
                "stop": 18261
            },
            "valueDomain": {
                "description": "N/A",
                "unitOfMeasure": "N/A"
            },
            "description": "Identifikator for person"
        }
    ],
    "keyType": {
        "name": "PERSON",
        "label": "Person",
        "description": "Statistisk enhet er person (individ, enkeltmenneske)."
    },
    "format": "RandomUInt48",
    "variableRole": "Identifier"
}

valuedomain_without_codelist = {
        "name": "FOEDSELSNUMMER",
        "title": [
            {"languageCode": "no", "value": "Pseudonymisert fødselsnummer"},
            {"languageCode": "en", "value": "Pseudonym for persons national identity number"}
        ],
        "uriDefinition": [
            "http://www.ssb.no/a/metadata/conceptvariable/vardok/26/nb",
            "https://www.ssb.no/a/metadata/conceptvariable/vardok/26/en"
        ],
        "measurementUnitDescription": [
            {"languageCode": "no", "value": "N/A"},
            {"languageCode": "en", "value": "N/A"}
        ]
}

expected_valuedomain_without_codelist = {
    'description': 'N/A',
    'unitOfMeasure': 'N/A'
}

valuedomain_with_codelist_same_start_date = {
        "name": "PET_TYPE",
        "title": [
            {"languageCode": "no", "value": "Type kjæledyr"},
            {"languageCode": "en", "value": "Type of pet"}
        ],
        "description": [
            {"languageCode": "no", "value": "Type kjæledyr vi selger i butikken"},
            {"languageCode": "en", "value": "Type of pet in our petshop"}
        ],
        "codeList": {
            "name": "PET_TYPE",
            "topLevelCodeItems": [
                {
                    "code": "CAT",
                    "categoryTitle": [
                        {"languageCode": "no", "value": "Katt"},
                        {"languageCode": "en", "value": "Cat"}
                    ],
                    "validityPeriodStart": "2010-01-01"
                },
                {
                    "code": "DOG",
                    "categoryTitle": [
                        {"languageCode": "no", "value": "Hund"},
                        {"languageCode": "en", "value": "Dog"}
                    ],
                    "validityPeriodStart": "2010-01-01"
                },
                {
                    "code": "FISH",
                    "categoryTitle": [
                        {"languageCode": "no", "value": "Fisk"},
                        {"languageCode": "en", "value": "Fish"}
                    ],
                    "validityPeriodStart": "2010-01-01"
                }
            ]
        }
    }

expected_valuedomain_with_codelist_same_start_date = {
        "description": "Type kjæledyr vi selger i butikken",
        "validPeriod": {
            "start": "2010-01-01"
        },
        "valueDomain": {
            "codeList": [
                {
                    "category": "Katt",
                    "code": "CAT"
                },
                {
                    "category": "Hund",
                    "code": "DOG"
                },
                {
                    "category": "Fisk",
                    "code": "FISH"
                }
            ],
            "missingValues": [
            ]
        }
    }

valuedomain_with_codelist_different_start_dates = {
        "name": "PET_TYPE",
        "title": [
            {"languageCode": "no", "value": "Type kjæledyr"},
            {"languageCode": "en", "value": "Type of pet"}
        ],
        "description": [
            {"languageCode": "no", "value": "Type kjæledyr vi selger i butikken"},
            {"languageCode": "en", "value": "Type of pet in our petshop"}
        ],
        "codeList": {
            "name": "PET_TYPE",
            "topLevelCodeItems": [
                {
                    "code": "CAT",
                    "categoryTitle": [
                        {"languageCode": "no", "value": "Katt"},
                        {"languageCode": "en", "value": "Cat"}
                    ],
                    "validityPeriodStart": "2010-01-01"
                },
                {
                    "code": "DOG",
                    "categoryTitle": [
                        {"languageCode": "no", "value": "Hund"},
                        {"languageCode": "en", "value": "Dog"}
                    ],
                    "validityPeriodStart": "2012-01-01"
                },
                {
                    "code": "FISH",
                    "categoryTitle": [
                        {"languageCode": "no", "value": "Fisk"},
                        {"languageCode": "en", "value": "Fish"}
                    ],
                    "validityPeriodStart": "2008-10-01"
                },
                {
                    "code": "BIRD",
                    "categoryTitle": [
                        {"languageCode": "no", "value": "Fugl"},
                        {"languageCode": "en", "value": "Bird"}
                    ],
                    "validityPeriodStart": "2010-01-01"
                },
                {
                    "code": "RABBIT",
                    "categoryTitle": [
                        {"languageCode": "no", "value": "Kanin"},
                        {"languageCode": "en", "value": "Rabbit"}
                    ],
                    "validityPeriodStart": "2011-06-01"
                }
            ]
        }
    }

expected_valuedomain_with_codelist_different_start_dates = [
        {
            "description": "Type kjæledyr vi selger i butikken",
            "validPeriod": {
                "start": datetime.strptime('2008-10-01', '%Y-%m-%d'),
                "stop": datetime.strptime('2009-12-31', '%Y-%m-%d')
            },
            "valueDomain": {
                "codeList": [
                    {
                        "category": "Fisk",
                        "code": "FISH"
                    }
                ],
                "missingValues": [
                ]
            }
        },
        {
            "description": "Type kjæledyr vi selger i butikken",
            "validPeriod": {
                "start": datetime.strptime('2010-01-01', '%Y-%m-%d'),
                "stop": datetime.strptime('2011-05-31', '%Y-%m-%d')
            },
            "valueDomain": {
                "codeList": [
                    {
                        "category": "Fisk",
                        "code": "FISH"
                    },
                    {
                        "category": "Katt",
                        "code": "CAT"
                    },
                    {
                        "category": "Fugl",
                        "code": "BIRD"
                    }
                ],
                "missingValues": [
                ]
            }
        },
        {
            "description": "Type kjæledyr vi selger i butikken",
            "validPeriod": {
                "start": datetime.strptime('2011-06-01', '%Y-%m-%d'),
                "stop": datetime.strptime('2011-12-31', '%Y-%m-%d')
            },
            "valueDomain": {
                "codeList": [
                    {
                        "category": "Fisk",
                        "code": "FISH"
                    },
                    {
                        "category": "Katt",
                        "code": "CAT"
                    },
                    {
                        "category": "Fugl",
                        "code": "BIRD"
                    },
                    {
                        "category": "Kanin",
                        "code": "RABBIT"
                    }
                ],
                "missingValues": [
                ]
            }
        },
        {
            "description": "Type kjæledyr vi selger i butikken",
            "validPeriod": {
                "start": datetime.strptime('2012-01-01', '%Y-%m-%d')
            },
            "valueDomain": {
                "codeList": [
                    {
                        "category": "Fisk",
                        "code": "FISH"
                    },
                    {
                        "category": "Katt",
                        "code": "CAT"
                    },
                    {
                        "category": "Fugl",
                        "code": "BIRD"
                    },
                    {
                        "category": "Kanin",
                        "code": "RABBIT"
                    },
                    {
                        "category": "Hund",
                        "code": "DOG"
                    }
                ],
                "missingValues": [
                ]
            }
        }
]

valuedomain_with_codelist_different_start_and_stop_dates = {
    "name": "PET_TYPE",
    "title": [
        {"languageCode": "no", "value": "Type kjæledyr"},
        {"languageCode": "en", "value": "Type of pet"}
    ],
    "description": [
        {"languageCode": "no", "value": "Type kjæledyr vi selger i butikken"},
        {"languageCode": "en", "value": "Type of pet in our petshop"}
    ],
    "codeList": {
        "name": "PET_TYPE",
        "topLevelCodeItems": [
            {
                "code": "CAT",
                "categoryTitle": [
                    {"languageCode": "no", "value": "Katt"},
                    {"languageCode": "en", "value": "Cat"}
                ],
                "validityPeriodStart": "2010-01-01"
            },
            {
                "code": "DOG",
                "categoryTitle": [
                    {"languageCode": "no", "value": "Hund"},
                    {"languageCode": "en", "value": "Dog"}
                ],
                "validityPeriodStart": "2012-01-01"
            },
            {
                "code": "FISH",
                "categoryTitle": [
                    {"languageCode": "no", "value": "Fisk"},
                    {"languageCode": "en", "value": "Fish"}
                ],
                "validityPeriodStart": "2008-10-01"
            },
            {
                "code": "BIRD",
                "categoryTitle": [
                    {"languageCode": "no", "value": "Fugl"},
                    {"languageCode": "en", "value": "Bird"}
                ],
                "validityPeriodStart": "2010-01-01",
                "validityPeriodStop": "2011-01-01"
            },
            {
                "code": "RABBIT",
                "categoryTitle": [
                    {"languageCode": "no", "value": "Kanin"},
                    {"languageCode": "en", "value": "Rabbit"}
                ],
                "validityPeriodStart": "2011-06-01"
            }
        ]
    }
}

expected_valuedomain_with_codelist_different_start_and_stop_dates = [
    {
        "description": "Type kjæledyr vi selger i butikken",
        "validPeriod": {
            "start": "2008-10-01",
            "stop": "2009-12-31"
        },
        "valueDomain": {
            "codeList": [
                {
                    "category": "Fisk",
                    "code": "FISH"
                }
            ],
            "missingValues": [
            ]
        }
    },
    {
        "description": "Type kjæledyr vi selger i butikken",
        "validPeriod": {
            "start": "2010-01-01",
            "stop": "2010-12-31"
        },
        "valueDomain": {
            "codeList": [
                {
                    "category": "Fisk",
                    "code": "FISH"
                },
                {
                    "category": "Katt",
                    "code": "CAT"
                },
                {
                    "category": "Fugl",
                    "code": "BIRD"
                }
            ],
            "missingValues": [
            ]
        }
    },
    {
        "description": "Type kjæledyr vi selger i butikken",
        "validPeriod": {
            "start": "2011-01-01",
            "stop": "2011-05-31"
        },
        "valueDomain": {
            "codeList": [
                {
                    "category": "Fisk",
                    "code": "FISH"
                },
                {
                    "category": "Katt",
                    "code": "CAT"
                }
            ],
            "missingValues": [
            ]
        }
    },
    {
        "description": "Type kjæledyr vi selger i butikken",
        "validPeriod": {
            "start": "2011-06-01",
            "stop": "2011-12-31"
        },
        "valueDomain": {
            "codeList": [
                {
                    "category": "Fisk",
                    "code": "FISH"
                },
                {
                    "category": "Katt",
                    "code": "CAT"
                },
                {
                    "category": "Kanin",
                    "code": "RABBIT"
                }
            ],
            "missingValues": [
            ]
        }
    },
    {
        "description": "Type kjæledyr vi selger i butikken",
        "validPeriod": {
            "start": "2012-01-01"
        },
        "valueDomain": {
            "codeList": [
                {
                    "category": "Fisk",
                    "code": "FISH"
                },
                {
                    "category": "Katt",
                    "code": "CAT"
                },
                {
                    "category": "Kanin",
                    "code": "RABBIT"
                },
                {
                    "category": "Hund",
                    "code": "DOG"
                }
            ],
            "missingValues": [
            ]
        }
    }
]