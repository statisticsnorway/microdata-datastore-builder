
### Insert new Python dictionary elements to configure a new Data Store ###

datastore = {
    "TEST": {
        "datastoreDomainName": "no.ssb.test",
        "datastoreName": "SSB test",
        "defaultLanguage": "no",
        "datastorePath": "C:/BNJ/prosjektutvikling/GitHub/statisticsnorway/microdata-datastore-builder/tests/resources/TestDataStore/no_ssb_test/"
    },
    "QA": {
        "datastoreDomainName": "no.ssb.qa",
        "datastoreName": "SSB QA",
        "defaultLanguage": "no",
        "datastorePath": "/path/no_ssb_qa/"
    },
    "PROD": {
        "datastoreDomainName": "no.ssb.prod",
        "datastoreName": "SSB prod",
        "defaultLanguage": "no",
        "datastorePath": "/path/no_ssb_prod/"
    }
}
