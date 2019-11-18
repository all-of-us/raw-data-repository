#
# # !!! This file is python 3.x compliant !!!
#
from collections import OrderedDict

import os

# path where temporary service account credential keys are stored
GCP_SERVICE_KEY_STORE = "{0}/.rdr/service-keys".format(os.path.expanduser("~"))

GCP_PROJECTS = [
    "all-of-us-rdr-prod",
    "all-of-us-rdr-stable",
    "all-of-us-rdr-staging",
    "all-of-us-rdr-sandbox",
    "pmi-drc-api-test",
    "all-of-us-rdr-careevo-test",
    "all-of-us-rdr-ptsc-1-test",
    "all-of-us-rdr-ptsc-2-test",
    "all-of-us-rdr-ptsc-3-test",
]

GCP_INSTANCES = {
    "all-of-us-rdr-prod": "all-of-us-rdr-prod:us-central1:rdrmaindb",
    "all-of-us-rdr-stable": "all-of-us-rdr-stable:us-central1:rdrmaindb",
    "all-of-us-rdr-staging": "all-of-us-rdr-staging:us-central1:rdrmaindb",
    "all-of-us-rdr-sandbox": "all-of-us-rdr-sandbox:us-central1:rdrmaindb",
    "pmi-drc-api-test": "pmi-drc-api-test:us-central1:rdrmaindb",
    "all-of-us-rdr-careevo-test": "all-of-us-rdr-careevo-test:us-central1:rdrmaindb",
    "all-of-us-rdr-ptsc-1-test": "all-of-us-rdr-ptsc-1-test:us-central1:rdrmaindb",
    "all-of-us-rdr-ptsc-2-test": "all-of-us-rdr-ptsc-2-test:us-central1:rdrmaindb",
    "all-of-us-rdr-ptsc-3-test": "all-of-us-rdr-ptsc-3-test:us-central1:rdrmaindb",

}

GCP_REPLICA_INSTANCES = {
    "all-of-us-rdr-prod": "all-of-us-rdr-prod:us-central1:rdrbackupdb-a",
    "all-of-us-rdr-stable": "all-of-us-rdr-stable:us-central1:rdrbackupdb",
    "all-of-us-rdr-staging": "all-of-us-rdr-staging:us-central1:rdrbackupdb",
    "all-of-us-rdr-sandbox": "all-of-us-rdr-sandbox:us-central1:rdrmaindb",
    "pmi-drc-api-test": "pmi-drc-api-test:us-central1:rdrbackupdb",
    "all-of-us-rdr-careevo-test": "all-of-us-rdr-careevo-test:us-central1:rdrbackupdb",
    "all-of-us-rdr-ptsc-1-test": "all-of-us-rdr-ptsc-1-test:us-central1:rdrbackupdb",
    "all-of-us-rdr-ptsc-2-test": "all-of-us-rdr-ptsc-2-test:us-central1:rdrbackupdb",
    "all-of-us-rdr-ptsc-3-test": "all-of-us-rdr-ptsc-3-test:us-central1:rdrbackupdb",
}

GCP_SERVICES = [
    'default',
    'offline',
]

# Map project id to App config files.
GCP_APP_CONFIG_MAP = OrderedDict({
    "all-of-us-rdr-prod": "config_prod.json",
    "all-of-us-rdr-stable": "config_stable.json",
    "all-of-us-rdr-staging": "config_staging.json",
    "all-of-us-rdr-sandbox": "config_sandbox.json",
    "pmi-drc-api-test": "config_test.json",
    "all-of-us-rdr-careevo-test": "config_test_careevo.json",
    "all-of-us-rdr-ptsc-1-test": "config_test_ptsc_1.json",
    "all-of-us-rdr-ptsc-2-test": "config_test_ptsc_2.json",
    "all-of-us-rdr-ptsc-3-test": "config_test_ptsc_3.json",
})

# Map GCP app service to configuration yaml files.
GCP_SERVICE_CONFIG_MAP = OrderedDict({
    'prod': {
        'default': {
            'type': 'service',
            'config_file': "app.yaml",
            'default': [
                'rdr_service/app_base.yaml',
                'rdr_service/app_prod.yaml'
            ]
        },
        'offline': {
            'type': 'service',
            'default': [
                'rdr_service/offline.yaml'
            ]
        },
        'cron': {
            'type': 'config',
            'default': [
                'rdr_service/cron_default.yaml',
                'rdr_service/cron_prod.yaml'
            ]
        },
        'queue': {
            'type': 'config',
            'default': [
                'rdr_service/queue.yaml'
            ]
        },
        'index': {
            'type': 'config',
            'default': [
                'rdr_service/index.yaml'
            ]
        }
    },
    'nonprod': {
        'default': {
            'type': 'service',
            'config_file': "app.yaml",
            'default': [
                'rdr_service/app_base.yaml',
                'rdr_service/app_nonprod.yaml'
            ],
        },
        'offline': {
            'type': 'service',
            'default': [
                'rdr_service/offline.yaml'
            ]
        },
        'cron': {
            'type': 'config',
            'default': [
                'rdr_service/cron_default.yaml',
            ],
            'careevo': [
                'rdr_service/cron_default.yaml',
                'rdr_service/cron_careevo.yaml'
            ],
            'ptsc': [
                'rdr_service/cron_default.yaml',
                'rdr_service/cron_ptsc.yaml'
            ],
            'sandbox': [
                'rdr_service/cron_default.yaml',
                'rdr_service/cron_sandbox.yaml'
            ]
        },
        'queue': {
            'type': 'config',
            'default': [
                'rdr_service/queue.yaml'
            ]
        },
        'index': {
            'type': 'config',
            'default': [
                'rdr_service/index.yaml'
            ]
        }
    }
})