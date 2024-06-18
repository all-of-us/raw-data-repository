SALIVA_DIET_SAMPLE = {
    "subject": "Patient/P100001",
    "identifier": [{
        "system": "http://www.pmi-ops.org/order-id",
        "value": "nph-order-id-123"
    }, {
        "system": "http://www.pmi-ops.org/sample-id",
        "value": "nph-sample-id-456"
    }, {
        "system": "http://www.pmi-ops.org/client-id",
        "value": "123456789"
    }],
    "createdInfo": {
        "author": {
            "system": "https://www.pmi-ops.org\/nph-username",
            "value": "test@example.com"
        },
        "site": {
            "system": "https://www.pmi-ops.org\/site-id",
            "value": "test-site-1"
        }
    },
    "collectedInfo": {
        "author": {
            "system": "https://www.pmi-ops.org\/nph-username",
            "value": "test@example.com"
        },
        "site": {
            "system": "https://www.pmi-ops.org\/site-id",
            "value": "test-site-1"
        }
    },
    "finalizedInfo": {
        "author": {
            "system": "https://www.pmi-ops.org\/nph-username",
            "value": "test@example.com"
        },
        "site": {
            "system": "https://www.pmi-ops.org\/site-id",
            "value": "test-site-1"
        }
    },
    "created": "2022-11-03T09:40:21Z",
    "module": "3",
    "visitType": "Diet",
    "timepoint": "Day 0",
    "sample": {
        "test": "Saliva",
        "description": "Saliva Sample",
        "collected": "2022-11-03T09:45:49Z",
        "finalized": "2022-11-03T10:55:41Z"
    },
    "aliquots": [
        {
            "id": "123",
            "identifier": "SALIVAA1",
            "container": "5mL Matrix tube (no glycerol)",
            "volume": "3.8",
            "description": "5mL Matrix tube",
            "collected": "2022-11-03T09:45:49Z",
            "units": "mL"
        }, {
            "id": "456",
            "identifier": "SALIVAA2",
            "container": "5mL Matrix tube (w/glycerol)",
            "volume": "3",
            "description": "5mL Matrix tube",
            "collected": "2022-11-03T09:45:49Z",
            "units": "mL",
            "glycerolAdditiveVolume": {
                "units": "uL",
                "volume": 1000
            }
        }
    ],
    "notes": {
        "collected": "Test notes 1",
        "finalized": "Test notes 2"
    }
}

URINE_DIET_SAMPLE = {
    "subject": "Patient/P124820391",
    "identifier": [{
        "system": "http://www.pmi-ops.org/order-id",
        "value": "nph-order-id-123"
    }, {
        "system": "http://www.pmi-ops.org/sample-id",
        "value": "nph-sample-id-456"
    }, {
        "system": "http://www.pmi-ops.org/client-id",
        "value": "123456789"
    }],
    "createdInfo": {
        "author": {
            "system": "https://www.pmi-ops.org\/nph-username",
            "value": "test@example.com"
        },
        "site": {
            "system": "https://www.pmi-ops.org\/site-id",
            "value": "test-site-1"
        }
    },
    "collectedInfo": {
        "author": {
            "system": "https://www.pmi-ops.org\/nph-username",
            "value": "test@example.com"
        },
        "site": {
            "system": "https://www.pmi-ops.org\/site-id",
            "value": "test-site-1"
        }
    },
    "finalizedInfo": {
        "author": {
            "system": "https://www.pmi-ops.org\/nph-username",
            "value": "test@example.com"
        },
        "site": {
            "system": "https://www.pmi-ops.org\/site-id",
            "value": "test-site-1"
        }
    },
    "created": "2022-11-03T09:40:21Z",
    "module": "3",
    "visitType": "OrangeDLW",
    "timepoint": "Day 0 Pre Dose A",
    "sample": {
        "test": "Urine",
        "description": "Urine Sample",
        "collected": "2022-11-03T09:45:49Z",
        "finalized": "2022-11-03T10:55:41Z",
        "dlwdose": {
            "batchid": "NPHDLW9172397",
            "participantweight": "56.38",
            "dose": "84",
            "calculateddose": "84.57",
            "dosetime": "2022-11-03T08:45:49Z"

        }
    },
    "aliquots": [{
        "id": "123",
        "identifier": "SALIVAA1",
        "container": "5mL Matrix tube (no glycerol)",
        "volume": "3.8",
        "description": "5mL Matrix tube",
        "collected": "2022-11-03T09:45:49Z",
        "units": "mL"
    }, {
        "id": "456",
        "identifier": "urineDlw",
        "container": "5mL Matrix tube (w/glycerol)",
        "volume": "3",
        "description": "5mL Matrix tube",
        "collected": "2022-11-03T09:45:49Z",
        "units": "mL"
    }],
    "notes": {
        "collected": "Test notes 1",
        "finalized": "Test notes 2"
    }
}

STOOL_DIET_SAMPLE = {
  "subject": "Patient/P124820391",
    "identifier": [{
      "system": "http://www.pmi-ops.org/order-id",
      "value": "nph-order-id-kit-12345678"
      },
      {
        "system": "http://www.pmi-ops.org/sample-id",
        "value": "nph-sample-id-kit-12345679"
      },
      {
        "system": "http://www.pmi-ops.org/client-id",
        "value": "123456789"
      }
    ],
    "createdInfo": {
        "author": {
            "system": "https://www.pmi-ops.org\/nph-username",
            "value": "test@example.com"
        },
        "site": {
            "system": "https://www.pmi-ops.org\/site-id",
            "value": "test-site-1"
        }
    },
    "collectedInfo": {
        "author": {
            "system": "https://www.pmi-ops.org\/nph-username",
            "value": "test@example.com"
        },
        "site": {
            "system": "https://www.pmi-ops.org\/site-id",
            "value": "test-site-1"
        }
    },
    "finalizedInfo": {
        "author": {
            "system": "https://www.pmi-ops.org\/nph-username",
            "value": "test@example.com"
        },
        "site": {
            "system": "https://www.pmi-ops.org\/site-id",
            "value": "test-site-1"
        }
    },
  "created": "2022-11-03T09:40:21Z",
  "module": "1",
  "visitType": "LMT",
  "timepoint": "Pre LMT",
  "sample": {
    "test": "ST1",
    "description": "95% Ethanol Tube 1",
    "collected": "2022-11-03T09:45:49Z",
    "finalized": "2022-11-03T10:55:41Z",
    "bowelMovement": "I was constipated (had difficulty passing stool), and my stool looks like Type 1 and/or 2",
    "bowelMovementQuality": "I tend to be constipated (have difficulty passing stool) - Type 1 and 2",
    "freezed": "2022-11-03T10:30:49Z"
  },
  "notes": {
    "collected": "Test notes 1",
    "finalized": "Test notes 2"
  }
}
