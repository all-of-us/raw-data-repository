"""SupplyRequest - Order SHIPPED
{
    "resourceType": "SupplyRequest",
    "id": @TODO: generate unique ID and return for PTSC
    "text": {
        "status": "generated",
        "div": "...."
    },
    "contained": [
        {
            "resourceType": "Organization",
            "id": "supplier-1",
            "name": "GenoTek"
        },
        {
            "resourceType": "Device",
            "id": "device-1",
            "identifier": [
                {
                    "system": "SKU",
                    "code": "4081"
                },
                {
                    "system": "SNOMED",
                    "code": "SNOMED CODE TBD"
                }
            ],
            "deviceName": [
                {
                    "name": "GenoTek DNA Kit",
                    "type": "manufacturer-name"
                }
            ]
        },
        {
                    "resourceType": "Patient",
                    "id": "patient-1",
                    "identifier": [
                        {
                            "system": "participantId",
                            "value": "123456"
                        }
                    ],
                    "address": [
                        {
                            "use": "home",
                            "type": "postal",
                            "line": [
                                "123 Fake St"
                            ],
                            "city": "FakeVille",
                            "state": "VA",
                            "postalCode": "22155",
                        },
                    ]
            }

    ],
    "identifier": [
        {
            "system": "orderId",
            "code": "123"
        },
        {
            "system": "fulfillmentId",
            "code": "B0A0A0A"
        }
    ],
    "status": "completed",
    "itemReference": {
        "reference": "#device-1"
    },
    "quantity": {
        "value": 1
    },
    "authoredOn": "2019-02-12",
    "requester": {
        "reference": "Patient/patient-1"
    },
    "supplier": {
        "reference": "#supplier-1"
    },
    "deliverFrom": {
        "reference": "#supplier-1"
    },
    "deliverTo": {
        "reference": "Patient/#patient-1"
    },
    "extension": [
                {
            "url": "http://vibrenthealth.com/fhir/barcode",
            "valueString": "AAAA20160121ZZZZ"
                },
        {
            "url": "http://vibrenthealth.com/fhir/order-type",
            "valueString": "Salivary Pilot"
        },
        {
            "url": "http://vibrenthealth.com/fhir/fullfilment-status",
            "valueString": "shipped"
        }
    ]
}
"""
