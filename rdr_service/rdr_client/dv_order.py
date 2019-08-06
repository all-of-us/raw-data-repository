"""Simple client demonstrating how to create and retrieve a participant"""

import logging
import pprint

from rdr_service.main_util import configure_logging
from rdr_service.rdr_client.client import Client


def main():
    client = Client()
    body = {
        "authoredOn": "2019-02-12",
        "contained": [
            {"id": "supplier-1", "name": "GenoTek", "resourceType": "Organization"},
            {
                "deviceName": [{"name": "GenoTek DNA Kit", "type": "manufacturer-name"}],
                "id": "device-1",
                "identifier": [{"code": "4081", "system": "SKU"}, {"code": "SNOMED CODE TBD", "system": "SNOMED"}],
                "resourceType": "Device",
            },
            {
                "address": [
                    {
                        "city": "FakeVille",
                        "line": ["123 Fake St"],
                        "postalCode": "22155",
                        "state": "VA",
                        "type": "postal",
                        "use": "home",
                    }
                ],
                "id": "847299265",
                "identifier": [{"system": "participantId", "value": "847299265"}],
                "resourceType": "Patient",
            },
        ],
        "deliverFrom": {"reference": "#supplier-1"},
        "deliverTo": {"reference": "Patient/#patient-1"},
        "extension": [
            {"url": "http://vibrenthealth.com/fhir/barcode", "valueString": "AAAA20160121ZZZZ"},
            {"url": "http://vibrenthealth.com/fhir/order-type", "valueString": "salivary pilot"},
            {"url": "http://vibrenthealth.com/fhir/fulfillment-status", "valueString": "shipped"},
        ],
        "identifier": [{"code": "123", "system": "orderId"}, {"code": "B0A0A0A", "system": "fulfillmentId"}],
        "itemReference": {"reference": "#device-1"},
        "quantity": {"value": 1},
        "requester": {"reference": "Patient/patient-1"},
        "resourceType": "SupplyRequest",
        "status": "completed",
        "supplier": {"reference": "#supplier-1"},
        "text": {"div": "....", "status": "generated"},
    }
    response = client.request_json("Participant/P847299265/DvOrder/12347", "PUT", body)
    logging.info(pprint.pformat(response))


if __name__ == "__main__":
    configure_logging()
    main()
