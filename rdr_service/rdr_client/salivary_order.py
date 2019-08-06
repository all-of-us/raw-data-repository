"""
Creates a POST or PUT request on SupplyDelivery or SupplyRequest.
"""
import json
import logging
import pprint

from rdr_service.fhir_utils import SimpleFhirR4Reader
from rdr_service.main_util import configure_logging, get_parser
from rdr_service.rdr_client.client import Client, client_log


def make_request_body(payload):
    with open(payload, "r") as payload:
        request = json.load(payload)
    pprint.pprint(request)
    return request


def main(client):
    request = make_request_body(client.args.file)

    resource = SimpleFhirR4Reader(request)

    if not client.args.endpoint:
        path = "SupplyRequest"
    else:
        path = client.args.endpoint

    if path == "SupplyRequest":
        order_id = resource.identifier.get(system="http://joinallofus.org/fhir/orderId").value
    else:
        order_id = resource.basedOn[0].identifier.value  # get(system='http://joinallofus.org/fhir/orderId').value

    if not client.args.verb:
        verb = "PUT"
    else:
        verb = client.args.verb

    if verb == "PUT":
        path = path + "/{}".format(order_id)

    response = client.request_json(path, verb, request, check_status=False)
    pprint.pprint(response)


if __name__ == "__main__":
    configure_logging()
    client_log.setLevel(logging.WARN)  # Suppress the log of HTTP requests.
    arg_parser = get_parser()
    arg_parser.add_argument("--file", help="The JSON file with payload.", required=True)
    arg_parser.add_argument("--endpoint", help="Should be SupplyRequest or SupplyDelivery, defaults to SupplyRequest")
    arg_parser.add_argument("--verb", help="PUT or POST, defaults to PUT")

    main(Client(parser=arg_parser))
