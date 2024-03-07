"""Assigns participants with the specified IDs to the organization.

Usage:
./rdr_client/run_client.sh --project all-of-us-rdr-prod --account $USER@pmi-ops.org \
  pairing_assigner.py file.csv --pairing [site|organization|awardee|biospecimen|physical_measurements] \
  [--dry_run] [--override_site] [--finalization]

Where site = google_group, organization = external_id, awardee = name.

The CSV contains lines with P12345678,NEW_ORGANIZATION (no headers necessary). e.g.:
Example awardees:
  P11111111,AZ_TUCSON
  P22222222,AZ_TUCSON
  P99999999,PITT
  P00000000,PITT

Example sites:
  P11111111,hpo-site-monroeville
  P22222222,hpo-site-phoenix
  P99999999,hpo-site-tucson
  P00000000,hpo-site-pitt
"""

import csv
import logging
import sys

from rdr_service.main_util import configure_logging, get_parser
from rdr_service.rdr_client.client import Client, HttpException, client_log
from rdr_service.dao.physical_measurements_dao import _CREATED_LOC_EXTENSION, _FINALIZED_LOC_EXTENSION

def main(client):
    num_no_change = 0
    num_updates = 0
    num_errors = 0
    p_pair_list = ["site", "organization", "awardee"]
    ps_pair_list = ["biospecimen", "physical_measurements"]
    biospecimen_fields = [
        "createdInfo",
        "collectedInfo",
        "processedInfo"
    ]
    pm_fields = [
        "createdSiteId",
    ]
    pm_sync = False
    biospecimen_sync = False

    if client.args.finalization:
        biospecimen_fields.append("finalizedInfo")
        pm_fields.append("finalizedSiteId")

    pairing_key = client.args.pairing

    if client.args.pairing not in p_pair_list and client.args.pairing not in ps_pair_list:
        sys.exit("Pairing must be one of site|organization|awardee|biospecimen|physical_measurements")

    if client.args.pairing == "physical_measurements":
        pm_sync = True
        pairing_list = pm_fields
    elif client.args.pairing == "biospecimen":
        biospecimen_sync = True
        pairing_list = biospecimen_fields
    else:
        pairing_list = [pairing_key]

    with (open(client.args.file) as csvfile):
        reader = csv.reader(csvfile)
        for line in reader:
            try:
                participant_id, new_pairing = [v.strip() for v in line]
            except ValueError as e:
                logging.error("Skipping invalid line %d (parsed as %r): %s.", reader.line_num, line, e)
                num_errors += 1
                continue

            if not (new_pairing and participant_id):
                logging.warning(
                    "Skipping invalid line %d: missing new_pairing (%r) or participant (%r).",
                    reader.line_num,
                    new_pairing,
                    participant_id,
                )
                num_errors += 1
                continue

            if not participant_id.startswith("P"):
                logging.error(
                    "Malformed participant ID from line %d: %r does not start with P.",
                    reader.line_num,
                    participant_id
                )
                num_errors += 1
                continue

            if biospecimen_sync:
                request_url = "Participant/%s/BiobankOrder" % participant_id
            elif pm_sync:
                request_url = "Participant/%s/PhysicalMeasurements" % participant_id
            else:
                request_url = "Participant/%s" % participant_id

            try:
                participant = client.request_json(request_url)
            except HttpException as e:
                logging.error("Skipping %s: %s", participant_id, e)
                num_errors += 1
                continue

            old_pairing = ""
            if pm_sync:
                for entry in participant['entry'][0]['resource']['entry']:
                    if entry['resource']['resourceType'] == 'Composition':
                        for extension in entry['resource']['extension']:
                            if extension.get('url', "") == _CREATED_LOC_EXTENSION:
                                old_pairing = extension.get('valueString', extension['valueReference']).split('/')[1]
                            break
                        break
            elif biospecimen_sync:
                old_pairing = participant['data'][0][pairing_list[0]]['site']['value']
            else:
                old_pairing = _get_old_pairing(participant, pairing_key)

            if new_pairing == old_pairing:
                num_no_change += 1
                logging.info("%s unchanged (already %s)", participant_id, old_pairing)
                continue

            if not client.args.override_site:
                if participant.get("site") and participant["site"] != "UNSET":
                    logging.info(
                        "Skipping participant %s already paired with site %s" % (participant_id, participant["site"])
                    )
                    continue

            if client.args.no_awardee_change:
                if participant.get("awardee") and participant["awardee"] != "UNSET":
                    if not new_pairing.startswith(participant["awardee"]):
                        logging.info(
                            "Skipping participant %s where pairing %s does not begin with old awardee %s"
                            % (participant_id, new_pairing, participant["awardee"])
                        )
                        continue

            logging.info("%s %s => %s", participant_id, old_pairing, new_pairing)

            if new_pairing == "UNSET":
                for i in pairing_list:
                    participant[i] = "UNSET"
                participant["providerLink"] = []
            else:
                if biospecimen_sync:
                    for order in participant['data']:
                        order['status'] = "re-pairing"
                        for i in pairing_list:
                            order[i]['site']['value'] = new_pairing
                elif pm_sync:
                    for result in participant['entry']:
                        result['resource']['status'] = 're-pairing'
                        for entry in result['resource']['entry']:
                            if entry['resource']['resourceType'] == 'Composition':
                                for extension in entry['resource']['extension']:
                                    if (extension.get('url', "") == _CREATED_LOC_EXTENSION or
                                            extension.get('url', "") == _FINALIZED_LOC_EXTENSION):
                                        if 'valueString' in extension:
                                            extension['valueString'] = 'Location/%s' % new_pairing
                                        elif 'valueReference' in extension:
                                            extension['valueReference'] = 'Location/%s' % new_pairing
                else:
                    for i in pairing_list:
                        del participant[i]
                    participant[pairing_key] = new_pairing

            if client.args.dry_run and client.args.pairing in pairing_list:
                logging.info("Dry run, would update participant[%r] to %r.", pairing_key, new_pairing)
            elif client.args.dry_run and biospecimen_sync:
                for i in range(len(pairing_list)):
                    logging.info("Dry run, would update biobank_order[%r] to %r", pairing_list[i], new_pairing)
            elif client.args.dry_run and pm_sync:
                for i in range(len(pairing_list)):
                    logging.info("Dry run, would update physical_measurements[%r] to %r", pairing_list[i], new_pairing)
            else:
                if biospecimen_sync:
                    for resource in participant['data']:
                        id_url = request_url + '/%s' % resource['id']
                        client.request_json(id_url)
                        client.request_json(id_url, "PATCH", resource, headers={"If-Match": client.last_etag})
                elif pm_sync:
                    for entry in participant['entry']:
                        id_url = entry['fullUrl'].split('rdr/v1/')[1]
                        client.request_json(id_url)
                        client.request_json(id_url, "PATCH", entry['resource'], headers={"If-Match": client.last_etag})
                else:
                    client.request_json(
                        request_url, "PUT", participant, headers={"If-Match": client.last_etag}
                    )
            num_updates += 1
    logging.info(
        "%s %d participants, %d unchanged, %d errors.",
        "Would update" if client.args.dry_run else "Updated",
        num_updates,
        num_no_change,
        num_errors,
    )


def _get_old_pairing(participant, pairing_key):
    old_pairing = participant[pairing_key]
    if not old_pairing:
        return "UNSET"
    return old_pairing


if __name__ == "__main__":
    configure_logging()
    client_log.setLevel(logging.WARN)  # Suppress the log of HTTP requests.
    arg_parser = get_parser()
    arg_parser.add_argument("file", help="The name of file containing the list of HPOs and participant IDs")
    arg_parser.add_argument("--dry_run", action="store_true")
    arg_parser.add_argument(
        "--pairing",
        help="set level of pairing as one of" "[site|organization|awardee|biospecimen|physical_measurements]",
        required=True
    )
    arg_parser.add_argument(
        "--override_site", help="Update pairings on participants that have a site pairing already", action="store_true"
    )
    arg_parser.add_argument(
        "--no_awardee_change",
        help="Do not re-pair participants if the awardee is changing; " + "just log that it happened",
        action="store_true",
    )
    arg_parser.add_argument(
        "--finalization",
        help="Attempt to re-pair the finalization site of the given field(biospecimen or physical_measurements only",
        action="store_true"
    )
    main(Client(parser=arg_parser))
