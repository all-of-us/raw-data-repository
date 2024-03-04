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
from rdr_service.dao.physical_measurements_dao import PhysicalMeasurementsDao

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

    with open(client.args.file) as csvfile:
        reader = csv.reader(csvfile)
        for line in reader:
            try:
                args_list = [v.strip() for v in line]
                participant_id = args_list[0]
                if len(args_list) > 2 and (biospecimen_sync or pm_sync):
                    new_pairing = args_list[2:]
                    obj_id = args_list[1]
                else:
                    new_pairing = [args_list[1]]
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

            if not (len(new_pairing) == len(pairing_list)):
                logging.warning(
                    "Skipping invalid line %d: invalid number of values provided %d. Expected %d",
                    reader.line_num,
                    len(new_pairing),
                    len(pairing_list)
                )
                num_errors += 1
                continue

            if not participant_id.startswith("P"):
                if not biospecimen_sync:
                    logging.error(
                        "Malformed participant ID from line %d: %r does not start with P.",
                        reader.line_num,
                        participant_id
                    )
                    num_errors += 1
                    continue

            if biospecimen_sync:
                request_url = "Participant/%s/BiobankOrder/%s" % (participant_id, obj_id)
            elif pm_sync:
                request_url = "Participant/%s/PhysicalMeasurements/%s" % (participant_id, obj_id)
            else:
                request_url = "Participant/%s" % participant_id

            try:
                participant = client.request_json(request_url)
            except HttpException as e:
                logging.error("Skipping %s: %s", participant_id, e)
                num_errors += 1
                continue

            print(participant)
            old_pairing = []
            if pm_sync:
                for i in range(len(participant['entry'])):
                    if participant['entry'][i]['resource']['resourceType'] == 'Composition':
                        for j in range(len(pairing_list)):
                            pair = participant['entry'][i]['resource']['extension'][j]

                            old_pairing.append(
                                pair.get('valueString', pair['valueReference']).split('Location/')[1]
                            )
            else:
                for key in pairing_list:
                    old_pairing.append(_get_old_pairing(participant, key))

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
                    if not new_pairing[0].startswith(participant["awardee"]):
                        logging.info(
                            "Skipping participant %s where pairing %s does not begin with old awardee %s"
                            % (participant_id, new_pairing, participant["awardee"])
                        )
                        continue

            if biospecimen_sync:
                for i in range(len(pairing_list)):
                    logging.info("%s %s => %s", obj_id, old_pairing[i]['site']['value'], new_pairing[i])
            elif pm_sync:
                for i in range(len(pairing_list)):
                    logging.info("%s %s => %s", participant_id, old_pairing[i], new_pairing[i])
            else:
                logging.info("%s %s => %s", participant_id, old_pairing, new_pairing)

            if new_pairing == "UNSET":
                for i in pairing_list:
                    participant[i] = "UNSET"
                participant["providerLink"] = []
            else:
                if biospecimen_sync:
                    participant['status'] = "re-pairing"
                    j = 0
                    for i in pairing_list:
                        participant[i]['site']['value'] = new_pairing[j]
                        j += 1
                elif pm_sync:
                    participant['status'] = "re-pairing"
                    for i in range(len(participant['entry'])):
                        if participant['entry'][i]['resource']['resourceType'] == 'Composition':
                            for j in range(len(pairing_list)):
                                pair = participant['entry'][i]['resource']['extension'][j]
                                if 'valueString' in pair:
                                    pair['valueString'] = new_pairing[j]
                                elif 'valueReference' in pair:
                                    pair['valueReference'] = new_pairing[j]
                else:
                    for i in pairing_list:
                        del participant[i]
                    participant[pairing_key] = new_pairing

            if client.args.dry_run and client.args.pairing in pairing_list:
                logging.info("Dry run, would update participant[%r] to %r.", pairing_key, new_pairing[0])
            elif client.args.dry_run and biospecimen_sync:
                for i in range(len(pairing_list)):
                    logging.info("Dry run, would update biobank_order[%r] to %r", pairing_list[i], new_pairing[i])
            elif client.args.dry_run and pm_sync:
                for i in range(len(pairing_list)):
                    logging.info("Dry run, would update physical_measurements[%r] to %r", pairing_list[i], new_pairing[i])
            else:
                print(participant)
                client.request_json(
                    request_url, "PATCH" if pm_sync or biospecimen_sync else "PUT", participant, headers={"If-Match": client.last_etag}
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
