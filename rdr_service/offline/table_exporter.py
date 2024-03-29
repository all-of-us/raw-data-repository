import hashlib
import random
import re
import struct

from rdr_service.config import GAE_PROJECT, LOCALHOST_DEFAULT_BUCKET_NAME
from werkzeug.exceptions import BadRequest

from rdr_service.dao.database_factory import get_database
from rdr_service.offline.sql_exporter import SqlExporter

_TABLE_PATTERN = re.compile("^[A-Za-z0-9_]+$")
_INSTANCE_PATTERN = re.compile("^[A-Za-z0-9_-]+$")

_DEIDENTIFY_DB_TABLE_ALLOWED = {
    "rdr": set(["ppi_participant_view", "physical_measurements_view", "questionnaire_response_answer_view"])
}


class TableExporter(object):
    """API that exports data from our database to UTF-8 CSV files in GCS.

  Used instead of Cloud SQL export because it handles newlines and null characters in a way that
  other CSV clients (e.g. BigQuery, Google Sheets) can actually understand.
  """

    @staticmethod
    def _obfuscate_id(pmi_id, salt):
        """
    One-way ID obfuscation, intended for PMI participant IDs.

    This method should aim to (1) avoid hash collisions across PMI ID inputs and (2) not be
    reversible or rerunnable by anyone without RDR production data access.

    Params:
      - pmi_id (number) must be an integer participant ID
      - salt (string) a salt to be combined with the pmi_id

    Returns: a positive integer
    """
        h = hashlib.sha1()
        # l for long, q for long long: https://docs.python.org/2/library/struct.html#format-characters
        h.update(struct.pack(">l", pmi_id))
        h.update(salt)
        # Just take the first 8 bytes so that the output ID is a long, roughly in the same domain as
        # the input PMI participant ID.
        b = h.digest()[0:8]
        return abs(struct.unpack(">q", b)[0])

    @classmethod
    def _export_csv(cls, bucket_name, database, directory, deidentify_salt, table_name, instance_name):
        assert _TABLE_PATTERN.match(table_name)
        assert _TABLE_PATTERN.match(database)
        if instance_name:
            assert _INSTANCE_PATTERN.match(instance_name)

        transformf = None
        if deidentify_salt:
            # Deidentification requested: hash outgoing participant IDs with a consistent salt across this
            # export. Cache obfuscated participant IDs across row callbacks to avoid recomputation and to
            # detect collisions.
            pmi_to_obfuscated = {}
            obfuscated_to_pmi = {}

            def f(row_proxy):
                out = [v for v in row_proxy]
                for i, key in enumerate(row_proxy.keys()):
                    if key != "participant_id":
                        continue

                    pmi_id = out[i]
                    if pmi_id not in pmi_to_obfuscated:
                        obf_id = TableExporter._obfuscate_id(pmi_id, deidentify_salt)
                        pmi_to_obfuscated[pmi_id] = obf_id
                        if obf_id in obfuscated_to_pmi:
                            raise ValueError(
                                "hash collision, {}, {} for salt {} both yield {}".format(
                                    pmi_id, obfuscated_to_pmi[obf_id], deidentify_salt, obf_id
                                )
                            )
                        obfuscated_to_pmi[obf_id] = pmi_id
                    out[i] = pmi_to_obfuscated[pmi_id]
                    break
                return out

            transformf = f

        output_path = "%s/%s.csv" % (directory, table_name)
        sql_table = "%s.%s" % (database, table_name)
        if get_database().db_type == "sqlite":
            # No schemas in SQLite.
            sql_table = table_name
        SqlExporter(bucket_name).run_export(
            output_path, "SELECT * FROM {}".format(sql_table), transformf=transformf, instance_name=instance_name
        )
        return "%s/%s" % (bucket_name, output_path)

    @staticmethod
    def export_tables(database, tables, directory, deidentify, instance_name=None):
        """
    Export the given tables from the given DB; deidentifying if requested.

    A deidentified request outputs exports into a different bucket which may have less restrictive
    ACLs than the other export buckets; for this reason the tables for these requests are also more
    restrictive.

    Deidentification also obfuscates participant IDs, as these are known by other systems (e.g.
    HealthPro, PTC). Currently this ID obfuscation is not reversible and is not stable across
    separate exports (note: it is stable across multiple tables in a single export request).
    """
        app_id = GAE_PROJECT
        # Determine what GCS bucket to write to based on the environment and database.
        if app_id == 'localhost':
            bucket_name = LOCALHOST_DEFAULT_BUCKET_NAME
        elif deidentify:
            bucket_name = "%s-deidentified-export" % app_id
        elif database == "rdr":
            bucket_name = "%s-rdr-export" % app_id
        elif database in ["cdm", "voc"]:
            bucket_name = "%s-cdm" % app_id
        else:
            raise BadRequest("Invalid database: %s" % database)
        if instance_name:
            if not _INSTANCE_PATTERN.match(instance_name):
                raise BadRequest("Invalid instance name: %s" % instance_name)
        for table_name in tables:
            if not _TABLE_PATTERN.match(table_name):
                raise BadRequest("Invalid table name: %s" % table_name)

        deidentify_salt = None
        if deidentify:
            if database not in _DEIDENTIFY_DB_TABLE_ALLOWED:
                raise BadRequest(
                    "deidentified exports are only supported for database: {}".format(
                        list(_DEIDENTIFY_DB_TABLE_ALLOWED.keys())
                    )
                )
            tableset = set(tables)
            table_allowed_list = _DEIDENTIFY_DB_TABLE_ALLOWED[database]
            if not tableset.issubset(table_allowed_list):
                raise BadRequest(
                    "deidentified exports are unsupported for tables:"
                    "[{}] (must be in [{}])".format(
                        ", ".join(tableset - table_allowed_list), ", ".join(table_allowed_list)
                    )
                )
            # This salt must be identical across all tables exported, otherwise the exported particpant
            # IDs will not be consistent. Used with sha1, so ensure this value isn't too short.
            deidentify_salt = str(random.getrandbits(256)).encode("utf-8")

        for table_name in tables:
            TableExporter._export_csv(bucket_name, database, directory, deidentify_salt, table_name, instance_name)

        # comment out the defer mechanism(from google.appengine.ext import deferred)
        # which not available any more, may need to put it back when we have a replacement
        #
        # for table_name in tables:
        #     deferred.defer(TableExporter._export_csv, bucket_name,
        #                    database, directory, deidentify_salt, table_name, instance_name)
        # return {'destination': 'gs://%s/%s' % (bucket_name, directory)}
