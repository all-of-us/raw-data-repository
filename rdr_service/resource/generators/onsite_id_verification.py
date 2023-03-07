#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging
from werkzeug.exceptions import NotFound

from rdr_service.dao.resource_dao import ResourceDataDao
from rdr_service.participant_enums import OnSiteVerificationType, OnSiteVerificationVisitType
from rdr_service.resource import generators, schemas

class OnSiteIdVerificationGenerator(generators.BaseGenerator):

    def __init__(self, ro_dao=None):
        self.ro_dao = ro_dao

    def make_resource(self, _pk):
        """
        Build a Resource object for the requested onsite_id_verification record
        :param _pk: Primary key id value from onsite_id_verification table
        :return: ResourceDataObject object
        """
        if not self.ro_dao:
            self.ro_dao = ResourceDataDao(backup=True)

        with self.ro_dao.session() as ro_session:
            sql = """
                select v.*, s.google_group as site
                from onsite_id_verification v
                left join site s on v.site_id = s.site_id
                where id = :id
            """

            row = ro_session.execute(sql, {'id': _pk}).first()
            if not row:
                msg = f'Lookup failed for onsite_id_verification_record {id}'
                logging.error(msg)
                raise NotFound(msg)

            data = self.ro_dao.to_dict(row)
            # This will exclude the provider user_email that is in the RDR record; not expected to be needed in PDR
            for field in schemas.OnSiteIdVerificationSchema.Meta.pii_fields:
                if data[field]:
                    del data[field]

            # Populate Enum fields. Note: When Enums have a possible zero value, explicitly check for None.
            if data['verification_type'] is not None:
                enum = OnSiteVerificationType(data['verification_type'])
                data['verification_type'] = str(enum)
                data['verification_type_id'] = int(enum)
            else:
                data['verification_type'] = str(OnSiteVerificationType.UNSET)
                data['verification_type_id'] = int(OnSiteVerificationType.UNSET)

            if data['visit_type'] is not None:
                enum = OnSiteVerificationVisitType(data['visit_type'])
                data['visit_type'] = str(enum)
                data['visit_type_id'] = int(enum)
            else:
                data['visit_type'] = str(OnSiteVerificationVisitType.UNSET)
                data['visit_type_id'] = int(OnSiteVerificationVisitType.UNSET)

            return generators.ResourceRecordSet(schemas.OnSiteIdVerificationSchema, data)

def onsite_id_verification_build(_pk, gen=None, w_dao=None):
    """
    Generate a single OnSiteIdVerification resource record.
    :param _pk: Primary Key integer value
    :param gen: OnSiteIdVerificationGenerator object
    :param w_dao: Writable DAO object.
    """
    if not w_dao:
        w_dao = ResourceDataDao()
    if not gen:
        gen = OnSiteIdVerificationGenerator()
    res = gen.make_resource(_pk)
    res.save(w_dao=w_dao)

def onsite_id_verification_batch_rebuild(_pk_ids):
    """
    Generate OnSiteIdVerification resource records for a batch/list of record ids.
    :param _pk_ids: list of primary key id integer values
    """
    gen = OnSiteIdVerificationGenerator()
    w_dao = ResourceDataDao()
    for _pk in _pk_ids:
        onsite_id_verification_build(_pk, gen=gen, w_dao=w_dao)

def onsite_id_verification_build_task(_pk_id: int):
    """
    Task endpoint triggered by RDR POST /OnSite/Id/Verification.  Builds a single resource record
    :param _pk_id:  Primary key id for RDR onsite_id_verification table
    """
    if not isinstance(_pk_id, int):
        raise ValueError(f'Invalid primary key {_pk_id}.  Expected integer value')
    onsite_id_verification_build(_pk_id)

def onsite_id_verification_batch_rebuild_task(_pk_ids):
    """
    Task endpoint to rebuild a list of PDR OnSiteIdVerification resource records
    Triggered by resource tool --batch mode
    :param _pk_ids:  List of integer primary key ids for RDR onsite_id_verification table
    """
    if not isinstance(_pk_ids, list) or not all(isinstance(pk_id, int) for pk_id in _pk_ids):
        raise ValueError(f'Invalid onsite_verification_id values {_pk_ids}.  Expected list of integer values')
    onsite_id_verification_batch_rebuild(_pk_ids)
