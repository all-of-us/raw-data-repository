import json

import pytz
from dateutil.parser import parse
from sqlalchemy.sql.functions import concat
from werkzeug.exceptions import BadRequest, Conflict, NotFound

from rdr_service.dao.base_dao import UpsertableDao
from rdr_service.dao.code_dao import CodeDao
from rdr_service.model.hpo import HPO
from rdr_service.model.organization import Organization
from rdr_service.model.participant import Participant
from rdr_service.model.patient_status import PatientStatus
from rdr_service.model.site import Site
from rdr_service.participant_enums import PatientStatusFlag


class PatientStatusDao(UpsertableDao):
    def __init__(self):
        self.code_dao = CodeDao()
        super(PatientStatusDao, self).__init__(PatientStatus)

    def from_client_json(
        self,
        resource_json,
        id_=None,
        expected_version=None,  # pylint: disable=unused-argument
        participant_id=None,
        client_id=None,
    ):  # pylint: disable=unused-argument

        if str(participant_id) not in resource_json["subject"]:
            raise BadRequest("Participant ID does not match participant ID in request.")

        with self.session() as session:
            p = session.query(Participant.participantId).filter_by(participantId=participant_id).first()
            if not p:
                raise BadRequest("invalid participant id")
            site = (
                session.query(Site.siteId, Site.organizationId, Site.hpoId)
                .filter_by(googleGroup=resource_json["site"])
                .first()
            )
            if not site:
                raise BadRequest("Invalid site value.")

        try:
            model = PatientStatus(
                participantId=participant_id,
                siteId=site.siteId,
                hpoId=site.hpoId,
                organizationId=site.organizationId,
                authored=parse(resource_json["authored"]),
                patientStatus=PatientStatusFlag(resource_json["patient_status"]),
                comment=resource_json["comment"] if "comment" in resource_json else None,
                user=resource_json["user"],
            )
        except KeyError as e:
            raise BadRequest("Patient status record missing json key: {0}".format(e.message))

        return model

    def update_participant_summary(self, p_id):
        """
    Update the participant summary with the patient status data for the given participant
    :param p_id: participant id
    """
        with self.session() as session:
            data = list()
            recs = (
                session.query(PatientStatus.patientStatus, Organization.externalId)
                .join(Organization)
                .filter(PatientStatus.participantId == p_id)
                .all()
            )
            for rec in recs:
                # for filtering purposes, this must be in order of organization and then status.
                data.append({"organization": rec.externalId, "status": str(rec.patientStatus)})

            if len(data) > 0:
                sql = "update participant_summary set patient_status = :data where participant_id = :pid"
                # Note: don't bother to output pretty json or sort keys.
                session.execute(sql, {"data": json.dumps(data, sort_keys=False), "pid": p_id})

    def insert(self, obj):
        """Inserts an object into the database. The calling object may be mutated
    in the process."""
        with self.session() as session:
            ps_obj = (
                session.query(PatientStatus.id)
                .filter_by(participantId=obj.participantId, organizationId=obj.organizationId)
                .first()
            )
            if ps_obj:
                raise Conflict("Duplicate record found. Patient status must be updated, not inserted.")

        obj = super(PatientStatusDao, self).insert(obj)
        self.update_participant_summary(obj.participantId)
        return obj

    def get_etag(self, org_id, pid):  # pylint: disable=unused-argument
        return None

    def get_for_update(self, session, obj_id):
        obj = super(PatientStatusDao, self).get_with_session(session, obj_id, for_update=True)
        if obj:
            obj.version = None
        return obj

    def update(self, obj):

        with self.session() as session:
            ps_obj = (
                session.query(PatientStatus)
                .filter_by(participantId=obj.participantId, organizationId=obj.organizationId)
                .first()
            )
            if ps_obj:
                obj.id = ps_obj.id
                # If authored has timezone information, convert to UTC and remove tzinfo.
                try:
                    obj.authored = obj.authored.astimezone(pytz.utc).replace(tzinfo=None)
                except ValueError:
                    pass

        self.upsert(obj)

        with self.session() as session:
            obj = (
                session.query(PatientStatus)
                .filter_by(participantId=obj.participantId, organizationId=obj.organizationId)
                .first()
            )
        self.update_participant_summary(obj.participantId)
        return obj

    def _build_response_query(self, session, p_id, org_id):
        query = (
            session.query(
                concat("Patient/P", PatientStatus.participantId).label("subject"),
                HPO.name.label("awardee"),
                Organization.externalId.label("organization"),
                Site.googleGroup.label("site"),
                PatientStatus.patientStatus.label("patient_status"),
                PatientStatus.comment.label("comment"),
                PatientStatus.created.label("created"),
                PatientStatus.modified.label("modified"),
                PatientStatus.user.label("user"),
                PatientStatus.authored.label("authored"),
            )
            .filter_by(participantId=p_id, organizationId=org_id)
            .join(Site, Site.siteId == PatientStatus.siteId)
            .join(HPO, HPO.hpoId == Site.hpoId)
            .join(Organization, Organization.organizationId == Site.organizationId)
        )
        return query

    def to_client_json(self, model):
        with self.session() as session:
            query = self._build_response_query(session, model.participantId, model.organizationId)
            data = self.to_dict(query.first())
            if not data:
                raise NotFound("Patient status record not found.")
            return data

    def to_dict(self, obj, result_proxy=None):
        """ Convert Model record or custom query result to dict """
        data = super(PatientStatusDao, self).to_dict(obj, result_proxy)
        if data:
            # sqlalchemy sets this to None when patient_status=0, don't know why.
            if not isinstance(data["patient_status"], str):
                if isinstance(data["patient_status"], int):
                    data["patient_status"] = str(PatientStatusFlag(data["patient_status"]))
                else:
                    data["patient_status"] = str(PatientStatusFlag.UNSET)
        return data

    def get_id(self, obj):
        with self.session() as session:
            query = session.query(PatientStatus.id).filter_by(
                participantId=obj.participantId, organizationId=obj.organizationId
            )
            return query.first()

    def get(self, p_id, org_name):
        """
    Return the record for the given organization id and participant id
    :param p_id: Participant ID
    :param org_name: Organization Name
    :return: model dict
    """
        with self.session() as session:
            org_obj = session.query(Organization).filter_by(externalId=org_name).first()
            if not org_obj:
                raise NotFound("Organization not found.")

            query = self._build_response_query(session, p_id, org_obj.organizationId)
            data = self.to_dict(query.first())
            if not data:
                raise NotFound("Patient status record not found.")
            return data

    def get_history(self, p_id, org_name):
        """
    Return an array of history records for the given participant and organization.
    :param p_id: Participant ID
    :param org_name: Organization Name
    :return: list of dicts
    """
        records = list()

        with self.session() as session:
            org_obj = session.query(Organization.organizationId).filter_by(externalId=org_name).first()
            if not org_obj:
                raise NotFound("Organization not found.")

            # Setup query for 'patient_status' table and then convert it to history table sql.
            query = self._build_response_query(session, p_id, org_obj.organizationId)
            sql = (
                self.query_to_text(query)
                .replace("concat(%s", "concat(:p1")
                .replace(".participant_id = %s", ".participant_id = :p2")
                .replace(".organization_id = %s", ".organization_id = :p3")
                .replace("FROM patient_status", "FROM patient_status_history")
                .replace("patient_status.", "patient_status_history.")
            )
            sql += "\nORDER BY patient_status_history.revision_id"
            args = {"p1": "Patient/P", "p2": p_id, "p3": org_obj.organizationId}
            results = session.execute(sql, args)
            for row in results:
                data = self.to_dict(row, results)
                records.append(data)

        return records
