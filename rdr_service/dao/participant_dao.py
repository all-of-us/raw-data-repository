import datetime
import json
import logging
from typing import Collection

from sqlalchemy.orm import joinedload, Session
from sqlalchemy.orm.session import make_transient
from sqlalchemy.sql.expression import literal

from werkzeug.exceptions import BadRequest, Forbidden

from rdr_service import clock
from rdr_service.api_util import (
    format_json_date,
    format_json_enum,
    format_json_hpo,
    format_json_org,
    format_json_site,
    get_awardee_id_from_name,
    get_organization_id_from_external_id,
    get_site_id_from_google_group,
    parse_json_enum,
    DEV_MAIL)
from rdr_service.app_util import get_oauth_id, lookup_user_info, get_account_origin_id, is_care_evo_and_not_prod
from rdr_service.code_constants import UNSET, ORIGINATING_SOURCES
from rdr_service.dao.base_dao import BaseDao, UpdatableDao
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.organization_dao import OrganizationDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.model.config_utils import to_client_biobank_id
from rdr_service.model.organization import Organization
from rdr_service.model.participant import Participant, ParticipantHistory
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.site import Site
from rdr_service.model.utils import to_client_participant_id
from rdr_service.participant_enums import (
    EhrStatus,
    EnrollmentStatus,
    SuspensionStatus,
    TEST_HPO_NAME,
    UNSET_HPO_ID,
    WithdrawalReason,
    WithdrawalStatus,
    make_primary_provider_link_for_id,
)


class ParticipantHistoryDao(BaseDao):
    """Maintains version history for participants.

  All previous versions of a participant are maintained (with the same participantId value and
  a new version value for each update.)

  Old versions of a participant are used to generate historical metrics (e.g. count the number of
  participants with different statuses or HPO IDs over time).

  Do not use this DAO for write operations directly; instead use ParticipantDao.
  """

    def __init__(self):
        super(ParticipantHistoryDao, self).__init__(ParticipantHistory)

    def get_id(self, obj):
        return [obj.participantId, obj.version]

    @classmethod
    def get_pairing_history(cls, session: Session, participant_ids: Collection[int]) -> Collection:
        """Loads the pairing history for the given participants"""
        return session.query(
            ParticipantHistory.participantId,
            ParticipantHistory.lastModified,
            ParticipantHistory.hpoId,
            ParticipantHistory.organizationId,
            Organization.externalId,
            ParticipantHistory.siteId
        ).join(
            Organization,
            Organization.organizationId == ParticipantHistory.organizationId
        ).filter(
            ParticipantHistory.participantId.in_(participant_ids)
        ).order_by(ParticipantHistory.lastModified).distinct().all()


class ParticipantDao(UpdatableDao):
    def __init__(self):
        super(ParticipantDao, self).__init__(Participant)

        self.hpo_dao = HPODao()
        self.organization_dao = OrganizationDao()
        self.site_dao = SiteDao()

    def get(self, id_: int) -> object:
        with self.session() as session:
            obj = self.get_with_session(session, id_)
        if obj:
            client = get_account_origin_id()
            # Care evolution can GET participants from PTSC as long as env < prod.
            if obj.participantOrigin != client and client in ORIGINATING_SOURCES and not is_care_evo_and_not_prod():
                raise BadRequest('Can not retrieve participant from a different origin')
            return obj


    def get_id(self, obj):
        return obj.participantId

    def insert_with_session(self, session, obj):
        obj.hpoId = self._get_hpo_id(obj)
        obj.version = 1
        obj.signUpTime = clock.CLOCK.now().replace(microsecond=0)
        obj.lastModified = obj.signUpTime
        obj.participantOrigin = get_account_origin_id()
        if obj.withdrawalStatus is None:
            obj.withdrawalStatus = WithdrawalStatus.NOT_WITHDRAWN
        if obj.suspensionStatus is None:
            obj.suspensionStatus = SuspensionStatus.NOT_SUSPENDED
        super(ParticipantDao, self).insert_with_session(session, obj)
        history = ParticipantHistory()
        history.fromdict(obj.asdict(), allow_pk=True)
        session.add(history)
        return obj

    def insert(self, obj):
        if obj.participantId:
            assert obj.biobankId
            return super(ParticipantDao, self).insert(obj)
        assert not obj.biobankId
        return self._insert_with_random_id(obj, ("participantId", "biobankId", "researchId"))

    def update_ghost_participant(self, session, pid):
        if not pid:
            raise Forbidden("Can not update participant without id")

        participant = self.get_for_update(session, pid)
        if participant is None:
            logging.warning(
                f"Tried to mark participant with id: [{pid}] as ghost \
                 but participant does not exist. Wrong environment?"
            )
        else:
            participant.isGhostId = 1
            participant.dateAddedGhost = clock.CLOCK.now()
            self._update_history(session, participant, participant)
            super(ParticipantDao, self)._do_update(session, participant, participant)

    def _check_if_external_id_exists(self, obj):
        with self.session() as session:
            return session.query(Participant).filter_by(externalId=obj.externalId).first()

    def _update_history(self, session, obj, existing_obj):
        # Increment the version and add a new history entry.
        obj.version = existing_obj.version + 1
        history = ParticipantHistory()
        history.fromdict(obj.asdict(), allow_pk=True)
        session.add(history)

    def _validate_update(self, session, obj, existing_obj):
        # Withdrawal and suspension have default values assigned on insert, so they should always have
        # explicit values in updates.

        if obj.withdrawalStatus is None:
            raise BadRequest("missing withdrawal status in update")
        if obj.suspensionStatus is None:
            raise BadRequest("missing suspension status in update")
        if (
            obj.withdrawalReason != WithdrawalReason.UNSET
            and obj.withdrawalReason is not None
            and obj.withdrawalReasonJustification is None
        ):
            raise BadRequest("missing withdrawalReasonJustification in update")
        if existing_obj:
            email = get_oauth_id()
            user_info = lookup_user_info(email)
            base_name = user_info.get('clientId')
            if not base_name:
                if email == DEV_MAIL:
                    base_name = "example"  # TODO: This is a hack because something sets up configs different
                    # when running all tests and it doesnt have the clientId key.
            base_name = base_name.lower()
            if base_name in ORIGINATING_SOURCES and base_name != existing_obj.participantOrigin:
                logging.warning(f"{base_name} tried to modify participant from \
                        {existing_obj.participantOrigin}")
                raise BadRequest(f"{base_name} not able to update participant from \
                        {existing_obj.participantOrigin}")
        super(ParticipantDao, self)._validate_update(session, obj, existing_obj)
        # Once a participant marks their withdrawal status as NO_USE, it can't be changed back.
        # TODO: Consider the future ability to un-withdraw.
        if (existing_obj.withdrawalStatus == WithdrawalStatus.NO_USE
            and obj.withdrawalStatus != WithdrawalStatus.NO_USE) \
            or (existing_obj.withdrawalStatus == WithdrawalStatus.EARLY_OUT
                and obj.withdrawalStatus != WithdrawalStatus.EARLY_OUT):
            raise Forbidden(f"Participant {obj.participantId} has withdrawn, cannot unwithdraw")

    def get_for_update(self, session, obj_id):
        # Fetch the participant summary at the same time as the participant, as we are potentially
        # updating both.
        return self.get_with_session(
            session, obj_id, for_update=True, options=joinedload(Participant.participantSummary)
        )

    def _do_update(self, session, obj, existing_obj):
        """Updates the associated ParticipantSummary, and extracts HPO ID from the provider link
      or set pairing at another level (site/organization/awardee) with parent/child enforcement."""
        obj.lastModified = clock.CLOCK.now()
        obj.signUpTime = existing_obj.signUpTime
        obj.biobankId = existing_obj.biobankId
        obj.withdrawalTime = existing_obj.withdrawalTime
        obj.suspensionTime = existing_obj.suspensionTime
        obj.participantOrigin = existing_obj.participantOrigin

        need_new_summary = False
        if obj.withdrawalStatus != existing_obj.withdrawalStatus:
            obj.withdrawalTime = obj.lastModified if obj.withdrawalStatus == WithdrawalStatus.NO_USE \
                                                     or obj.withdrawalStatus == WithdrawalStatus.EARLY_OUT else None
            obj.withdrawalAuthored = (
                obj.withdrawalAuthored if obj.withdrawalStatus == WithdrawalStatus.NO_USE
                                          or obj.withdrawalStatus == WithdrawalStatus.EARLY_OUT else None
            )

            need_new_summary = True

            # Participants that haven't yet consented should be withdrawn with EARLY_OUT
            if existing_obj.participantSummary is None and obj.withdrawalStatus != WithdrawalStatus.EARLY_OUT:
                logging.error(
                    f'Un-consented participant {existing_obj.participantId} was withdrawn with {obj.withdrawalStatus}'
                )

        if obj.suspensionStatus != existing_obj.suspensionStatus:
            obj.suspensionTime = obj.lastModified if obj.suspensionStatus == SuspensionStatus.NO_CONTACT else None
            need_new_summary = True
        update_pairing = True
        if existing_obj.enrollmentSiteId:
            obj.enrollmentSiteId = existing_obj.enrollmentSiteId
        if obj.siteId is None and obj.organizationId is None and obj.hpoId is None and obj.providerLink == "null":
            # Prevent unpairing if /PUT is sent with no pairing levels.
            update_pairing = False

        if update_pairing is True:
            has_id = False
            if obj.organizationId or obj.siteId or (obj.hpoId >= 0):
                has_id = True

            provider_link_unchanged = True
            if obj.providerLink is not None:
                if existing_obj.providerLink:
                    provider_link_unchanged = json.loads(obj.providerLink) == json.loads(existing_obj.providerLink)
                else:
                    provider_link_unchanged = False

            null_provider_link = obj.providerLink == "null"
            # site,org,or awardee is sent in request: Get relationships and try to set provider link.
            if has_id and (provider_link_unchanged or null_provider_link):
                site, organization, awardee = self.get_pairing_level(obj)
                obj.organizationId = organization
                obj.siteId = site
                obj.hpoId = awardee
                if awardee is not None and (obj.hpoId != existing_obj.hpoId):
                    # get provider link for hpo_id (awardee)
                    obj.providerLink = make_primary_provider_link_for_id(awardee)

                need_new_summary = True
            else:  # providerLink has changed
                # If the provider link changes, update the HPO ID on the participant and its summary.
                if obj.hpoId is None:
                    obj.hpoId = existing_obj.hpoId
                new_hpo_id = self._get_hpo_id(obj)
                if new_hpo_id != existing_obj.hpoId:
                    obj.hpoId = new_hpo_id
                    obj.siteId = None
                    obj.organizationId = None
                    need_new_summary = True

        # No pairing updates sent, keep existing values.
        if update_pairing == False:
            obj.siteId = existing_obj.siteId
            obj.organizationId = existing_obj.organizationId
            obj.hpoId = existing_obj.hpoId
            obj.providerLink = existing_obj.providerLink

        if need_new_summary and existing_obj.participantSummary:
            # Copy the existing participant summary, and mutate the fields that
            # come from participant.
            summary = existing_obj.participantSummary
            summary.hpoId = obj.hpoId
            summary.organizationId = obj.organizationId
            summary.siteId = obj.siteId
            summary.enrollmentSiteId = obj.enrollmentSiteId
            summary.withdrawalStatus = obj.withdrawalStatus
            summary.withdrawalReason = obj.withdrawalReason
            summary.withdrawalReasonJustification = obj.withdrawalReasonJustification
            summary.withdrawalTime = obj.withdrawalTime
            summary.withdrawalAuthored = obj.withdrawalAuthored
            summary.suspensionStatus = obj.suspensionStatus
            summary.suspensionTime = obj.suspensionTime
            summary.lastModified = clock.CLOCK.now()
            make_transient(summary)
            make_transient(obj)
            obj.participantSummary = summary
        self._update_history(session, obj, existing_obj)
        super(ParticipantDao, self)._do_update(session, obj, existing_obj)

    def get_pairing_level(self, obj):
        organization_id = obj.organizationId
        site_id = obj.siteId
        awardee_id = obj.hpoId
        # TODO: DO WE WANT TO PREVENT PAIRING IF EXISTING SITE HAS PM/BIO.

        if site_id != UNSET and site_id is not None:
            site = self.site_dao.get(site_id)
            if site is None:
                raise BadRequest(f"Site with site id {site_id} does not exist.")
            organization_id = site.organizationId
            awardee_id = site.hpoId
            return site_id, organization_id, awardee_id
        elif organization_id != UNSET and organization_id is not None:
            organization = self.organization_dao.get(organization_id)
            if organization is None:
                raise BadRequest(f"Organization with id {organization_id} does not exist.")
            awardee_id = organization.hpoId
            return None, organization_id, awardee_id
        return None, None, awardee_id

    @staticmethod
    def create_summary_for_participant(obj):
        return ParticipantSummary(
            participantId=obj.participantId,
            lastModified=obj.lastModified,
            biobankId=obj.biobankId,
            signUpTime=obj.signUpTime,
            hpoId=obj.hpoId,
            organizationId=obj.organizationId,
            siteId=obj.siteId,
            enrollmentSiteId=obj.enrollmentSiteId,
            withdrawalStatus=obj.withdrawalStatus,
            withdrawalReason=obj.withdrawalReason,
            withdrawalReasonJustification=obj.withdrawalReasonJustification,
            suspensionStatus=obj.suspensionStatus,
            enrollmentStatus=EnrollmentStatus.INTERESTED,
            ehrStatus=EhrStatus.NOT_PRESENT,
            participantOrigin=obj.participantOrigin
        )

    @staticmethod
    def _get_hpo_id(obj: Participant):
        if obj.isTestParticipant:
            return HPODao().get_by_name(TEST_HPO_NAME).hpoId

        hpo_name = _get_hpo_name_from_participant(obj)
        if hpo_name:
            hpo = HPODao().get_by_name(hpo_name)
            if not hpo:
                raise BadRequest(f"No HPO found with name {hpo_name}")
            return hpo.hpoId
        else:
            return UNSET_HPO_ID

    def validate_participant_reference(self, session, obj):
        """Raises BadRequest if an object has a missing or invalid participantId reference,
    or if the participant has a withdrawal status of NO_USE."""
        if obj.participantId is None:
            raise BadRequest(f"{obj.__class__.__name__}.participantId required.")
        return self.validate_participant_id(session, obj.participantId)

    def validate_participant_id(self, session, participant_id):
        """Raises BadRequest if a participant ID is invalid,
    or if the participant has a withdrawal status of NO_USE."""
        participant = self.get_with_session(session, participant_id)
        if participant is None:
            raise BadRequest(f"Participant with ID {participant_id} is not found.")
        raise_if_withdrawn(participant)
        return participant

    def get_biobank_ids_sample(self, session, percentage, batch_size):
        """Returns biobank ID and signUpTime for a percentage of participants.

    Used in generating fake biobank samples."""
        return (
            session.query(Participant.biobankId, Participant.signUpTime)
            .filter(Participant.biobankId % 100 <= percentage * 100)
            .yield_per(batch_size)
        )

    def get_pid_rid_mapping(self, **kwargs):
        sign_up_after = kwargs.get('sign_up_after')
        sort = kwargs.get('sort') if kwargs.get('sort') == 'lastModified' else 'signUpTime'
        try:
            sign_up_after_date = datetime.datetime.strptime(sign_up_after, '%Y-%m-%d')
        except TypeError or ValueError:
            raise BadRequest("Invalid parameter signUpAfter, the format should be: YYYY-MM-DD")

        with self.session() as session:
            query = session.query(Participant.participantId, Participant.researchId, Participant.signUpTime,
                                  Participant.lastModified)
            query = query.filter(Participant.signUpTime >= sign_up_after_date)
            if sort == 'lastModified':
                query = query.order_by(Participant.lastModified)
            else:
                query = query.order_by(Participant.signUpTime)

            query = query.limit(10000)

            items = query.all()

        result = {'data': [], 'sort_by': sort}
        for item in items:
            result['data'].append({
                'participant_id': item.participantId,
                'research_id': item.researchId,
                'sign_up_time': item.signUpTime,
                'last_modified': item.lastModified
            })

        return result

    def to_client_json(self, model):
        client_json = {
            "participantId": to_client_participant_id(model.participantId),
            "externalId": model.externalId,
            "hpoId": model.hpoId,
            "awardee": model.hpoId,
            "organization": model.organizationId,
            "siteId": model.siteId,
            "enrollmentSiteId": model.enrollmentSiteId,
            "biobankId": to_client_biobank_id(model.biobankId),
            "lastModified": model.lastModified.isoformat(),
            "signUpTime": model.signUpTime.isoformat(),
            "providerLink": json.loads(model.providerLink),
            "withdrawalStatus": model.withdrawalStatus,
            "withdrawalReason": model.withdrawalReason,
            "withdrawalReasonJustification": model.withdrawalReasonJustification,
            "withdrawalTime": model.withdrawalTime,
            "withdrawalAuthored": model.withdrawalAuthored,
            "suspensionStatus": model.suspensionStatus,
            "suspensionTime": model.suspensionTime,
        }
        format_json_hpo(client_json, self.hpo_dao, "hpoId"),
        format_json_org(client_json, self.organization_dao, "organization"),
        format_json_site(client_json, self.site_dao, "site"),
        format_json_site(client_json, self.site_dao, "enrollmentSite"),
        format_json_enum(client_json, "withdrawalStatus")
        format_json_enum(client_json, "withdrawalReason")
        format_json_enum(client_json, "suspensionStatus")
        format_json_date(client_json, "withdrawalTime")
        format_json_date(client_json, "suspensionTime")
        client_json["awardee"] = client_json["hpoId"]
        if "siteId" in client_json:
            del client_json["siteId"]
        if "enrollmentSiteId" in client_json:
            del client_json["enrollmentSiteId"]
        return client_json

    def from_client_json(self, resource_json, id_=None, expected_version=None, client_id=None):
        parse_json_enum(resource_json, "withdrawalStatus", WithdrawalStatus)
        parse_json_enum(resource_json, "withdrawalReason", WithdrawalReason)
        parse_json_enum(resource_json, "suspensionStatus", SuspensionStatus)
        if "withdrawalTimeStamp" in resource_json and resource_json["withdrawalTimeStamp"] is not None:
            try:
                resource_json["withdrawalTimeStamp"] = datetime.datetime.utcfromtimestamp(
                    float(resource_json["withdrawalTimeStamp"]) / 1000
                )
            except (ValueError, TypeError):
                raise ValueError("Could not parse {} as TIMESTAMP".format(resource_json["withdrawalTimeStamp"]))

        # allow for only sending the test flag (PATCH) if updating a participant as a test account
        test_flag = resource_json.get("testParticipant", False)
        participant = None

        if test_flag:
            participant = self.get(id_)
        if participant is None:
            participant = Participant(participantId=id_)

        # biobankId, lastModified, signUpTime are set by DAO.
        for participant_model_field, resource_value in [
                    ('externalId', resource_json.get("externalId")),
                    ('version', expected_version),
                    ('providerLink', json.dumps(resource_json.get("providerLink"))),
                    ('clientId', client_id),
                    ('withdrawalStatus', resource_json.get("withdrawalStatus")),
                    ('withdrawalReason', resource_json.get("withdrawalReason")),
                    ('withdrawalAuthored', resource_json.get("withdrawalTimeStamp")),
                    ('withdrawalReasonJustification', resource_json.get("withdrawalReasonJustification")),
                    ('suspensionStatus', resource_json.get("suspensionStatus")),
                    ('organizationId', get_organization_id_from_external_id(resource_json, self.organization_dao)),
                    ('hpoId', get_awardee_id_from_name(resource_json, self.hpo_dao)),
                    ('siteId', get_site_id_from_google_group(resource_json, self.site_dao)),
                    ('enrollmentSiteId', get_site_id_from_google_group(resource_json, self.site_dao)),
                    ('isTestParticipant', test_flag)
                ]:
            if resource_value is not None:
                participant.__setattr__(participant_model_field, resource_value)

        if participant.isTestParticipant:
            self.switch_to_test_account(None, participant, commit_update=False)
        return participant

    def add_missing_hpo_from_site(self, session, participant_id, site_id):
        if site_id is None:
            raise BadRequest("No site ID given for auto-pairing participant.")
        site = SiteDao().get_with_session(session, site_id)
        if site is None:
            raise BadRequest(f"Invalid siteId reference {site_id}.")

        participant = self.get_for_update(session, participant_id)
        if participant is None:
            raise BadRequest(f"No participant {participant_id} for HPO ID udpate.")

        if participant.siteId == site.siteId:
            return
        participant.hpoId = site.hpoId
        participant.organizationId = site.organizationId
        participant.siteId = site.siteId
        participant.providerLink = make_primary_provider_link_for_id(site.hpoId)
        if participant.participantSummary is None:
            raise RuntimeError(f"No ParticipantSummary available for P{participant_id}.")
        participant.participantSummary.hpoId = site.hpoId
        participant.lastModified = clock.CLOCK.now()
        # Update the version and add history row
        self._do_update(session, participant, participant)

    def switch_to_test_account(self, session, participant, commit_update=True):
        test_hpo_id = HPODao().get_by_name(TEST_HPO_NAME).hpoId

        if participant is None:
            raise BadRequest("No participant for HPO ID update.")

        if participant.hpoId == test_hpo_id:
            return

        participant.hpoId = test_hpo_id
        participant.organizationId = None
        participant.siteId = None

        if commit_update:
            # Update the version and add history row
            self._do_update(session, participant, participant)

    def handle_integrity_error(self, tried_ids, e, obj):
        if "external_id" in str(e.orig):
            existing_participant = self._check_if_external_id_exists(obj)
            if existing_participant:
                return existing_participant
        return super(ParticipantDao, self).handle_integrity_error(tried_ids, e, obj)

    def get_participant_id_mapping(self, is_sql=False):
        with self.session() as session:
            participant_map = (
                session.query(
                    Participant.participantId.label('p_id'),
                    literal('r_id'),
                    Participant.researchId.label('id_value'),
                ).union(
                    session.query(
                        Participant.participantId.label('p_id'),
                        literal('vibrent_id'),
                        Participant.externalId.label('id_value'),
                    )).filter(
                    Participant.researchId.isnot(None),
                    Participant.externalId.isnot(None)
                ))

            if is_sql:
                sql = self.literal_sql_from_query(participant_map)
                sql = sql.replace('param_1', 'id_source')
                sql = sql.replace('param_2', 'id_source')
                return sql

            return participant_map.all()

    def get_org_and_site_for_ids(self, participant_ids: Collection[int]):
        """
        Returns tuples of the format (participant id, org external id, site google group)
        If a participant is unpaired to an org they will be left out, if they're unpaired to
        a site then their third item will be null
        """
        with self.session() as session:
            return (
                session.query(Participant.participantId, Organization.externalId, Site.googleGroup)
                .select_from(Participant)
                .join(Organization)
                .outerjoin(Site, Site.siteId == Participant.siteId)
                .filter(Participant.participantId.in_(participant_ids))
                .all()
            )


def _get_primary_provider_link(participant):
    if participant.providerLink:
        provider_links = json.loads(participant.providerLink)
        if provider_links:
            for provider in provider_links:
                if provider.get("primary") == True:
                    return provider
    return None


def _get_hpo_name_from_participant(participant):
    """Returns ExtractionResult with the string representing the HPO."""
    primary_provider_link = _get_primary_provider_link(participant)
    if primary_provider_link and primary_provider_link.get("organization"):
        reference = primary_provider_link.get("organization").get("reference")
        if reference and reference.lower().startswith("organization/"):
            return reference[13:]
    return None


def raise_if_withdrawn(obj):
    if obj.withdrawalStatus == WithdrawalStatus.NO_USE or obj.withdrawalStatus == WithdrawalStatus.EARLY_OUT:
        raise Forbidden(f"Participant {obj.participantId} has withdrawn")
