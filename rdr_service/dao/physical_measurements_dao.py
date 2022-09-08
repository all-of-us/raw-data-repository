import json
import logging

from rdr_service.lib_fhir.fhirclient_1_0_6.models import observation as fhir_observation
from rdr_service.lib_fhir.fhirclient_1_0_6.models.fhirabstractbase import FHIRValidationError
from sqlalchemy.orm import subqueryload
from sqlalchemy.orm.attributes import flag_modified
from werkzeug.exceptions import BadRequest

from rdr_service import clock
from rdr_service.api_util import parse_date
from rdr_service.concepts import Concept
from rdr_service.dao.base_dao import UpdatableDao
from rdr_service.dao.participant_dao import ParticipantDao, raise_if_withdrawn
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.model.log_position import LogPosition
from rdr_service.model.measurements import Measurement, PhysicalMeasurements
from rdr_service.participant_enums import PhysicalMeasurementsStatus, PhysicalMeasurementsCollectType, \
    OriginMeasurementUnit, SelfReportedPhysicalMeasurementsStatus

_AMENDMENT_URL = "http://terminology.pmi-ops.org/StructureDefinition/amends"
_OBSERVATION_RESOURCE_TYPE = "Observation"
_COMPOSITION_RESOURCE_TYPE = "Composition"
_CREATED_LOC_EXTENSION = "http://terminology.pmi-ops.org/StructureDefinition/authored-location"
_FINALIZED_LOC_EXTENSION = "http://terminology.pmi-ops.org/StructureDefinition/finalized-location"
_PM_SYSTEM_PREFIX = "http://terminology.pmi-ops.org/CodeSystem/"
_AUTHORING_STEP = "http://terminology.pmi-ops.org/StructureDefinition/authoring-step"
_CREATED_STATUS = "created"
_FINALIZED_STATUS = "finalized"
_LOCATION_PREFIX = "Location/"
_AUTHOR_PREFIX = "Practitioner/"
_QUALIFIED_BY_RELATED_TYPE = "qualified-by"
_ALL_EXTENSIONS = set([_AMENDMENT_URL, _CREATED_LOC_EXTENSION, _FINALIZED_LOC_EXTENSION])
_BYTE_LIMIT = 65535  # 65535 chars, 64KB


class PhysicalMeasurementsDao(UpdatableDao):
    def __init__(self):
        super(PhysicalMeasurementsDao, self).__init__(PhysicalMeasurements, order_by_ending=["logPositionId"])

    def get_id(self, obj):
        return obj.physicalMeasurementsId

    def get_with_session(self, session, obj_id, **kwargs):
        result = super(PhysicalMeasurementsDao, self).get_with_session(session, obj_id, **kwargs)
        if result:
            ParticipantDao().validate_participant_reference(session, result)
        return result

    def get_with_children(self, physical_measurements_id, for_update=False):
        """Make a new session and query db."""
        with self.session() as session:
            return self.get_with_children_with_session(session, physical_measurements_id, for_update)

    def get_with_children_with_session(self, session, physical_measurements_id, for_update=False):
        """Pass in an existing session to query db."""
        query = (
            session.query(PhysicalMeasurements)
            .options(subqueryload(PhysicalMeasurements.measurements).subqueryload(Measurement.measurements))
            .options(subqueryload(PhysicalMeasurements.measurements).subqueryload(Measurement.qualifiers))
        )

        if for_update:
            query = query.with_for_update()

        return query.get(physical_measurements_id)

    def get_measuremnets_for_participant(self, pid):
        with self.session() as session:
            query = session.query(PhysicalMeasurements).filter(PhysicalMeasurements.participantId == pid).all()

            return query

    def get_date_from_pm_resource(self, pid, pm_id):
        """ Retrieves a specific measurement and fetches the date from the measurement payload
            which corresponds to 'finalized date'.
            :param pid = participant id
            :param pm_id = physical measurement id
            :returns date from resource of measurement payload - UTC time"""
        with self.session() as session:
            record = session.query(PhysicalMeasurements).filter(PhysicalMeasurements.participantId == pid)\
                .filter(PhysicalMeasurements.physicalMeasurementsId == pm_id).first()

            doc, composition = self.load_record_fhir_doc(record)  # pylint: disable=unused-variable
            measurement_date = composition['date']
            original_date = parse_date(measurement_date)
            return original_date

    @staticmethod
    def handle_measurement(measurement_map, m):
        """Populating measurement_map with information extracted from measurement and its
    descendants."""
        code_concept = Concept(m.codeSystem, m.codeValue)
        measurement_data = measurement_map.get(code_concept)
        if not measurement_data:
            measurement_data = {
                "bodySites": set(),
                "types": set(),
                "units": set(),
                "codes": set(),
                "submeasurements": set(),
                "qualifiers": set(),
            }
            measurement_map[code_concept] = measurement_data
        if m.bodySiteCodeSystem:
            measurement_data["bodySites"].add(Concept(m.bodySiteCodeSystem, m.bodySiteCodeValue))
        if m.valueString:
            if len(m.valueString) > _BYTE_LIMIT:
                raise BadRequest("Notes field exceeds limit.")
            measurement_data["types"].add("string")
        if m.valueDecimal:
            measurement_data["types"].add("decimal")
            min_decimal = measurement_data.get("min")
            max_decimal = measurement_data.get("max")
            if min_decimal is None or min_decimal > m.valueDecimal:
                measurement_data["min"] = m.valueDecimal
            if max_decimal is None or max_decimal < m.valueDecimal:
                measurement_data["max"] = m.valueDecimal
        if m.valueUnit:
            measurement_data["units"].add(m.valueUnit)
        if m.valueCodeSystem:
            measurement_data["codes"].add(Concept(m.valueCodeSystem, m.valueCodeValue))
        if m.valueDateTime:
            measurement_data["types"].add("date")
        for sm in m.measurements:
            measurement_data["submeasurements"].add(Concept(sm.codeSystem, sm.codeValue))
            PhysicalMeasurementsDao.handle_measurement(measurement_map, sm)
        for q in m.qualifiers:
            measurement_data["qualifiers"].add(Concept(q.codeSystem, q.codeValue))

    def get_distinct_measurements(self):
        """Returns metadata about all the distinct physical measurements in use for participants."""
        with self.session() as session:
            measurement_map = {}
            for pms in session.query(PhysicalMeasurements).yield_per(100):
                try:
                    doc, composition = self.load_record_fhir_doc(pms)  # pylint: disable=unused-variable
                    parsed_pms = PhysicalMeasurementsDao.from_client_json(doc, pms.participantId)
                    for measurement in parsed_pms.measurements:
                        PhysicalMeasurementsDao.handle_measurement(measurement_map, measurement)
                except FHIRValidationError as e:
                    logging.error(f"Could not parse measurements as FHIR: {pms.resource}; exception = {e}")
            return measurement_map

    @staticmethod
    def concept_json(concept):
        return {"system": concept.system, "code": concept.code}

    @staticmethod
    def get_measurements_json(concept, measurement_data, m_map):
        result = {}
        result["code"] = PhysicalMeasurementsDao.concept_json(concept)
        result["bodySites"] = list(
            PhysicalMeasurementsDao.concept_json(body_concept) for body_concept in measurement_data["bodySites"]
        )
        result["types"] = list(measurement_data["types"])
        result["units"] = list(measurement_data["units"])
        if measurement_data.get("min"):
            result["min"] = measurement_data["min"]
        if measurement_data.get("max"):
            result["max"] = measurement_data["max"]
        result["valueCodes"] = list(
            PhysicalMeasurementsDao.concept_json(code_concept) for code_concept in measurement_data["codes"]
        )
        result["qualifiers"] = list(
            PhysicalMeasurementsDao.concept_json(qualifier_concept)
            for qualifier_concept in measurement_data["qualifiers"]
        )
        result["submeasurements"] = [
            PhysicalMeasurementsDao.get_measurements_json(sm, m_map[sm], m_map)
            for sm in measurement_data["submeasurements"]
        ]

        return result

    def get_distinct_measurements_json(self):
        """Returns metadata about all the distinct physical measurements in use for participants,
    in a JSON format that can be used to generate fake physical measurement data later."""
        measurement_map = self.get_distinct_measurements()
        measurements_json = []
        submeasurements = set()
        for concept, measurement_data in list(measurement_map.items()):
            for submeasurement_concept in measurement_data["submeasurements"]:
                submeasurements.add(submeasurement_concept)
        for concept, measurement_data in list(measurement_map.items()):
            # Only include submeasurements under their parents.
            if concept not in submeasurements:
                measurements_json.append(
                    PhysicalMeasurementsDao.get_measurements_json(concept, measurement_data, measurement_map)
                )
        return measurements_json

    def _initialize_query(self, session, query_def):
        participant_id = None
        for field_filter in query_def.field_filters:
            if field_filter.field_name == "participantId":
                participant_id = field_filter.value
                break
        # Sync queries don't specify a participant ID, and can return measurements for participants
        # who have subsequently withdrawn; for all requests that do specify a participant ID,
        # make sure the participant exists and is not withdrawn.
        if participant_id:
            ParticipantDao().validate_participant_id(session, participant_id)
        return super(PhysicalMeasurementsDao, self)._initialize_query(session, query_def)

    @staticmethod
    def _measurements_as_dict(measurements):
        result = measurements.asdict()
        del result["physicalMeasurementsId"]
        del result["created"]
        del result["logPositionId"]
        del result["origin"]
        del result["collectType"]
        del result["originMeasurementUnit"]
        del result["questionnaireResponseId"]

        if result["resource"].get("id", None):
            del result["resource"]["id"]

        return result

    @staticmethod
    def set_measurement_ids(physical_measurements):
        measurement_count = 0
        pm_id = physical_measurements.physicalMeasurementsId
        for measurement in physical_measurements.measurements:
            measurement.physicalMeasurementsId = pm_id
            measurement.measurementId = PhysicalMeasurementsDao.make_measurement_id(pm_id, measurement_count)
            measurement_count += 1
            for sub_measurement in measurement.measurements:
                sub_measurement.physicalMeasurementsId = pm_id
                sub_measurement.measurementId = PhysicalMeasurementsDao.make_measurement_id(pm_id, measurement_count)
                measurement_count += 1

    def get_exist_remote_pm(self, participant_id, finalized):
        with self.session() as session:
            return session.query(PhysicalMeasurements).filter(
                PhysicalMeasurements.participantId == participant_id,
                PhysicalMeasurements.finalized == finalized,
                PhysicalMeasurements.collectType == PhysicalMeasurementsCollectType.SELF_REPORTED).first()

    def insert_remote_pm(self, obj):
        if obj.physicalMeasurementsId:
            with self.session() as session:
                return self.insert_remote_pm_with_session(session, obj)
        else:
            return self._insert_with_random_id(obj, ["physicalMeasurementsId"], self.insert_remote_pm_with_session)

    def insert_remote_pm_with_session(self, session, obj):
        self.set_measurement_ids(obj)
        self.set_self_reported_pm_resource_json(obj)
        self._update_participant_summary(session, obj, is_amendment=False, is_self_reported=True)
        return super(PhysicalMeasurementsDao, self).insert_with_session(session, obj)

    def insert_with_session(self, session, obj):
        is_amendment = False
        obj.logPosition = LogPosition()
        obj.final = True
        obj.created = clock.CLOCK.now()
        resource_json = obj.resource if isinstance(obj.resource, dict) else json.loads(obj.resource)
        finalized_date = resource_json["entry"][0]["resource"].get("date")
        if finalized_date:
            obj.finalized = parse_date(finalized_date)
        for extension in resource_json["entry"][0]["resource"].get("extension", []):
            url = extension.get("url")
            if url not in _ALL_EXTENSIONS:
                logging.info(
                    f"Ignoring unsupported extension for PhysicalMeasurements: {url}. \
                    Expected one of: {_ALL_EXTENSIONS}"
                )
                continue
            if url == _AMENDMENT_URL:
                self._update_amended(obj, extension, url, session)
                is_amendment = True
                break
        participant_summary = self._update_participant_summary(session, obj, is_amendment)
        existing_measurements = (
            session.query(PhysicalMeasurements).filter(PhysicalMeasurements.participantId == obj.participantId).all()
        )
        if existing_measurements:
            new_dict = self._measurements_as_dict(obj)
            for measurements in existing_measurements:
                if self._measurements_as_dict(measurements) == new_dict:
                    # If there are already measurements that look exactly like this, return them
                    # without inserting new measurements.
                    return measurements
        PhysicalMeasurementsDao.set_measurement_ids(obj)

        inserted_obj = super(PhysicalMeasurementsDao, self).insert_with_session(session, obj)
        if not is_amendment:  # Amendments aren't expected to have site ID extensions.
            if participant_summary.biospecimenCollectedSiteId is None:
                ParticipantDao().add_missing_hpo_from_site(
                    session, inserted_obj.participantId, inserted_obj.finalizedSiteId
                )

        # Flush to assign an ID to the measurements, as the client doesn't provide one.
        session.flush()
        # Update the resource to contain the ID.
        resource_json["id"] = str(obj.physicalMeasurementsId)
        obj = self.store_record_fhir_doc(obj, resource_json)
        return obj

    def _update_participant_summary(self, session, obj, is_amendment=False, is_self_reported=False):
        participant_id = obj.participantId
        if participant_id is None:
            raise BadRequest("participantId is required")
        participant_summary_dao = ParticipantSummaryDao()
        participant = ParticipantDao().get_for_update(session, participant_id)
        if not participant:
            raise BadRequest(f"Can't submit physical measurements for unknown participant {participant_id}")
        participant_summary = participant.participantSummary
        if not participant_summary:
            raise BadRequest(f"Can't submit physical measurements for participant {participant_id} without consent")
        raise_if_withdrawn(participant_summary)
        participant_summary.lastModified = clock.CLOCK.now()
        if not is_self_reported:
            is_distinct_visit = participant_summary_dao.calculate_distinct_visits(
                participant_id, obj.finalized, obj.physicalMeasurementsId
            )
            if (
                obj.status
                and obj.status == PhysicalMeasurementsStatus.CANCELLED
                and is_distinct_visit
                and not is_amendment
            ):
                participant_summary.numberDistinctVisits -= 1

            # These fields set on measurement that is cancelled and doesn't have a previous good measurement
            if (
                obj.status
                and obj.status == PhysicalMeasurementsStatus.CANCELLED
                and not self.has_uncancelled_pm(session, participant)
            ):

                participant_summary.clinicPhysicalMeasurementsStatus = PhysicalMeasurementsStatus.CANCELLED
                participant_summary.clinicPhysicalMeasurementsTime = None
                participant_summary.clinicPhysicalMeasurementsFinalizedTime = None
                participant_summary.clinicPhysicalMeasurementsFinalizedSiteId = None

            # These fields set on any measurement not cancelled
            elif obj.status != PhysicalMeasurementsStatus.CANCELLED:
                # new PM or if a PM was restored, it is complete again.
                participant_summary.clinicPhysicalMeasurementsStatus = PhysicalMeasurementsStatus.COMPLETED
                participant_summary.clinicPhysicalMeasurementsTime = obj.created
                participant_summary.clinicPhysicalMeasurementsFinalizedTime = obj.finalized
                participant_summary.clinicPhysicalMeasurementsCreatedSiteId = obj.createdSiteId
                participant_summary.clinicPhysicalMeasurementsFinalizedSiteId = obj.finalizedSiteId
                if is_distinct_visit and not is_amendment:
                    participant_summary.numberDistinctVisits += 1

            elif (
                obj.status
                and obj.status == PhysicalMeasurementsStatus.CANCELLED
                and self.has_uncancelled_pm(session, participant)
            ):

                get_latest_pm = self.get_latest_pm(session, participant)
                participant_summary.clinicPhysicalMeasurementsFinalizedTime = get_latest_pm.finalized
                participant_summary.clinicPhysicalMeasurementsTime = get_latest_pm.created
                participant_summary.clinicPhysicalMeasurementsCreatedSiteId = get_latest_pm.createdSiteId
                participant_summary.clinicPhysicalMeasurementsFinalizedSiteId = get_latest_pm.finalizedSiteId
        else:
            participant_summary.selfReportedPhysicalMeasurementsStatus = \
                SelfReportedPhysicalMeasurementsStatus.COMPLETED
            participant_summary.selfReportedPhysicalMeasurementsAuthored = obj.finalized

        participant_summary_dao.update_enrollment_status(participant_summary, session=session)
        session.merge(participant_summary)

        return participant_summary

    def get_latest_pm(self, session, participant):
        return (
            session.query(PhysicalMeasurements)
            .filter_by(participantId=participant.participantId)
            .filter(PhysicalMeasurements.finalized != None)
            .order_by(PhysicalMeasurements.finalized.desc())
            .first()
        )

    def has_uncancelled_pm(self, session, participant):
        """return True if participant has at least one physical measurement that is not cancelled"""
        query = (
            session.query(PhysicalMeasurements.status)
            .filter_by(participantId=participant.participantId)
            .filter(PhysicalMeasurements.finalized != None)
            .all()
        )
        valid_pm = False
        for pm in query:
            if pm.status != PhysicalMeasurementsStatus.CANCELLED:
                valid_pm = True

        return valid_pm

    def insert(self, obj):
        if obj.physicalMeasurementsId:
            return super(PhysicalMeasurementsDao, self).insert(obj)
        return self._insert_with_random_id(obj, ["physicalMeasurementsId"])

    def _update_amended(self, obj, extension, url, session):
        """Finds the measurements that are being amended; sets the resource status to 'amended',
        the 'final' flag to False, and sets the new measurements' amendedMeasurementsId field to
        its ID."""
        value_ref = extension.get("valueReference")
        if value_ref is None:
            raise BadRequest(f"No valueReference in extension {url}.")
        ref = value_ref.get("reference")
        if ref is None:
            raise BadRequest(f"No reference in extension {url}.")
        type_name, ref_id = ref.split("/")
        if type_name != "PhysicalMeasurements":
            raise BadRequest(f"Bad reference type in extension {url}: {ref}.")

        try:
            amended_measurement_id = int(ref_id)
        except ValueError:
            raise BadRequest(f"Invalid ref id: {ref_id}")

        amended_measurement = self.get_with_session(session, amended_measurement_id)
        if amended_measurement is None:
            raise BadRequest(f"Amendment references unknown PhysicalMeasurement {ref_id}.")
        amended_resource_json, composition = self.load_record_fhir_doc(amended_measurement)
        composition["status"] = "amended"
        amended_measurement.final = False
        amended_measurement = self.store_record_fhir_doc(amended_measurement, amended_resource_json)
        session.merge(amended_measurement)
        obj.amendedMeasurementsId = amended_measurement_id


    def update_with_patch(self, id_, session, resource):
        record = self.get_with_children_with_session(session, id_, for_update=True)
        return self._do_update_with_patch(session, record, resource)

    def patch(self, id_, resource, p_id):
        # pylint: disable=unused-argument
        with self.session() as session:
            # resource = request.get_json(force=True)
            order = self.update_with_patch(id_, session, resource)
            return self.to_client_json(order)

    def _do_update_with_patch(self, session, record, resource):
        self._validate_patch_update(record, resource)
        if resource["status"].lower() == "cancelled":
            record = self._cancel_record(record, resource)

        if resource["status"].lower() == "restored":
            record = self._restore_record(record, resource)

        logging.info(f"{resource['status']} physical measurement {record.physicalMeasurementsId}.")

        super(PhysicalMeasurementsDao, self)._do_update(session, record, record)
        self._update_participant_summary(session, record)

        return record

    @staticmethod
    def make_measurement_id(physical_measurements_id, measurement_count):
        # To generate unique IDs for measurements that are randomly distributed for different
        # participants (without having to randomly insert and check for the existence of IDs for each
        # measurement row), we multiply the parent physical measurements ID (nine digits) by 1000 and
        # add the measurement count within physical_measurements. This must not reach 1000 to avoid
        # collisions; log an error if we start getting anywhere close. (We don't expect to.)
        assert measurement_count < 1000
        if measurement_count == 900:
            logging.error("measurement_count > 900; nearing limit of 1000.")
        return (physical_measurements_id * 1000) + measurement_count

    @staticmethod
    def get_preferred_coding(codeable_concept):
        """Extract the code with the PMI system, if there is one."""
        pm_coding = None
        for coding in codeable_concept.coding:
            if pm_coding is None:
                pm_coding = coding
            elif coding.system.startswith(_PM_SYSTEM_PREFIX):
                if pm_coding.system.startswith(_PM_SYSTEM_PREFIX):
                    raise BadRequest(f"Multiple measurement codes starting system {_PM_SYSTEM_PREFIX}")
                pm_coding = coding
        return pm_coding

    @staticmethod
    def from_component(observation, component):
        if not component.code or not component.code.coding:
            logging.warning(f"Skipping component without coding: {component.as_json()}")
            return None
        value_string = None
        value_decimal = None
        value_unit = None
        value_code_system = None
        value_code_value = None
        value_date_time = None
        if component.valueQuantity:
            value_decimal = component.valueQuantity.value
            value_unit = component.valueQuantity.code
        if component.valueDateTime:
            value_date_time = component.valueDateTime.date
        if component.valueString:
            value_string = component.valueString
            if len(value_string) > _BYTE_LIMIT:
                raise BadRequest("Component notes field exceeds limit.")
        if component.valueCodeableConcept and component.valueCodeableConcept.coding:
            value_coding = PhysicalMeasurementsDao.get_preferred_coding(component.valueCodeableConcept)
            value_code_system = value_coding.system
            value_code_value = value_coding.code
        pm_coding = PhysicalMeasurementsDao.get_preferred_coding(component.code)
        return Measurement(
            codeSystem=pm_coding.system,
            codeValue=pm_coding.code,
            measurementTime=observation.effectiveDateTime.date,
            valueString=value_string,
            valueDecimal=value_decimal,
            valueUnit=value_unit,
            valueCodeSystem=value_code_system,
            valueCodeValue=value_code_value,
            valueDateTime=value_date_time,
        )

    @staticmethod
    def from_observation(observation, full_url, qualifier_map, first_pass):
        if first_pass:
            if observation.related:
                # Skip anything with a related observation on the first pass.
                return None
        else:
            if not observation.related:
                # Skip anything *without* a related observation on the second pass.
                return None
        if not observation.effectiveDateTime:
            logging.warning(f"Skipping observation without effectiveDateTime: {observation.as_json()}")
            return None
        if not observation.code or not observation.code.coding:
            logging.warning(f"Skipping observation without coding: {observation.as_json()}")
            return None
        body_site_code_system = None
        body_site_code_value = None
        value_string = None
        value_decimal = None
        value_unit = None
        value_code_system = None
        value_code_value = None
        value_code_description = None
        value_date_time = None
        if observation.bodySite and observation.bodySite.coding:
            body_site_coding = PhysicalMeasurementsDao.get_preferred_coding(observation.bodySite)
            body_site_code_system = body_site_coding.system
            body_site_code_value = body_site_coding.code
        if observation.valueQuantity:
            value_decimal = observation.valueQuantity.value
            value_unit = observation.valueQuantity.code
        if observation.valueDateTime:
            value_date_time = observation.valueDateTime.date.replace(tzinfo=None)
        if observation.valueString:
            value_string = observation.valueString
            if len(value_string) > _BYTE_LIMIT:
                raise BadRequest("Observation notes field exceeds limit.")
        if observation.valueCodeableConcept and observation.valueCodeableConcept.coding:
            value_coding = PhysicalMeasurementsDao.get_preferred_coding(observation.valueCodeableConcept)
            value_code_system = value_coding.system
            value_code_value = value_coding.code

            value_code_description = observation.valueCodeableConcept.text
            desc_char_count = len(value_code_description)
            char_limit = Measurement.valueCodeDescription.type.length
            if desc_char_count > char_limit:
                logging.warning(f'Truncating codeable concept description of length {desc_char_count}')
                value_code_description = value_code_description[:char_limit]

        measurements = []
        if observation.component:
            for component in observation.component:
                child = PhysicalMeasurementsDao.from_component(observation, component)
                if child:
                    measurements.append(child)
        qualifiers = []
        if observation.related:
            for related in observation.related:
                if related.type == _QUALIFIED_BY_RELATED_TYPE and related.target and related.target.reference:
                    qualifier = qualifier_map.get(related.target.reference)
                    if qualifier:
                        qualifiers.append(qualifier)
                    else:
                        logging.warning(f"Could not find qualifier {related.target.reference}")
        pm_coding = PhysicalMeasurementsDao.get_preferred_coding(observation.code)
        result = Measurement(
            codeSystem=pm_coding.system,
            codeValue=pm_coding.code,
            measurementTime=observation.effectiveDateTime.date.replace(tzinfo=None),
            bodySiteCodeSystem=body_site_code_system,
            bodySiteCodeValue=body_site_code_value,
            valueString=value_string,
            valueDecimal=value_decimal,
            valueUnit=value_unit,
            valueCodeSystem=value_code_system,
            valueCodeValue=value_code_value,
            valueCodeDescription=value_code_description,
            valueDateTime=value_date_time,
            measurements=measurements,
            qualifiers=qualifiers,
        )
        if first_pass:
            qualifier_map[full_url] = result
        return result

    @staticmethod
    def get_location_site_id(location_value):
        if not location_value.startswith(_LOCATION_PREFIX):
            logging.warning(f"Invalid location: {location_value}")
            return None
        google_group = location_value[len(_LOCATION_PREFIX) :]
        site = SiteDao().get_by_google_group(google_group)
        if not site:
            logging.warning(f"Unknown site: {google_group}")
            return None
        return site.siteId

    @staticmethod
    def get_author_username(author_value):
        if not author_value.startswith(_AUTHOR_PREFIX):
            logging.warning(f"Invalid author: {author_value}")
            return None
        return author_value[len(_AUTHOR_PREFIX) :]

    @staticmethod
    def get_authoring_step(extension):
        url = extension.get("url")
        if url == _AUTHORING_STEP:
            return extension.get("valueCode")
        return None

    def to_client_json(self, model):
        # pylint: disable=unused-argument
        """Converts the given model to a JSON object to be returned to API clients.
        Subclasses must implement this unless their model store a model.resource attribute.
        """

        doc, composition = self.load_record_fhir_doc(model)  # pylint: disable=unused-variable

        doc['collectType'] = str(model.collectType if model.collectType is not None else
                                 PhysicalMeasurementsCollectType.UNSET)
        doc['originMeasurementUnit'] = str(model.originMeasurementUnit if model.originMeasurementUnit is not None else
                                           OriginMeasurementUnit.UNSET)
        doc['origin'] = model.origin

        return doc

    def from_client_json(self, resource_json, participant_id=None, **unused_kwargs):
        # pylint: disable=unused-argument
        measurements = []
        observations = []
        qualifier_map = {}
        created_site_id = None
        created_username = None
        finalized_site_id = None
        finalized_username = None
        for entry in resource_json["entry"]:
            resource = entry.get("resource")
            if resource:
                resource_type = resource.get("resourceType")
                if resource_type == _OBSERVATION_RESOURCE_TYPE:
                    observations.append((entry["fullUrl"], fhir_observation.Observation(resource)))
                elif resource_type == _COMPOSITION_RESOURCE_TYPE:
                    extensions = resource.get("extension", [])
                    if not extensions:
                        logging.warning("No extensions in composition resource (expected site info).")
                    for extension in extensions:
                        # DA-1499 convert to 'valueString' key value instead of 'valueReference'.
                        value_reference = extension.get("valueString")
                        if not value_reference:
                            value_reference = extension.get("valueReference")
                        if value_reference:
                            url = extension.get("url")
                            if url == _CREATED_LOC_EXTENSION:
                                created_site_id = PhysicalMeasurementsDao.get_location_site_id(value_reference)
                            elif url == _FINALIZED_LOC_EXTENSION:
                                finalized_site_id = PhysicalMeasurementsDao.get_location_site_id(value_reference)
                            elif url not in _ALL_EXTENSIONS:
                                logging.warning(
                                    f"Unrecognized extension URL: {url} (should be one of {_ALL_EXTENSIONS})"
                                )
                        else:
                            logging.warning(f"No valueReference in extension, skipping: {extension}")
                    authors = resource.get("author")
                    for author in authors:
                        author_extension = author.get("extension")
                        # DA-1435 Support author extension as both an object and an array of objects.
                        # Convert object to list to meet FHIR spec.
                        if author_extension and not isinstance(author_extension, list):
                            new_ae = list()
                            new_ae.append(author_extension)
                            author_extension = author['extension'] = new_ae
                        reference = author.get("reference")
                        if author_extension and reference:
                            authoring_step = PhysicalMeasurementsDao.get_authoring_step(author_extension[0])
                            if authoring_step == _FINALIZED_STATUS:
                                finalized_username = PhysicalMeasurementsDao.get_author_username(reference)
                            elif authoring_step == _CREATED_STATUS:
                                created_username = PhysicalMeasurementsDao.get_author_username(reference)
                else:
                    logging.warning(
                        f"Unrecognized resource type (expected {_OBSERVATION_RESOURCE_TYPE} \
                        or {_COMPOSITION_RESOURCE_TYPE}), skipping: {resource_type}"
                    )

        # Take two passes over the observations; once to find all the qualifiers and observations
        # without related qualifiers, and a second time to find all observations with related
        # qualifiers.
        for first_pass in [True, False]:
            for fullUrl, observation in observations:
                measurement = PhysicalMeasurementsDao.from_observation(observation, fullUrl, qualifier_map, first_pass)
                if measurement:
                    measurements.append(measurement)
        record = PhysicalMeasurements(
            participantId=participant_id,
            measurements=measurements,
            createdSiteId=created_site_id,
            createdUsername=created_username,
            finalizedSiteId=finalized_site_id,
            finalizedUsername=finalized_username,
            origin='hpro',
            collectType=PhysicalMeasurementsCollectType.SITE,
            originMeasurementUnit=OriginMeasurementUnit.UNSET
        )
        record = self.store_record_fhir_doc(record, resource_json)
        return record

    def _validate_patch_update(self, measurement, resource):
        """validates request of resource"""
        cancelled_required_fields = ["status", "reason", "cancelledInfo"]
        restored_required_fields = ["status", "reason", "restoredInfo"]

        if resource.get("status").lower() == "cancelled":
            if measurement.status == PhysicalMeasurementsStatus.CANCELLED:
                raise BadRequest("This order is already cancelled")
            for field in cancelled_required_fields:
                if field not in resource:
                    raise BadRequest(f"{field} is required in cancel request.")

        elif resource.get("status").lower() == "restored":
            if measurement.status != PhysicalMeasurementsStatus.CANCELLED:
                raise BadRequest("Can not restore an order that is not cancelled.")
            for field in restored_required_fields:
                if field not in resource:
                    raise BadRequest(f"{field} is required in restore request.")
        else:
            raise BadRequest("status is required in restore request.")

    def _get_patch_args(self, resource):
        """
        returns author and site based on resource cancelledInfo/restoredInfo. Validation that
        these exists is handled by _validate_patch_update
        :param resource: Request JSON Payload
        :return: Tuple (site_id, author, reason)
        """
        site_id = None
        author = None
        reason = resource.get("reason", None)

        if "cancelledInfo" in resource:
            site_id = self.get_location_site_id(_LOCATION_PREFIX + resource["cancelledInfo"]["site"]["value"])
            author = self.get_author_username(_AUTHOR_PREFIX + resource["cancelledInfo"]["author"]["value"])

        elif "restoredInfo" in resource:
            site_id = self.get_location_site_id(_LOCATION_PREFIX + resource["restoredInfo"]["site"]["value"])
            author = self.get_author_username(_AUTHOR_PREFIX + resource["restoredInfo"]["author"]["value"])

        return site_id, author, reason

    def set_self_reported_pm_resource_json(self, record):
        doc = {
            "id": record.physicalMeasurementsId,
            "type": "document",
            "resourceType": "Bundle",
            "entry": []

        }

        coding_map = {
            'height': {
                "text": "Height",
                "coding": [
                    {
                        "code": "8302-2",
                        "system": "http://loinc.org",
                        "display": "Body height"
                    },
                    {
                        "code": "height",
                        "system": "http://terminology.pmi-ops.org/CodeSystem/physical-measurements",
                        "display": "Height"
                    }
                ]
            },
            'weight': {
                "text": "Weight",
                "coding": [
                    {
                        "code": "29463-7",
                        "system": "http://loinc.org",
                        "display": "Body weight"
                    },
                    {
                        "code": "weight",
                        "system": "http://terminology.pmi-ops.org/CodeSystem/physical-measurements",
                        "display": "Weight"
                    }
                ]
            }
        }

        for measurement in record.measurements:
            entry = {
                "fullUrl": "",
                "resource": {
                    "code": coding_map.get(measurement.codeValue, None),
                    "status": "final",
                    "subject": {
                        "reference": "Patient/P" + str(record.participantId)
                    },
                    "resourceType": "Observation",
                    "valueQuantity": {
                        "code": measurement.valueUnit,
                        "unit": measurement.valueUnit,
                        "value": measurement.valueDecimal,
                        "system": "http://unitsofmeasure.org"
                    },
                    "effectiveDateTime": record.finalized.strftime('%Y-%m-%dT%H:%M:%S')
                }
            }
            doc['entry'].append(entry)

        record.resource = doc

    @staticmethod
    def load_record_fhir_doc(record):
        """
        Retrieve the FHIR document from the DB record.
        :param record: Measurement DB record
        :return: Tuple (FHIR document dict, Composition entry)
        """
        # DA-1435 Support old/new resource field type
        if str(PhysicalMeasurements.resource.property.columns[0].type) == 'JSON':
            doc = record.resource
        else:
            doc = json.loads(record.resource)

        composition = None
        entries = doc.get('entry', list())

        for entry in entries:
            resource = entry.get('resource', None)
            if resource and 'resourceType' in resource and resource['resourceType'].lower() == 'composition':
                composition = resource

        return doc, composition

    @staticmethod
    def store_record_fhir_doc(record, doc):
        """
        Store the FHIR document into the DB record.
        :param record: Measurement DB record
        :param doc: FHIR document dict
        :return: Measurement DB record
        """
        if isinstance(doc, str):
            doc = json.loads(doc)
        # DA-1435 Support old/new resource field type
        if str(PhysicalMeasurements.resource.property.columns[0].type) == 'JSON':
            record.resource = doc
        else:
            record.resource = json.dumps(doc)

        # sqlalchemy does not mark the 'resource' field as dirty, we need to force it.
        flag_modified(record, 'resource')

        return record

    def _cancel_record(self, record, resource):
        """
        Cancel the Physical Measurements record.
        :param record: Measurement DB record
        :param resource: Request JSON payload
        :return: Measurement record
        """
        site_id, author, reason = self._get_patch_args(resource)
        record.cancelledUsername = author
        record.cancelledSiteId = site_id
        record.reason = reason
        record.cancelledTime = clock.CLOCK.now()
        record.status = PhysicalMeasurementsStatus.CANCELLED
        record.createdSiteId = None
        record.finalizedSiteId = None
        record.finalized = None

        doc, composition = self.load_record_fhir_doc(record)

        composition['status'] = 'entered-in-error'
        # remove all restored entries if found
        extensions = list()
        for ext in composition['extension']:
            if 'restore' not in ext['url']:
                extensions.append(ext)

        extensions.append({
            'url': 'http://terminology.pmi-ops.org/StructureDefinition/cancelled-site',
            'valueInteger': site_id
        })
        extensions.append({
            'url': 'http://terminology.pmi-ops.org/StructureDefinition/cancelled-time',
            'valueString': record.cancelledTime.isoformat()
        })
        extensions.append({
            'url': 'http://terminology.pmi-ops.org/StructureDefinition/cancelled-username',
            'valueString': author
        })
        extensions.append({
            'url': 'http://terminology.pmi-ops.org/StructureDefinition/cancelled-reason',
            'valueString': reason
        })

        composition['extension'] = extensions

        record = self.store_record_fhir_doc(record, doc)
        return record

    def _restore_record(self, record, resource):
        """
        Restore a cancelled Physical Measurements record.
        :param record: Measurement DB record
        :param resource: Request JSON payload
        :return: Measurement record
        """
        site_id, author, reason = self._get_patch_args(resource)
        record.cancelledUsername = None
        record.cancelledSiteId = None
        record.cancelledTime = None
        record.reason = reason
        record.status = PhysicalMeasurementsStatus.UNSET
        record.createdSiteId = site_id
        record.finalizedSiteId = site_id
        record.finalizedUsername = author
        # get original finalized time
        record.finalized = self.get_date_from_pm_resource(record.participantId,
                                                          record.physicalMeasurementsId)

        doc, composition = self.load_record_fhir_doc(record)

        composition['status'] = 'final'
        # remove all cancel entries if found
        extensions = list()
        for ext in composition['extension']:
            if 'cancel' not in ext['url']:
                extensions.append(ext)

        extensions.append({
            'url': 'http://terminology.pmi-ops.org/StructureDefinition/restore-site',
            'valueInteger': site_id
        })
        extensions.append({
            'url': 'http://terminology.pmi-ops.org/StructureDefinition/restore-time',
            'valueString': clock.CLOCK.now().isoformat()
        })
        extensions.append({
            'url': 'http://terminology.pmi-ops.org/StructureDefinition/restore-username',
            'valueString': author
        })
        extensions.append({
            'url': 'http://terminology.pmi-ops.org/StructureDefinition/restore-reason',
            'valueString': reason
        })

        composition['extension'] = extensions

        record = self.store_record_fhir_doc(record, doc)
        return record
