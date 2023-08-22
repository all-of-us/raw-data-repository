import logging
from typing import Optional

from flask_restful import Resource
from werkzeug.exceptions import BadRequest

from rdr_service.api.base_api import ApiUtilMixin
from rdr_service.api_util import PTC
from rdr_service.app_util import auth_required
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.pediatric_data_log_dao import PediatricDataLogDao
from rdr_service.fhir_utils import find_extension
from rdr_service.model.pediatric_data_log import PediatricDataLog, PediatricDataType
from rdr_service.model.utils import from_client_participant_id
from rdr_service.lib_fhir.fhirclient_4_0_0.models.contactpoint import ContactPoint
from rdr_service.lib_fhir.fhirclient_4_0_0.models.patient import Patient as FhirPatient
from rdr_service.participant_enums import PediatricAgeRange
from rdr_service.repository.profile_update_repository import ProfileUpdateRepository
from rdr_service.services.ancillary_studies.study_enrollment import EnrollmentInterface
from rdr_service.model.base import InvalidDataState


PEDIATRIC_AGE_RANGE_EXTENSION = 'https://pmi-fhir-ig.github.io/pmi-fhir-ig/StructureDefinition/child-account-type'


class PatientPayload:
    def __init__(self, json):
        self.source = json
        self._fhir_patient = FhirPatient(json)

    @property
    def participant_id(self):
        return from_client_participant_id(self._fhir_patient.id)

    @property
    def has_first_name_update(self):
        return self._has_value_provided(
            field_name='given',
            path_to_field=['name'],
            json=self.source
        )

    @property
    def first_name(self):
        name_list = self._fhir_patient.name
        if not name_list:
            return None

        name_object = name_list[0]
        if not name_object.given:
            return None

        return name_object.given[0] or None

    @property
    def has_middle_name_update(self):
        if self._has_value_provided(
            field_name='given',
            path_to_field=['name'],
            json=self.source
        ):
            name_list = self.source['name']
            if not name_list:
                return True

            namesource = name_list[0]
            provided_name_list = namesource['given']
            return provided_name_list and len(provided_name_list) > 1

        return False

    @property
    def middle_name(self):
        name_list = self._fhir_patient.name
        if not name_list:
            return None

        name_object = name_list[0]
        if not name_object.given or len(name_object.given) < 2:
            return None

        return name_object.given[1] or None

    @property
    def has_last_name_update(self):
        return self._has_value_provided(
            field_name='family',
            path_to_field=['name'],
            json=self.source
        )

    @property
    def last_name(self):
        name_list = self._fhir_patient.name
        if not name_list:
            return None

        name_object = name_list[0]
        return name_object.family or None

    @property
    def has_phone_number_update(self):
        if not self._fhir_patient.telecom:
            return False

        return any([
            telecom_object.system == 'phone' and not self._is_verified(telecom_object)
            for telecom_object in self._fhir_patient.telecom
        ])

    @property
    def phone_number(self):
        for telecom_object in self._fhir_patient.telecom:
            if telecom_object.system == 'phone' and not self._is_verified(telecom_object):
                return telecom_object.value or None

    @property
    def has_login_phone_number_update(self):
        """
        Login phone number looks the same as a phone number, but it's verified.
        """
        if not self._fhir_patient.telecom:
            return False

        return any([
            telecom_object.system == 'phone' and self._is_verified(telecom_object)
            for telecom_object in self._fhir_patient.telecom
        ])

    @property
    def login_phone_number(self):
        for telecom_object in self._fhir_patient.telecom:
            if telecom_object.system == 'phone' and self._is_verified(telecom_object):
                return telecom_object.value or None

    @classmethod
    def _is_verified(cls, contact_point: ContactPoint):
        """
        Given a ContactPoint (such as an email address or phone number),
        return whether it is verified (defaulting to False).
        """
        return any([
            'pmi-verified' in extension.url and getattr(extension, 'valueBoolean', False)
            for extension in contact_point.extension or []
        ])

    @property
    def has_email_update(self):
        if not self._fhir_patient.telecom:
            return False

        return any([telecom_object.system == 'email' for telecom_object in self._fhir_patient.telecom])

    @property
    def email(self):
        for telecom_object in self._fhir_patient.telecom:
            if telecom_object.system == 'email':
                return telecom_object.value or None

    @property
    def has_birthdate_update(self):
        return 'birthDate' in self.source

    @property
    def birthdate(self):
        fhir_date = self._fhir_patient.birthDate
        if not fhir_date:
            return None

        return fhir_date.date

    @property
    def has_address_line1_update(self):
        if not self._fhir_patient.address:
            return False

        address_object = self._fhir_patient.address[0]
        return address_object.line is not None

    @property
    def address_line1(self):
        address_line_list = self._fhir_patient.address[0].line
        if not address_line_list:
            return None
        return address_line_list[0] or None

    @property
    def has_address_line2_update(self):
        return self.has_address_line1_update

    @property
    def address_line2(self):
        address_line_list = self._fhir_patient.address[0].line
        if not address_line_list or len(address_line_list) < 2:
            return None

        return address_line_list[1] or None

    @property
    def has_address_city_update(self):
        return self._has_value_provided(
            field_name='city',
            path_to_field=['address'],
            json=self.source
        )

    @property
    def address_city(self):
        return self._fhir_patient.address[0].city or None

    @property
    def has_address_state_update(self):
        return self._has_value_provided(
            field_name='state',
            path_to_field=['address'],
            json=self.source
        )

    @property
    def address_state(self):
        return self._fhir_patient.address[0].state or None

    @property
    def has_address_zip_code_update(self):
        return self._has_value_provided(
            field_name='postalCode',
            path_to_field=['address'],
            json=self.source
        )

    @property
    def address_zip_code(self):
        return self._fhir_patient.address[0].postalCode or None

    @property
    def has_language_update(self):
        return 'communication' in self.source

    @property
    def has_ancillary_identifier(self):
        if hasattr(self._fhir_patient, "identifier"):
            if self._fhir_patient.identifier:
                for item in self._fhir_patient.identifier:
                    if "PMIIdentifierTypeCS" in item.type.coding[0].system:
                        return True

    @property
    def ancillary_study_code(self):
        for item in self._fhir_patient.identifier:
            if "PMIIdentifierTypeCS" in item.type.coding[0].system:
                return item.type.coding[0].code

    @property
    def ancillary_study_pid(self):
        for item in self._fhir_patient.identifier:
            if "PMIIdentifierTypeCS" in item.type.coding[0].system:
                return item.value

    @property
    def ancillary_event_authored_time(self):
        if hasattr(self._fhir_patient, "contained"):
            for item in self._fhir_patient.contained:
                if hasattr(item, "recorded"):
                    return item.recorded.date.isoformat(timespec='milliseconds')

    @property
    def preferred_language(self):
        communication_list = self._fhir_patient.communication

        preferred_communication = None
        for communication in communication_list:
            if communication.preferred:
                preferred_communication = communication
                break

        if not preferred_communication and len(communication_list) == 1:
            preferred_communication = communication_list[0]

        if preferred_communication:
            return preferred_communication.language.coding[0].code

        return None

    @property
    def pediatric_age_range(self) -> Optional[PediatricAgeRange]:
        age_range_extension = find_extension(
            comparator=lambda extension: extension.url == PEDIATRIC_AGE_RANGE_EXTENSION,
            container=self._fhir_patient
        )

        if not age_range_extension:
            return None

        age_range_str = age_range_extension.valueCode
        if age_range_str == 'UNSET':
            # non-pediatric participants will get UNSET sent as their age range.
            # No need to record or log these as an error
            return None
        if age_range_str not in PediatricAgeRange.names():
            logging.error(f'Unrecognized age range value "{age_range_str}"')
            return None

        return PediatricAgeRange(age_range_extension.valueCode)

    @classmethod
    def _has_value_provided(cls, field_name, path_to_field, json):
        """
        Determine if the provided JSON payload has a value specified for a given field,
        or if the field was missing entirely (which would mean there should be no update for that field).
        """

        # If there is no path to the field, then it's not nested and should be searched for at the root of the json
        if not path_to_field:
            return field_name in json

        # If there is a path to a field provided, then we should first check to see if the first item in the path
        # is available on the current json. If it's not then we haven't been given a value.
        next_path_field = path_to_field[0]
        if next_path_field not in json:
            return False

        # If the next path structure is explicitly given a value, but it's been cleared then we can assume anything
        # specified within it should be cleared too.
        next_nested_json = json[next_path_field]
        if not next_nested_json:
            return True

        # The "name" and "address structures are lists with one object in them,
        # in that case we should pass the object in it rather than the list
        if next_path_field in ['name', 'address']:
            next_nested_json = next_nested_json[0]

        # Recursively return whether the nested json has the value
        return cls._has_value_provided(
            field_name=field_name,
            path_to_field=path_to_field[1:],
            json=next_nested_json
        )


class ProfileUpdateApi(Resource, ApiUtilMixin):
    @auth_required(PTC)
    def post(self):
        json = self.get_request_json()
        update_payload = PatientPayload(json)
        self._process_request(update_payload)
        self._record_request(update_payload)
        return json

    @classmethod
    def _process_request(cls, update_payload: PatientPayload):
        update_field_list = {
            'participant_id': update_payload.participant_id
        }

        if update_payload.has_first_name_update:
            update_field_list['first_name'] = update_payload.first_name
        if update_payload.has_middle_name_update:
            update_field_list['middle_name'] = update_payload.middle_name
        if update_payload.has_last_name_update:
            update_field_list['last_name'] = update_payload.last_name
        if update_payload.has_phone_number_update:
            update_field_list['phone_number'] = update_payload.phone_number
        if update_payload.has_login_phone_number_update:
            update_field_list['login_phone_number'] = update_payload.login_phone_number
        if update_payload.has_email_update:
            update_field_list['email'] = update_payload.email
        if update_payload.has_birthdate_update:
            update_field_list['birthdate'] = update_payload.birthdate
        if update_payload.has_address_line1_update:
            update_field_list['address_line1'] = update_payload.address_line1
        if update_payload.has_address_line2_update:
            update_field_list['address_line2'] = update_payload.address_line2
        if update_payload.has_address_city_update:
            update_field_list['address_city'] = update_payload.address_city
        if update_payload.has_address_state_update:
            update_field_list['address_state'] = update_payload.address_state
        if update_payload.has_address_zip_code_update:
            update_field_list['address_zip_code'] = update_payload.address_zip_code
        if update_payload.has_language_update:
            update_field_list['preferred_language'] = update_payload.preferred_language

        try:
            ParticipantSummaryDao.update_profile_data(**update_field_list)
        except InvalidDataState as exc:
            logging.error('Data error encountered', exc_info=True)
            raise BadRequest('Invalid data state encountered. Please verify request data.') from exc

        # Handle Ancillary Study Enrollment
        if update_payload.has_ancillary_identifier:
            study_interface = EnrollmentInterface(update_payload.ancillary_study_code)
            study_interface.create_study_participant(
                aou_pid=update_field_list['participant_id'],
                ancillary_pid=update_payload.ancillary_study_pid,
                event_authored_time=update_payload.ancillary_event_authored_time
            )

        # Handle pediatric date range update
        if update_payload.pediatric_age_range:
            PediatricDataLogDao.insert(
                PediatricDataLog(
                    participant_id=update_payload.participant_id,
                    data_type=PediatricDataType.AGE_RANGE,
                    value=str(update_payload.pediatric_age_range)
                )
            )

    @classmethod
    def _record_request(cls, update_payload: PatientPayload):
        repository = ProfileUpdateRepository()
        repository.store_update_json(
            participant_id=update_payload.participant_id,
            json=update_payload.source
        )
