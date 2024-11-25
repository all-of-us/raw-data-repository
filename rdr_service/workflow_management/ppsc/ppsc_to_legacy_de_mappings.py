from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.participant_enums import QuestionnaireStatus

def map_source_to_summary(record: dict, data_element_mapping: dict) -> ParticipantSummary:
    """
        Maps source data to a ParticipantSummary object using the data_element_mapping definition.

        Args:
            record: The source data row containing participant_id and data elements.
            data_element_mapping: Mapping of source fields to ParticipantSummary fields.

        Returns:
            ParticipantSummary: The mapped ParticipantSummary object.
    """
    participant_id = record["participant_id"]
    participant_summary = ParticipantSummary(participantId=participant_id)

    for source_field, mapping in data_element_mapping.items():
        if source_field in record and record[source_field] is not None:
            target_field = mapping["field"]
            value_mapping = mapping["value"]

            if isinstance(value_mapping, dict):  # Handle value transformation
                transformed_value = value_mapping.get(record[source_field].lower())
            elif value_mapping == "date_string":  # Handle date strings
                transformed_value = record[source_field]
            elif value_mapping == "string":  # Handle strings strings
                transformed_value = record[source_field]
            else:
                transformed_value = record[source_field]

            # Dynamically set the field on the ParticipantSummary object
            if target_field is None:
                print("paused")
            setattr(participant_summary, target_field.key, transformed_value)

    return participant_summary

consent_data_elements = {
    "primary_consent": {
        "field": ParticipantSummary.consentForStudyEnrollment,
        "value": {
            "yes": QuestionnaireStatus.SUBMITTED,
            "no": QuestionnaireStatus.SUBMITTED_NO_CONSENT
        }
    },
    "primary_consent_event_authored": {
        "field": ParticipantSummary.consentForStudyEnrollmentAuthored,
        "value": "date_string"
    },
    "ehr_authorization": {
        "field": ParticipantSummary.consentForElectronicHealthRecords,
        "value": {
            "yes": QuestionnaireStatus.SUBMITTED,
            "no": QuestionnaireStatus.SUBMITTED_NO_CONSENT
        }
    },
    "ehr_authorization_event_authored": {
        "field": ParticipantSummary.consentForElectronicHealthRecordsAuthored,
        "value": "date_string"
    }
}

profile_updates_data_elements = {
    "piiname_first": {
        "field": ParticipantSummary.firstName,
        "value": "string"  # Direct mapping, no transformation needed
    },
    "piiname_middle": {
        "field": ParticipantSummary.middleName,
        "value": "string"
    },
    "piiname_last": {
        "field": ParticipantSummary.lastName,
        "value": "string"
    },
    "streetaddress_piizip": {
        "field": ParticipantSummary.zipCode,
        "value": "string"
    },
    # Skipping state implementation for now
    # "streetaddress_piistate": {
    #     "field": ParticipantSummary.stateId,
    #     "value": "string"
    # },
    "streetaddress_piicity": {
        "field": ParticipantSummary.city,
        "value": "string"
    },
    "piiaddress_streetaddress": {
        "field": ParticipantSummary.streetAddress,
        "value": "string"
    },
    "piiaddress_streetaddress2": {
        "field": ParticipantSummary.streetAddress2,
        "value": "string"
    },
    "piicontactinformation_phone": {
        "field": ParticipantSummary.phoneNumber,
        "value": "string"
    },
    "piicontactinformation_email": {
        "field": ParticipantSummary.email,
        "value": "string"
    },
    "language_preference": {
        "field": ParticipantSummary.primaryLanguage,
        "value": "string"
    },
    "piibirthinformation_birthdate": {
        "field": ParticipantSummary.dateOfBirth,
        "value": "date_string"
    }
}
