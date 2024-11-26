from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.participant_enums import QuestionnaireStatus, WithdrawalStatus, WithdrawalReason, SuspensionStatus, \
    DeceasedStatus, RetentionStatus, RetentionType, Race, GenderIdentity


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
            print(source_field)
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

withdrawal_data_elements = {
    "withdrawal_status": {
        "field": ParticipantSummary.withdrawalStatus,
        "value": {
            "withdrawn": WithdrawalStatus.NO_USE,
            "not_withdrawn": WithdrawalStatus.NOT_WITHDRAWN
        }
    },
    "withdrawal_status_authored_time": {
        "field": ParticipantSummary.withdrawalAuthored,
        "value": "date_string"
    },
    "withdrawal_reason": {
        "field": ParticipantSummary.withdrawalReason,
        "value": {
            "duplicate account": WithdrawalReason.DUPLICATE,
            "fraudulent account": WithdrawalReason.FRAUDULENT,
            "hpo/tps requested": WithdrawalReason.FRAUDULENT,
            "other": WithdrawalReason.FRAUDULENT
        }
    }
}

deactivation_data_elements = {
    "deactivation_status": {
        "field": ParticipantSummary.suspensionStatus,
        "value": {
            "deactivated": SuspensionStatus.NO_CONTACT,
            "not_deactivated": SuspensionStatus.NOT_SUSPENDED
        }
    },
    "deactivation_status_time": {
        "field": ParticipantSummary.suspensionTime,
        "value": "date_string"
    }
}

participant_status_data_elements = {
    # Test Account
    "test_account": {
        "field": Participant.isTestParticipant,
        "value": "string"
    },
    # Death
    "deceased_status": {
        "field": ParticipantSummary.deceasedStatus,
        "value": {
            "deceased": DeceasedStatus.APPROVED,
            "accepted": DeceasedStatus.APPROVED,
            "yes": DeceasedStatus.APPROVED,
            "pending": DeceasedStatus.PENDING,
        }
    },
    "deceased_authored": {
        "field": ParticipantSummary.deceasedAuthored,
        "value": "date_string"
    },

    # Retention Status
    "retention_eligible_status": {
        "field": ParticipantSummary.retentionEligibleStatus,
        "value": {
            "eligible": RetentionStatus.ELIGIBLE,
            "not_eligible": RetentionStatus.NOT_ELIGIBLE
        }
    },
    "retention_eligible_status_authored": {
        "field": ParticipantSummary.retentionEligibleTime,
        "value": "date_string"
    },
    "retention_type": {
        "field": ParticipantSummary.retentionType,
        "value": {
            "unset": RetentionType.UNSET,
            "active": RetentionType.ACTIVE,
            "passive": RetentionType.PASSIVE,
            "active and passive": RetentionType.ACTIVE_AND_PASSIVE
        }
    },

    # Enrollment Status
    "participant_time": {
        "field": ParticipantSummary.enrollmentStatusParticipantV3_2Time,
        "value": "date_string"
    },
    "participant_ehr_consent_time": {
        "field": ParticipantSummary.enrollmentStatusParticipantPlusEhrV3_2Time,
        "value": "date_string"
    },
    "enrolled_time": {
        "field": ParticipantSummary.enrollmentStatusEnrolledParticipantV3_2Time,
        "value": "date_string"
    },
    "pmb_eligible_time": {
        "field": ParticipantSummary.enrollmentStatusPmbEligibleV3_2Time,
        "value": "date_string"
    },
    "core_minus_pm_time": {
        "field": ParticipantSummary.enrollmentStatusCoreMinusPmV3_2Time,
        "value": "date_string"
    },
    "core_participant_time": {
        "field": ParticipantSummary.enrollmentStatusCoreV3_2Time,
        "value": "date_string"
    }
}
survey_completion_data_elements = {
    # Basics Data
    "gender_identity": {
        "field": ParticipantSummary.genderIdentity,
        "value": {
            "genderidentity_man": GenderIdentity.GenderIdentity_Man,
            "genderidentity_woman": GenderIdentity.GenderIdentity_Woman,
            "genderidentity_nonbinary": GenderIdentity.GenderIdentity_NonBinary,
            "genderidentity_transgender": GenderIdentity.GenderIdentity_Transgender,
            "genderidentity_additionaloptions": GenderIdentity.GenderIdentity_AdditionalOptions,
            "pmi_prefernottoanswer": GenderIdentity.PMI_PreferNotToAnswer
        }
    },
    "sex": {
        "field": ParticipantSummary.sexId,
        "value": {
            "sexatbirth_male": 302,
            "sexatbirth_female": 303,
            "sexatbirth_intersex": 301,
            "sexatbirth_sexatbirthnoneofthese": 304,
            "pmi_prefernottoanswer": 924
        }
    },
    "sexual_orientation": {
        "field": ParticipantSummary.sexualOrientationId,
        "value": {
            "pmi_prefernottoanswer": 924,
            "sexualorientation_bisexual": 307,
            "sexualorientation_gay": 311,
            "sexualorientation_lesbian": 310,
            "sexualorientation_none": 309,
            "sexualorientation_straight": 308
        }
    },
    "race": {
        "field": ParticipantSummary.race,
        "value": {
            "whatraceethnicity_aian": Race.AMERICAN_INDIAN_OR_ALASKA_NATIVE,
            "whatraceethnicity_asian": Race.ASIAN,
            "whatraceethnicity_black": Race.BLACK_OR_AFRICAN_AMERICAN,
            "whatraceethnicity_hispanic": Race.HISPANIC_LATINO_OR_SPANISH,
            "whatraceethnicity_mena": Race.MIDDLE_EASTERN_OR_NORTH_AFRICAN,
            "whatraceethnicity_nhpi": Race.NATIVE_HAWAIIAN_OR_OTHER_PACIFIC_ISLANDER,
            "whatraceethnicity_white": Race.WHITE,
            "whatraceethnicity_raceethnicitynoneofthese": Race.OTHER_RACE,
            "pmi_prefernottoanswer": Race.PREFER_NOT_TO_SAY
        }
    },
    "education": {
        "field": ParticipantSummary.educationId,
        "value": {
            "highestgrade_advanceddegree": 39,
            "highestgrade_collegegraduate": 33,
            "highestgrade_collegeonetothree": 36,
            "highestgrade_fivethrougheight": 35,
            "highestgrade_neverattended": 38,
            "highestgrade_ninethrougheleven": 32,
            "highestgrade_onethroughfour": 37,
            "highestgrade_twelveorged": 34,
            "pmi_prefernottoanswer": 924
        }
    },
    "income": {
        "field": ParticipantSummary.incomeId,
        "value": {
            "annualincome_100k150k": 292,
            "annualincome_10k25k": 291,
            "annualincome_150k200k": 297,
            "annualincome_25k35k": 293,
            "annualincome_35k50k": 294,
            "annualincome_50k75k": 295,
            "annualincome_75k100k": 290,
            "annualincome_less10k": 296,
            "annualincome_more200k": 298,
            "pmi_prefernottoanswer": 924
        }
    },
    "aian": {  # AIAN specifically
        "field": ParticipantSummary.aian,
        "value": {
            "yes": 1
        }
    },

    # Overall Health
    "questionnaire_on_overall_health": {
        "field": ParticipantSummary.questionnaireOnOverallHealth,
        "value": {
            "submitted_complete": QuestionnaireStatus.SUBMITTED,
            "submitted_incomplete": QuestionnaireStatus.UNSET
        }
    },
    "questionnaire_on_overall_health_authored": {
        "field": ParticipantSummary.questionnaireOnOverallHealthAuthored,
        "value": "date_string"
    },

    # Lifestyle
    "questionnaire_on_lifestyle": {
        "field": ParticipantSummary.questionnaireOnLifestyle,
        "value": {
            "submitted_complete": QuestionnaireStatus.SUBMITTED,
            "submitted_incomplete": QuestionnaireStatus.UNSET
        }
    },
    "questionnaire_on_lifestyle_authored": {
        "field": ParticipantSummary.questionnaireOnLifestyleAuthored,
        "value": "date_string"
    },

    # The Basics
    "questionnaire_on_the_basics": {
        "field": ParticipantSummary.questionnaireOnTheBasics,
        "value": {
            "submitted_complete": QuestionnaireStatus.SUBMITTED,
            "submitted_incomplete": QuestionnaireStatus.UNSET
        }
    },
    "questionnaire_on_the_basics_authored": {
        "field": ParticipantSummary.questionnaireOnTheBasicsAuthored,
        "value": "date_string"
    },

    # Health Care Access
    "questionnaire_on_healthcare_access": {
        "field": ParticipantSummary.questionnaireOnHealthcareAccess,
        "value": {
            "submitted_complete": QuestionnaireStatus.SUBMITTED,
            "submitted_incomplete": QuestionnaireStatus.UNSET
        }
    },
    "questionnaire_on_healthcare_access_authored": {
        "field": ParticipantSummary.questionnaireOnHealthcareAccessAuthored,
        "value": "date_string"
    },

    # Social Determinants of Health
    "questionnaire_on_social_determinants_of_health": {
        "field": ParticipantSummary.questionnaireOnSocialDeterminantsOfHealth,
        "value": {
            "submitted_complete": QuestionnaireStatus.SUBMITTED,
            "submitted_incomplete": QuestionnaireStatus.UNSET
        }
    },
    "questionnaire_on_social_determinants_of_health_authored": {
        "field": ParticipantSummary.questionnaireOnSocialDeterminantsOfHealthAuthored,
        "value": "date_string"
    },

    # Personal and Family Health History
    "questionnaire_on_personal_and_family_health_history": {
        "field": ParticipantSummary.questionnaireOnPersonalAndFamilyHealthHistory,
        "value": {
            "submitted_complete": QuestionnaireStatus.SUBMITTED,
            "submitted_incomplete": QuestionnaireStatus.UNSET
        }
    },
    "questionnaire_on_personal_and_family_health_history_authored": {
        "field": ParticipantSummary.questionnaireOnPersonalAndFamilyHealthHistoryAuthored,
        "value": "date_string"
    },

    # Life Functioning Survey
    "questionnaire_on_life_functioning": {
        "field": ParticipantSummary.questionnaireOnLifeFunctioning,
        "value": {
            "submitted_complete": QuestionnaireStatus.SUBMITTED,
            "submitted_incomplete": QuestionnaireStatus.UNSET
        }
    },
    "questionnaire_on_life_functioning_authored": {
        "field": ParticipantSummary.questionnaireOnLifeFunctioningAuthored,
        "value": "date_string"
    },

    # Emotional Health History and Well Being
    "questionnaire_on_emotional_health_history_and_well_being": {
        "field": ParticipantSummary.questionnaireOnEmotionalHealthHistoryAndWellBeing,
        "value": {
            "submitted_complete": QuestionnaireStatus.SUBMITTED,
            "submitted_incomplete": QuestionnaireStatus.UNSET
        }
    },
    "questionnaire_on_emotional_health_history_and_well_being_authored": {
        "field": ParticipantSummary.questionnaireOnEmotionalHealthHistoryAndWellBeingAuthored,
        "value": "date_string"
    },

    # Behavioral Health and Personality
    "questionnaire_on_behavioral_health_and_personality": {
        "field": ParticipantSummary.questionnaireOnBehavioralHealthAndPersonality,
        "value": {
            "submitted_complete": QuestionnaireStatus.SUBMITTED,
            "submitted_incomplete": QuestionnaireStatus.UNSET
        }
    },
    "questionnaire_on_behavioral_health_and_personality_authored": {
        "field": ParticipantSummary.questionnaireOnBehavioralHealthAndPersonalityAuthored,
        "value": "date_string"
    },
    # Pediatric Environmental Health
    # TODO Skipping pediatric for now
    # "questionnaire_on_environmental_exposures": {
    #     "field": ParticipantSummary.questionnaireOnEnvironmentalExposures,
    #     "value": {
    #         "submitted_complete": QuestionnaireStatus.SUBMITTED,
    #         "submitted_incomplete": QuestionnaireStatus.UNSET
    #     }
    # },
    # "questionnaire_on_environmental_exposures_authored": {
    #     "field": ParticipantSummary.questionnaireOnEnvironmentalExposuresAuthored,
    #     "value": "date_string"
    # }
}
