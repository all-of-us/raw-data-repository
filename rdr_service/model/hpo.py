from sqlalchemy import Column, Integer, String, UniqueConstraint
from sqlalchemy.orm import relationship

from rdr_service.model.base import Base
from rdr_service.model.site_enums import ObsoleteStatus
from rdr_service.model.utils import Enum, UTCDateTime
from rdr_service.participant_enums import OrganizationType


class HPO(Base):
    """An awardee, containing organizations (which in turn contain sites.)"""

    __tablename__ = "hpo"
    hpoId = Column("hpo_id", Integer, primary_key=True, autoincrement=False)
    name = Column("name", String(20))
    """
    An identifier for the HPO (just the resource id, like PITT — not a reference like Organization/PITT)
    @rdr_dictionary_show_unique_values
    """
    displayName = Column("display_name", String(255))
    """
    The hpo display name
    @rdr_dictionary_show_unique_values
    """
    organizationType = Column("organization_type", Enum(OrganizationType), default=OrganizationType.UNSET)
    """The type of organization responsible for signing up participants"""
    organizations = relationship("Organization", cascade="all, delete-orphan", order_by="Organization.externalId")
    isObsolete = Column("is_obsolete", Enum(ObsoleteStatus))
    """Whether or not the hpo has been inactivated (1 if obsolete)"""
    resourceId = Column('resource_id', String(255))

    __table_args__ = (UniqueConstraint("name"),)


class HpoCountsReport(Base):
    """Daily record of the table_counts_with_upload_timestamp_for_hpo_sites BigQuery view

  NOTE: line comments identify each column from the BigQuery view as of 2019-03-08
  """

    __tablename__ = "hpo_counts_report"

    # report_run_time DATETIME	NULLABLE
    reportRunTime = Column("report_run_time", UTCDateTime, primary_key=True)
    # HPO_id	STRING	NULLABLE
    hpoId = Column("hpo_id", Integer, primary_key=True)
    hpoIdString = Column("hpo_id_string", String(20))
    # Display_Order	INTEGER	NULLABLE
    displayOrder = Column("display_order", Integer)
    # person	INTEGER	NULLABLE
    person = Column("person", Integer)
    # person_upload_time	TIMESTAMP	NULLABLE
    personUploadTime = Column("person_upload_time", UTCDateTime)
    # condition_occurrence	INTEGER	NULLABLE
    conditionOccurrence = Column("condition_occurrence", Integer)
    # condition_occurrence_upload_time	TIMESTAMP	NULLABLE
    conditionOccurrenceUploadTime = Column("condition_occurrence_upload_time", UTCDateTime)
    # procedure_occurrence	INTEGER	NULLABLE
    procedureOccurrence = Column("procedure_occurrence", Integer)
    # procedure_occurrence_upload_time	TIMESTAMP	NULLABLE
    procedureOccurrenceUploadTime = Column("procedure_occurrence_upload_time", UTCDateTime)
    # drug_exposure	INTEGER	NULLABLE
    drugExposure = Column("drug_exposure", Integer)
    # drug_exposure_upload_time	TIMESTAMP	NULLABLE
    drugExposureUploadTime = Column("drug_exposure_upload_time", UTCDateTime)
    # visit_occurrencence	INTEGER	NULLABLE
    visitOccurrence = Column("visit_occurrence", Integer)
    # visit_occurrencence_upload_time	TIMESTAMP	NULLABLE
    visitOccurrenceUploadTime = Column("visit_occurrence_upload_time", UTCDateTime)
    # measurement	INTEGER	NULLABLE
    measurement = Column("measurement", Integer)
    # measurement_upload_time	TIMESTAMP	NULLABLE
    measurementUploadTime = Column("measurement_upload_time", UTCDateTime)
    # observation	INTEGER	NULLABLE
    observation = Column("observation", Integer)
    # observation_upload_time	TIMESTAMP	NULLABLE
    observationUploadTime = Column("observation_upload_time", UTCDateTime)
    # device_exposure	INTEGER	NULLABLE
    deviceExposure = Column("device_exposure", Integer)
    # device_exposure_upload_time	TIMESTAMP	NULLABLE
    deviceExposureUploadTime = Column("device_exposure_upload_time", UTCDateTime)
    # death	INTEGER	NULLABLE
    death = Column("death", Integer)
    # death_upload_time	TIMESTAMP	NULLABLE
    deathUploadTime = Column("death_upload_time", UTCDateTime)
    # provider	INTEGER	NULLABLE
    provider = Column("provider", Integer)
    # provider_upload_time	TIMESTAMP	NULLABLE
    providerUploadTime = Column("provider_upload_time", UTCDateTime)
    # specimen	INTEGER	NULLABLE
    specimen = Column("specimen", Integer)
    # specimen_upload_time	TIMESTAMP	NULLABLE
    specimenUploadTime = Column("specimen_upload_time", UTCDateTime)
    # location	INTEGER	NULLABLE
    location = Column("location", Integer)
    # location_upload_time	TIMESTAMP	NULLABLE
    locationUploadTime = Column("location_upload_time", UTCDateTime)
    # care_site	INTEGER	NULLABLE
    careSite = Column("care_site", Integer)
    # care_site_upload_time	TIMESTAMP	NULLABLE
    careSiteUploadTime = Column("care_site_upload_time", UTCDateTime)
    # note	INTEGER	NULLABLE
    note = Column("note", Integer)
    # note_upload_time	TIMESTAMP	NULLABLE
    noteUploadTime = Column("note_upload_time", UTCDateTime)
