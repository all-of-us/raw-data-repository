from sqlalchemy import Boolean, Column, Date, Integer, String

from rdr_service import clock
from rdr_service.model.base import Base
from rdr_service.model.utils import UTCDateTime


class MetricsEnrollmentStatusCache(Base):
    """Contains enrollment status metrics data grouped by HPO ID and date.
  """

    __tablename__ = "metrics_enrollment_status_cache"
    __rdr_internal_table__ = True
    dateInserted = Column("date_inserted", UTCDateTime, default=clock.CLOCK.now, nullable=False, primary_key=True)
    hpoId = Column("hpo_id", String(20), primary_key=True)
    hpoName = Column("hpo_name", String(255), primary_key=True)
    date = Column("date", Date, nullable=False, primary_key=True)
    registeredCount = Column("registered_count", Integer, nullable=False)
    participantCount = Column("participant_count", Integer, nullable=False)
    consentedCount = Column("consented_count", Integer, nullable=False)
    coreCount = Column("core_count", Integer, nullable=False)
    participantOrigin = Column("participant_origin", String(50), primary_key=True)


class MetricsRaceCache(Base):
    """Contains race metrics data grouped by HPO ID and date.
  """

    __tablename__ = "metrics_race_cache"
    __rdr_internal_table__ = True
    dateInserted = Column("date_inserted", UTCDateTime, default=clock.CLOCK.now, nullable=False, primary_key=True)
    type = Column("type", String(50), primary_key=True)
    registeredFlag = Column("registered_flag", Boolean, nullable=False, primary_key=True)
    participantFlag = Column('participant_flag', Boolean, nullable=False, primary_key=True)
    consentedFlag = Column("consent_flag", Boolean, nullable=False, primary_key=True)
    coreFlag = Column("core_flag", Boolean, nullable=False, primary_key=True)
    hpoId = Column("hpo_id", String(20), primary_key=True)
    hpoName = Column("hpo_name", String(255), primary_key=True)
    date = Column("date", Date, nullable=False, primary_key=True)
    americanIndianAlaskaNative = Column("american_indian_alaska_native", Integer, nullable=False)
    asian = Column("asian", Integer, nullable=False)
    blackAfricanAmerican = Column("black_african_american", Integer, nullable=False)
    middleEasternNorthAfrican = Column("middle_eastern_north_african", Integer, nullable=False)
    nativeHawaiianOtherPacificIslander = Column("native_hawaiian_other_pacific_islander", Integer, nullable=False)
    white = Column("white", Integer, nullable=False)
    hispanicLatinoSpanish = Column("hispanic_latino_spanish", Integer, nullable=False)
    noneOfTheseFullyDescribeMe = Column("none_of_these_fully_describe_me", Integer, nullable=False)
    preferNotToAnswer = Column("prefer_not_to_answer", Integer, nullable=False)
    multiAncestry = Column("multi_ancestry", Integer, nullable=False)
    noAncestryChecked = Column("no_ancestry_checked", Integer, nullable=False)
    participantOrigin = Column("participant_origin", String(50), primary_key=True)
    unsetNoBasics = Column('unset_no_basics', Integer, nullable=True)
    """Flag for individuals that have no data in TheBasics"""


class MetricsGenderCache(Base):
    """Contains gender metrics data grouped by HPO ID and date.
  """

    __tablename__ = "metrics_gender_cache"
    __rdr_internal_table__ = True
    dateInserted = Column("date_inserted", UTCDateTime, default=clock.CLOCK.now, nullable=False, primary_key=True)
    type = Column("type", String(50), primary_key=True)
    enrollment_status = Column("enrollment_status", String(50), primary_key=True, default="")
    hpoId = Column("hpo_id", String(20), primary_key=True)
    hpoName = Column("hpo_name", String(255), primary_key=True)
    date = Column("date", Date, nullable=False, primary_key=True)
    genderName = Column("gender_name", String(255), primary_key=True)
    genderCount = Column("gender_count", Integer, nullable=False)
    participantOrigin = Column("participant_origin", String(50), primary_key=True)


class MetricsAgeCache(Base):
    """Contains age range metrics data grouped by HPO ID and date.
  """

    __tablename__ = "metrics_age_cache"
    __rdr_internal_table__ = True
    dateInserted = Column("date_inserted", UTCDateTime, default=clock.CLOCK.now, nullable=False, primary_key=True)
    enrollment_status = Column("enrollment_status", String(50), primary_key=True, default="")
    type = Column("type", String(50), primary_key=True)
    hpoId = Column("hpo_id", String(20), primary_key=True)
    hpoName = Column("hpo_name", String(255), primary_key=True)
    date = Column("date", Date, nullable=False, primary_key=True)
    ageRange = Column("age_range", String(255), primary_key=True)
    ageCount = Column("age_count", Integer, nullable=False)
    participantOrigin = Column("participant_origin", String(50), primary_key=True)


class MetricsRegionCache(Base):
    """Contains region metrics data grouped by HPO and date.
  """

    __tablename__ = "metrics_region_cache"
    __rdr_internal_table__ = True
    dateInserted = Column("date_inserted", UTCDateTime, default=clock.CLOCK.now, nullable=False, primary_key=True)
    enrollmentStatus = Column("enrollment_status", String(50), primary_key=True, default="")
    hpoId = Column("hpo_id", String(20), primary_key=True)
    hpoName = Column("hpo_name", String(255), primary_key=True)
    date = Column("date", Date, nullable=False, primary_key=True)
    stateName = Column("state_name", String(255), primary_key=True)
    stateCount = Column("state_count", Integer, nullable=False)
    participantOrigin = Column("participant_origin", String(50), primary_key=True)


class MetricsLanguageCache(Base):
    """Contains language metrics data grouped by HPO and date.
  """

    __tablename__ = "metrics_language_cache"
    __rdr_internal_table__ = True
    dateInserted = Column("date_inserted", UTCDateTime, default=clock.CLOCK.now, nullable=False, primary_key=True)
    enrollmentStatus = Column("enrollment_status", String(50), primary_key=True, default="")
    hpoId = Column("hpo_id", String(20), primary_key=True)
    hpoName = Column("hpo_name", String(255), primary_key=True)
    date = Column("date", Date, nullable=False, primary_key=True)
    languageName = Column("language_name", String(50), primary_key=True)
    languageCount = Column("language_count", Integer, nullable=False)


class MetricsLifecycleCache(Base):
    """Contains lifecycle metrics data grouped by HPO and date.
    """

    __tablename__ = "metrics_lifecycle_cache"
    __rdr_internal_table__ = True
    dateInserted = Column("date_inserted", UTCDateTime, default=clock.CLOCK.now, nullable=False, primary_key=True)
    enrollmentStatus = Column('enrollment_status', String(50), primary_key=True, default='')
    type = Column("type", String(50), primary_key=True)
    hpoId = Column("hpo_id", String(20), primary_key=True)
    hpoName = Column("hpo_name", String(255), primary_key=True)
    date = Column("date", Date, nullable=False, primary_key=True)
    registered = Column("registered", Integer, nullable=False)
    consentEnrollment = Column("consent_enrollment", Integer, nullable=False)
    consentComplete = Column("consent_complete", Integer, nullable=False)
    ppiBasics = Column("ppi_basics", Integer, nullable=False)
    ppiOverallHealth = Column("ppi_overall_health", Integer, nullable=False)
    ppiLifestyle = Column("ppi_lifestyle", Integer, nullable=False)
    ppiHealthcareAccess = Column("ppi_healthcare_access", Integer, nullable=False)
    ppiMedicalHistory = Column("ppi_medical_history", Integer, nullable=False)
    ppiMedications = Column("ppi_medications", Integer, nullable=False)
    ppiFamilyHealth = Column("ppi_family_health", Integer, nullable=False)
    ppiBaselineComplete = Column("ppi_baseline_complete", Integer, nullable=False)
    retentionModulesEligible = Column("retention_modules_eligible", Integer, nullable=False)
    retentionModulesComplete = Column("retention_modules_complete", Integer, nullable=False)
    physicalMeasurement = Column("physical_measurement", Integer, nullable=False)
    sampleReceived = Column("sample_received", Integer, nullable=False)
    fullParticipant = Column("full_participant", Integer, nullable=False)
    participantOrigin = Column("participant_origin", String(50), primary_key=True)


class MetricsCacheJobStatus(Base):
    __tablename__ = "metrics_cache_job_status"
    __rdr_internal_table__ = True
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    cacheTableName = Column("cache_table_name", String(100), nullable=False)
    type = Column("type", String(50))
    inProgress = Column("in_progress", Boolean, default=False, nullable=False)
    complete = Column("complete", Boolean, default=False, nullable=False)
    dateInserted = Column("date_inserted", UTCDateTime, nullable=False)
