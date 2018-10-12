## OPS-DATA API

The ops-data API offers access to the ParticipantSummary resource and represents an aggregated view of relevant
participant details, including data from consent (name, contact information),
from PPI modules (a status indicating whether the participant has completed
each questionnaire), basic demographics (age, gender, race).

Uses:
* Full data extracts for participants associated with their awardee
* Incremental updates on participants affiliated with their site
* Updates for withdrawn participants

Target users:
* System developers
* Informatics users at Awardee Partners

Endpoints:
* stable: all-of-us-rdr-stable.appspot.com/rdr/v1/ParticipantSummary
* prod: all-of-us-rdr-prod.appspot.com/rdr/v1/ParticipantSummary

See https://github.com/all-of-us/raw-data-repository/blob/master/rdr_client/work_queue.py for Python examples.

The recommended time frame for calling the sync link is every 5 minutes.

**Service Accounts must use the `awardee` parameter in request.**

The summary includes the following fields:

* `participantId`
* `biobankId`
* `firstName`
* `middleName`
* `lastName`
* `zipCode`
* `state`
* `city`
* `streetAddress`
* `phoneNumber`
* `email`
* `recontactMethod`
* `language`
* `dateOfBirth`
* `ageRange`
* `genderIdentity`
* `sex`
* `sexualOrientation`
* `education`
* `income`
* `enrollmentStatus`
* `race`
* `physicalMeasurementsStatus`: indicates whether this participant has completed physical measurements
* `physicalMeasurementsFinalizedTime`: indicates the first time physical measurements were finalized for the participant
* `physicalMeasurementsTime`: indicates the first time physical measurements were submitted for the participant
* `physicalMeasurementsCreatedSite`: indicates the site where physical measurements were created for the participant
* `physicalMeasurementsFinalizedSite`: indicates the site where physical measurements were finalized for the participant
* `signUpTime`: the time at which the participant initially signed up for All Of Us
* `hpoId`: HPO marked as `primary` for this participant, if any (just the resource id, like `PITT` — not a reference like `Organization/PITT`)
* `awardee`: An awardee a participant is paired with or "unset" if none.
* `organization`: An organization a participant is paired with or "unset" if none.
* `site`: A physical location a participant is paired with or "unset" if none.
* `consentForStudyEnrollment`:  indicates whether enrollment consent has been received (`UNSET` or `SUBMITTED`)
* `consentForStudyEnrollmentTime`: indicates the time at which enrollment consent has been received (ISO-8601 time)
* `consentForElectronicHealthRecords`
* `consentForElectronicHealthRecordsTime`
* `questionnaireOnOverallHealth`: indicates status for Overall Health PPI module
* `questionnaireOnOverallHealthTime`
* `questionnaireOnPersonalHabits`
* `questionnaireOnPersonalHabitsTime`
* `questionnaireOnSociodemographics`
* `questionnaireOnSociodemographicsTime`
* `questionnaireOnHealthcareAccess`
* `questionnaireOnHealthcareAccessTime`
* `questionnaireOnMedicalHistory`
* `questionnaireOnMedicalHistoryTime`
* `questionnaireOnMedications`
* `questionnaireOnMedicationsTime`
* `questionnaireOnFamilyHealth`
* `questionnaireOnFamilyHealthTime`
* `biospecimenStatus`: whether biospecimens have been finalized for the participant
* `biospecimenOrderTime`: the first time at which biospecimens were finalized
* `biospecimenSourceSite`: the site where biospecimens were initially created for the participant
* `biospecimenCollectedSite`: the site where biospecimens were initially collected for the participant
* `biospecimenProcessedSite`: the site where biospecimens were initially processed for the participant
* `biospecimenFinalizedSite`: the site where biospecimens were initially finalized for the participant
* `sampleOrderStatus1SST8`
* `sampleOrderStatus1SST8Time`
* `sampleOrderStatus1PST8`
* `sampleOrderStatus1PST8Time`
* `sampleOrderStatus1HEP4`
* `sampleOrderStatus1HEP4Time`
* `sampleOrderStatus1ED04`
* `sampleOrderStatus1ED04Time`
* `sampleOrderStatus1ED10`
* `sampleOrderStatus1ED10Time`
* `sampleOrderStatus2ED10`
* `sampleOrderStatus2ED10Time`
* `sampleOrderStatus1UR10`
* `sampleOrderStatus1UR10Time`
* `sampleOrderStatus1UR90`
* `sampleOrderStatus1UR90Time`
* `sampleOrderStatus1ED02`
* `sampleOrderStatus1ED02Time`
* `sampleOrderStatus1CFD9`
* `sampleOrderStatus1CFD9Time`
* `sampleOrderStatus1PXR2`
* `sampleOrderStatus1PXR2Time`
* `sampleOrderStatus1SAL`
* `sampleOrderStatus1SALTime`
* `sampleStatus1SST8`
* `sampleStatus1SST8Time`
* `sampleStatus1PST8`
* `sampleStatus1PST8Time`
* `sampleStatus1HEP4`
* `sampleStatus1HEP4Time`
* `sampleStatus1ED04`
* `sampleStatus1ED04Time`
* `sampleStatus1ED10`
* `sampleStatus1ED10Time`
* `sampleStatus2ED10`
* `sampleStatus2ED10Time`
* `sampleStatus1UR10`
* `sampleStatus1UR10Time`
* `sampleStatus1UR90`
* `sampleStatus1UR90Time`
* `sampleStatus1ED02`
* `sampleStatus1ED02Time`
* `sampleStatus1CFD9`
* `sampleStatus1CFD9Time`
* `sampleStatus1PXR2`
* `sampleStatus1PXR2Time`
* `sampleStatus1SAL`
* `sampleStatus1SALTime`
* `numCompletedBaselinePPIModules`
* `numCompletedPPIModules`
* `numBaselineSamplesArrived`
* `samplesToIsolateDNA`
* `withdrawalStatus`
* `suspensionStatus`

For enumeration fields, the following values are defined:

`hpoId`: `UNSET`, `UNMAPPED`, `PITT`, `COLUMBIA`, `ILLINOIS`, `AZ_TUCSON`, `COMM_HEALTH`, `SAN_YSIDRO`, `CHEROKEE`, `EAU_CLAIRE`, `HRHCARE`, `JACKSON`, `GEISINGER`, `CAL_PMC`, `NE_PMC`, `TRANS_AM`, `VA`

`ageRange`: `0-17`, `18-25`, `26-35`, `36-45`, `46-55`, `56-65`, `66-75`, `76-85`, `86-`

`physicalMeasurementsStatus`: `UNSET`, `SCHEDULED`, `COMPLETED`, `RESULT_READY`

`questionnaireOn[x]`: `UNSET`, `SUBMITTED`

`biospecimenStatus`: `UNSET`, `FINALIZED`

`sampleOrderStatus[x]`: `UNSET`, `CREATED`, `COLLECTED`, `PROCESSED`, `FINALIZED`

`sampleStatus[x]` and `samplesToIsolateDNA`: `UNSET`, `RECEIVED`

`withdrawalStatus`: `NOT_WITHDRAWN`, `NO_USE`

`suspensionStatus`: `NOT_SUSPENDED`, `NO_CONTACT`

`enrollmentStatus`: `INTERESTED`, `MEMBER`, `FULL_PARTICIPANT`

`race`: `UNSET`, `UNMAPPED`, `AMERICAN_INDIAN_OR_ALASKA_NATIVE`, `BLACK_OR_AFRICAN_AMERICAN`,
        `ASIAN`, `NATIVE_HAWAIIAN_OR_OTHER_PACIFIC_ISLANDER`, `WHITE`, `HISPANIC_LATINO_OR_SPANISH`,
        `MIDDLE_EASTERN_OR_NORTH_AFRICAN`, `HLS_AND_WHITE`, `HLS_AND_BLACK`,
        `HLS_AND_ONE_OTHER_RACE`, `HLS_AND_MORE_THAN_ONE_OTHER_RACE`, `MORE_THAN_ONE_RACE`,
  		`OTHER_RACE`, `PREFER_NOT_TO_SAY`

`awardee`: See https://github.com/all-of-us/raw-data-repository/blob/master/rest-api/data/awardees.csv

Note: 
- hpoId maps to awardee. If awardee is set in a request, hpoId is updated and vice versa.
- The GET request needs to identify the awardee via the parameter. 
- SA is only able to call the parameter specific to its own awardee.

See `GET` examples below using `awardee=` for requests.

The values for the following fields are defined in the [codebook](
https://docs.google.com/spreadsheets/d/1TNqJ1ekLFHF4vYA2SNCb-4NL8QgoJrfuJsxnUuXd-is/edit):

* `state`
* `recontactMethod`
* `language`
* `genderIdentity`
* `sex`
* `sexualOrientation`
* `education`
* `income`
* `race`

If one of these fields has a value that is not mapped in the codebook, the API
returns `"UNMAPPED"`.  If the participant has not yet provided a value the API
returns `"UNSET"` (this is the default state).  If the participant elected to skip
the question the API will return `"SKIPPED"`.

#### `GET /ParticipantSummary?`

List participants matching a set of search parameters. This supports in-clinic
lookup (for physical measurements and biospecimen donation) as well as a
Participant Work Queue. Any of the above parameters can be provided as a query parameter to do
an exact match.


The  ops-data API supports filtering and sorting on last modified time.
The default order results are returned is...
* last modified time
* participant ID (ascending)

For service accounts access, the awardee parameter is required.
Example:

    GET /ParticipantSummary?awardee=PITT&_sort=lastModified

Example sync:

    GET /ParticipantSummary?awardee=PITT&_sort=lastModified&_sync=true

Pagination is provided with a token i.e.

    GET /ParticipantSummary?awardee=PITT&_sort=lastModified&_token=<token string>

It is possible to get the same participant data back in multiple sync responses.
The recommended time between syncs is 5 minutes.

See FHIR search prefixes below

Synchronize ops-data last modified link.
This allows Awardees to stay up-to-date
with newly-arrived summaries. The return value is a FHIR History [Bundle](http://hl7.org/fhir/bundle.html)
where each entry is a `ParticipantSummary` document.

The Bundle's `link` array will include a link with relation=`next` if more results are available immediately.
Otherwise the array will contain a `link` with relation=`sync` that can be used to check for new results.

Example response:
```json
 "link": [
        {
            "relation": "sync",
            "url": "GET /ParticipantSummary?awardee=PITT&_sort=lastModified&_token=WzM1XQ%3D%3D"
        }
```

##### Service Accounts
* Each awardee partner is issued one service account.
* Authorized users can generate API keys for access.
* Awardees are responsible for rotating keys on a three day timeframe.
    ** Permissions will be revoked after this time.
* Service account for specific awardees  must specify the awardee parameter in requests.

    `GET /ParticipantSummary?awardee=PITT`

    `GET /ParticipantSummary?awardee=PITT&state=PIIState_MA`

    `GET /ParticipantSummary?awardee=PITT&organization=PITT_UPMC`

    `GET /ParticipantSummary?awardee=PITT&site=hpo-site-UPMC`


For integer and date fields, the following prefixes can be provided for query parameter values to
indicate inequality searches, as per the [FHIR search spec](https://www.hl7.org/fhir/search.html):

  * `lt`: less than
  * `le`: less than or equal to
  * `gt`: greater than
  * `ge`: greater than or equal to
  * `ne`: not equal to


Other supported parameters from the FHIR spec:

* `_count`: the maximum number of participant summaries to return; the default is 100, the maximum
  supported value is 10,000

* `_sort`: the name of a field to sort results by, in ascending order, followed by last name, first
  name, date of birth, and participant ID.

* `_sort:desc`: the name of a field to sort results by, in descending order, followed by last name,
  first name, date of birth, and participant ID.

We furthermore support an `_includeTotal` query parameter that will execute a
count of the given set of summaries and attach that to the returned FHIR Bundle
as a `total` key.

If no sort order is requested, the default sort order is last name, first name, date of birth, and
participant ID.

The response is an FHIR Bundle containing participant summaries. If more than the requested number
of participant summaries match the specified criteria, a "next" link will be returned that can
be used in a follow on request to fetch more participant summaries.
