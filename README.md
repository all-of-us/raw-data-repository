# Precision Medicine Initiative: Raw Data Repository

## Purpose of this document

Describe the PMI's Raw Data Repository API, intended to support 2017
launch-time requirements. This document is version controlled; you
should read the version that lives in the branch or tag you need.

## Contributions

The Raw Data Repository is being developed to house data for the All of Us Research
Program. We are developing this project in the open, and publishing the code
with an open-source license, to share knowledge and provide insight and sample
code for the community. We welcome your feedback! Please send any security
concerns to security@pmi-ops.org, and feel free to file other issues via GitHub.
Please note that we do not plan to incorporate external code contributions at
this time, given that the RDR exists to meet the specific
operational needs of the All of Us Research Program.

## Documentation and Code Directory Overview

CircleCI Build Status ![Build Status of master](https://circleci.com/gh/all-of-us/raw-data-repository.png?circle-token=be5ab3e1a27746993aa0eca88d90f421b72a2b6e)

*   `rest-api` Source for the API server. [README](rest-api/README.md) describes
    development processes, auth model and other design, and summarizes API
    endpoints.
    *   `offline` Batch processes run by the API server: metrics, and biobank
        samples reconciliation. [README](rest-api/offline/README.md) describes
        pipeline function and inputs/outputs.
    *   `tools` Scripts for setup, maintenance, deployment, etc.
        [README](rest-api/tools/README.md) summarizes each tool's purpose.
    *   `test` Unit and client/integration tests (CircleCI runs these).
        [README](rest-api/test/README.md) had instructions for running tests.
*   `rdr_client` API client for communicating with the API. Used in
    integration tests, and includes basic examples for a few APIs.
    [README](rdr_client/README.md) describes library setup.
*   `rdr_common` Python modules shared between API and client
*   `ci` CircleCI (continuous integration testing and deployment) scripts.
*   `git-hooks` Suggested script to run for automated checks during development.

## Modules Shared via PIP

The `rdr_client` and `rdr_common` Python modules may be installed with:

```Shell
pip install -e 'git+git@github.com:all-of-us/raw-data-repository.git#egg=all-of-us-rdr'
```

and then used like

```Python
from rdr_client.client import Client
from rdr_common.main_util import configure_logging
```

## API Overview

These are the APIs that the RDR will support for the initial launch:

* PTC to RDR (Raw Data Repository)
  * Create and update Participant information
  * Create and update Questionnaires and Responses ("PPI", Participant-provided information)
  * Read data about a participant
* Health Professional Portal to RDR
  * Search Participants
    * At check-in time, look up an individual by name, date of birth, zip code
    * For a Work Queue, filter participants based on summary information (e.g. age, race)
  * Get Participant Summary (by id)
  * Update a Participant with a new Medical Record Number
  * Insert results from physical measurements
  * Insert biospecimen orders
* BioBank to RDR
  * (Google Cloud Storage “add a csv file to bucket”, performed daily)
* For release management and operational monitoring
  * Serving version identifier - no auth required

## API Details

### Technologies Used

* JSON + REST
* OAuth 2.0 / Google authentication
* HTTPS
* FHIR API and resource definitions where appropriate (questionnaires, physical measurements)
* FHIR API and resource conventions elsewhere (search, biospecimen orders)
* API handlers written in Python + Flask, backed by AppEngine.
* Configs stored in [Cloud Datastore](https://cloud.google.com/datastore/docs/concepts/overview) (indexed on fields required for search)
* Data stored in [Google Cloud SQL](https://cloud.google.com/sql/docs/)
* [Alembic](http://alembic.zzzcomputing.com/en/latest/) for SQL change management

### Authentication Details

All actors calling these APIs in production will use [service accounts](https://cloud.google.com/compute/docs/access/service-accounts).

For development and testing only, they can be called with the developer’s credentials using OAuth.

We will use a Google Cloud Project owned by Vanderbilt for testing:  "PMI DRC API Test" (id: `pmi-drc-api-test`).

### Test System

The API will be set up against a Cloud Datastore instance.  Schema is evolving to support launch.

### Version Management

When updating a resource (via PUT), the
RDR API requires a client to provide an HTTP “If-Match” header that points to
the current version of a resource. This prevents a client from blindly
overwriting data that it hasn’t had the opportunity to see yet. The client uses
the value from a resource’s “ETag” header, following RFC7232 (or, for a more
readable tutorial, see [this
article](http://fideloper.com/etags-and-optimistic-concurrency-control)).
Concretely, a request looks like:

```
PUT /rdr/v1/Participant/P123456789
If-Match: W/"98172982174921"

{
  "providerLink": [{
    "primary": true,
    "site": "Organization/PITT",
  }]
}
```

And this request either fails (if the supplied ETag does not match the current
resource version), or it succeeds in updating the resource and returns a new

ETag in a header like like:

    ETag: W/"99817291822"

ETag values are also returned in a *`meta.versionId`* property within each
resource, following FHIR’s convention.


## Metadata API

Clients of the RDR can use this API to verify that the version they did
integration testing against matches the production version.  This API does not
check authorization because the information is not sensitive, making it
suitable for operational monitoring (uptime checks) as well.

#### `GET /`

Retrieve metadata for the server. Response is a JSON object including a
`version_id` field.  The version ID will change with each binary release.
Format and semantics of the identifier are not otherwise defined.


## Participant API

The Participant is a very thin resource—essentially it has a set of identifiers including:

* `participantId`: PMI-specific ID generated by the RDR and used for
  tracking/linking participant data. Human-readable 10-character string
beginning with `P`.
* `biobankId`: PMI-specific ID generated by the RDR and used exclusively for
  communicating with the biobank. Human-readable 10-character string beginning
with `B`.
*  `providerLink`: list of "provider link" objects indicating that this
   participant is known to a provider, including:
  * `primary`: `true` | `false`, indicating whether this provider is the "main"
    provider responsible for recruiting a participant and performing physical
    measurements and biospecimen collection
  * `awardee`: Reference to an awardee  pairing level, like `awardee: AZ_TUCSON`
  * `organization`: Reference to an organizational pairing level below awardee, like `organization: WISCONSIN_MADISON`
  * `site`: Reference to a physical location pairing level below organization. Site name are a subset of google group, like `site: hpo-site-uabkirklin`
  * `identifier`: array of
    [identifiers](http://hl7.org/fhir/datatypes#identifier)  with `system` and
    `value` indicating medical record numbers by which this participant is known
  * `withdrawalStatus`: `NOT_WITHDRAWN` | `NO_USE`; indicates whether the participant
    has withdrawn from the study, and does not want their data used in future
  * `suspensionStatus`: `NOT_SUSPENDED` | `NO_CONTACT`; indicates whether the participant
    has indicated they do not want to be contacted anymore

The Participant API supports the following API calls:

#### `POST /Participant`

Insert a new participant. Request body is an empty JSON document. Response is a
Participant object with newly-created ids.

#### `PUT /Participant/:id`

Update an existing participant with new values. Request body is
a Participant object reflecting desired changes (which should include values for all fields,
including values from the existing resource if nothing has changed). Response is the Participant
object as stored.

#### `GET  /Participant/:id`

Read a single participant.

## ParticipantSummary API

The ParticipantSummary resource represents an aggregated view of relevant
participant details, including data from consent (name, contact information),
from PPI modules (a status indicating whether the participant has completed
each questionnaire), basic demographics (age, gender, race).

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

Note: hpoId maps to awardee. If awardee is set in a request, hpoId is updated and vice versa.

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


The participant summary API supports filtering and sorting on last modified time.
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

Synchronize Participant Summary last modified link.
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

If no HPO is provided, then a last name and date of birth (at minimum) should be
supplied. Example query:

    GET /ParticipantSummary?dateOfBirth=1980-12-30&lastName=Smith


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

## Questionnaire and QuestionnaireResponse API

We use the FHIR [Questionnaire](http://hl7.org/fhir/questionnaire.html) and
[QuestionnaireResponse](http://hl7.org/fhir/questionnaireresponse.html)
resources to track consent and participant-provided information. We store the
blank forms (Questionnaires) at the RDR level, and we store responses at the
participant level.


#### `POST /Questionnaire`

Create a new Questionnaire in the RDR. Body is a FHIR DSTU2 Questionnaire
resource. Response is the stored resource, which includes an `id`.

#### `PUT /Questionnaire/:id`

Replace the questionnaire with the specified ID. Body is a FHIR DSTU2 Questionnaire
resource. Response is the stored resource, which includes an `id`.

#### `GET /Questionnaire/:id`

Read a single Questionnaire from the RDR. Response is the stored resource.

#### `GET /Questionnaire?concept=:concept_code`

Returns the last submitted questionnaire that has the specified top-level concept code. Response
is the stored resource.

#### `POST /Participant/:pid/QuestionnaireResponse`

Create a new QuestionnaireResponse in the RDR. Body is a FHIR DSTU2
QuestionnaireResponse resource which must include:

* `subject`: a reference to the participant, in the form `Patient/:pid`.  The
  `:pid` variable of this refernce must match the participant ID supplied in
  the POST URL. (Note the use of the word "Patient" here, which comes from FHIR.)

* `questionnaire`: a reference to the questionnaire for which this response
  has been written, in the form `Questionnaire/:qid` or `Questionnaire/:qid/_history/:version`
  (the latter indicating the version of the questionnaire in use)

* `linkId`s for each question, corresponding to a `linkId` specified in the
  questionnaire

#### `GET /Participant/:pid/QuestionnaireResponse/:qid`

Example query:

    GET /Participant/P123456789/Questionnaire/810572271


## PhysicalMeasurements API

We use the FHIR `Document` model to represent a set of physical measurements
recorded at a PMI visit. This stores a `Bundle` of resources, with a
`Composition` as the first entry (listing basic metadata for the document,
including a document type, creation time, author, and an index of contents),
and a set of `Observation`s as subsequent entries (recording, for example,
individual blood pressure or weight measurements).


#### `POST /Participant/:pid/PhysicalMeasurements`

Create a new PhysicalMeasurements for a given participant. The payload is a
Bundle (see [example](rest-api/test/test-data/measurements-as-fhir.json))
where the first entry is a `Composition` including:

* `subject`: a reference to the participant, in the form `Patient/:pid`.  The
  `:pid` variable of this refernce must match the participant ID supplied in
  the POST URL. (Note the use of the word "Patient" here, which comes from FHIR.)

* `type`: a coding indicating the document type. This should have a `system` of
  `http://terminology.pmi-ops.org/CodeSystem/document-type`, so the compete type looks like:

```
"type": {
  "coding": [{
    "system": "http://terminology.pmi-ops.org/CodeSystem/document-type",
    "code": "intake-exam-v0.0.1",
    "display": "PMI Intake Measurements v0.0.1"
  }]
}

```

* a series of `Observation`s each with times, codes, and values.

See also: [Physical measurements form
specs](https://docs.google.com/spreadsheets/d/10kYqLSPigl02jUBpwEHpGAHwuwExFu-BedvMOJ5afpE/edit#gid=0)
and
[methods](https://drive.google.com/file/d/0B7ko4YYX_fIca0QwRWx5VkdfSW1SSWFldWN2UTZtWFNMTS1B/view)

#### `POST /Participant/:pid/PhysicalMeasurements`

Create a new PhysicalMeasurements for a given participant. Request body is a FHIR
Document-type Bundle. Response is the resource as stored.

If these measurements are an amendment of previously submitted measurements, that can be indicated
in the request body with the following extension:

```
  "extension": [{
    "url": "http://terminology.pmi-ops.org/StructureDefinition/amends",
    "valueReference": {
      "reference": "PhysicalMeasurements/:measurements_id"
    }
  }
```

The resource returned in the response will have a status of 'amended' if another set of physical
measurements have been submitted as an amendment to the measurements in question.

#### `GET /Participant/:pid/PhysicalMeasurements/:id`

Read PhysicalMeasurements by id.

#### `GET /Participant/:pid/PhysicalMeasurements`

Search for all PhysicalMeasurements available for a given participant. Response
body is a Bundle (possibly empty) of documents (that is: a bundle of search
results whose entries are bundles of measurements).

#### `GET /PhysicalMeasurements/_history`

Synchronize PhysicalMeasurements across all participants. This allows PTC to stay up-to-date
with newly-arrived measurements. The return value is a FHIR History [Bundle](http://hl7.org/fhir/bundle.html)
where each entry is a `PhysicalMeasurements` document.

The Bundle's `link` array will include a link with relation=`next` if more results are available immediately.
Otherwise the array will contain a `link` with relation=`sync` that can be used to check for new results in
one minute.

## BiobankOrder API

Maintains records of orders placed from HealthPro to the Biobank. Each order is
a resource [as documented
here](https://docs.google.com/document/d/1nTvFU3V7ssizGwdwzlc0-EfByTGCBoEjJOzQ-YKExqc/edit),
including:

* `subject`: a reference to the participant, in the form `Patient/:pid`.  The
  `:pid` variable of this refernce must match the participant ID supplied in
  the POST URL. (Note the use of the word "Patient" here, which comes from FHIR.)

* `identifier`: an array of Identifiers, each with a `system` and `value`.
  These should include the HealthPro identifier for this order as well as the
biobank identifier for this order.


#### `POST /Participant/:pid/BiobankOrder`

Create a new BiobankOrder for a given participant. Request body is a
BiobankOrder resource to be created. Response is the resource as stored.

#### `GET /Participant/:pid/BiobankOrder/:oid`

Read a BiobankOrder by id.

#### `GET /Participant/:pid/BiobankOrder`

Search for BiobankOrders about a given participant. Response body is a bundle
of BiobankOrders (possibly empty).

## Metrics API

Metrics provide a high-level overview of participants counts by date for a
variety of metrics (e.g. by race, ethnicity, or consent status). These can be
broken down by "facet" (e.g. HPO). These metrics are intended to provide "just
enough" data to drive known launch-time dashboard requirements. (Different
technology will be required to provide a flexible ad-hoc analysis system.)

#### `POST /Metrics`

Retrieve RDR metrics up to the present time. The request body should contain a start date and
end date range to retrieve metrics for, e.g.:

```
{
  "start_date": "2017-01-01"
  "end_date": "2017-02-01"
}
```

The response body includes:

* `field_definition`: an array of fields that appear in the returned metrics. Each field is an object with a name and array of values, like:


```
{
  "name": "Participant.genderIdentity",
  "values": [
    "FEMALE",
    "MALE",
    "FEMALE_TO_MALE_TRANSGENDER",
    "MALE_TO_FEMALE_TRANSGENDER",
    "INTERSEX",
    "OTHER",
    "PREFER_NOT_TO_SAY"
  ]
}
```

## Metric Sets API

Metrics are grouped into metric sets, which share a common schema. One metric
set may correspond to a live view of the RDR data, while others will be fixed
snapshots taken during curated data releases. These primarily exist to provide
high-level aggregations vs the above `Metrics API`, which predates this and
primarily drives an internal operational view.

#### `GET /MetricSets`

List all available metric sets and their IDs.

The response body includes:

* `metricSets`: an array of metric sets

```
{
  "metricSets": [
    {
      "id": "live.public_participants",
      "type": "participants"
    }
    ...
  ]
}
```

#### `GET /MetricSets/:msid/Metrics`

List all metrics within the given metric set, optionally limited to a subset of
metric key names. See [MetricsKey](rest-api/participant_enums.py) for an
up-to-date list of possible key names.

Keys may optionally be filtered as follows:

```
GET /MetricSets/:msid/Metrics?keys=GENDER,STATE
```

Example unfiltered response:

```
{
  "metrics": [
    {
      "key": "GENDER",
      "values": [
        {
          "count": 1,
          "value": "GenderIdentity_Man"
        },
        ...
      ]
    },
    {
      "key": "RACE",
      "values": [
        {
          "count": 1,
          "value": "ASIAN"
        },
        ...
      ]
    },
    {
      "key": "STATE",
      "values": [
        {
          "count": 1,
          "value": "AL"
        },
        ...
      ]
    },
    {
      "key": "AGE_RANGE",
      "values": [
        {
          "count": 2,
          "value": "0-17"
        },
        ...
      ]
    },
    {
      "key": "PHYSICAL_MEASUREMENTS",
      "values": [
        {
          "count": 12,
          "value": "COMPLETED"
        },
        {
          "count": 8,
          "value": "UNSET"
        }
      ]
    },
    {
      "key": "BIOSPECIMEN_SAMPLES",
      "values": [
        {
          "count": 11,
          "value": "COLLECTED"
        },
        {
          "count": 9,
          "value": "UNSET"
        }
      ]
    },
    {
      "key": "QUESTIONNAIRE_ON_OVERALL_HEALTH",
      "values": [
        {
          "count": 10,
          "value": "SUBMITTED"
        },
        {
          "count": 10,
          "value": "UNSET"
        }
      ]
    },
    {
      "key": "QUESTIONNAIRE_ON_PERSONAL_HABITS",
      "values": [
        {
          "count": 13,
          "value": "SUBMITTED"
        },
        {
          "count": 7,
          "value": "UNSET"
        }
      ]
    },
    {
      "key": "QUESTIONNAIRE_ON_SOCIODEMOGRAPHICS",
      "values": [
        {
          "count": 14,
          "value": "SUBMITTED"
        },
        {
          "count": 6,
          "value": "UNSET"
        }
      ]
    },
    {
      "key": "ENROLLMENT_STATUS",
      "values": [
        {
          "count": 20,
          "value": "CONSENTED"
        }
      ]
    }
  ]
}
```

## BiobankSamples API

Mayo has defined a sample manifest format that will be uploaded to the RDR
daily. The RDR scans this manifest and uses it to populate `BiobankSamples`
resources. Once these are created, a client can query for available samples:

#### TODO `GET /Participant/:pid/BiobankSamples`

## Organization API

Retrieves metadata about awardees / organizations / sites.

#### `GET /Awardee`

Retrieves a JSON bundle of metadata for all awardees, with nested resources for child
organizations and sites within them, representing the full hierarchy of
awardees > organizations > sites. No pagination, syncing, or filtering is currently
supported on this endpoint.

Accepts an `_inactive=true` parameter to allow the addition of inactive sites to
JSON bundle. The default is false.
By default, only sites with an ACTIVE status are included in the response.

Example: `GET /Awardee/:aid?_inactive=true`

Example response:

```
{
  "entry": [
    {
      "fullUrl": "http://localhost:8080/rdr/v1/Awardee/AZ_TUCSON",
      "resource": {
        "displayName": "Arizona",
        "id": "AZ_TUCSON",
        "organizations": [
          {
            "displayName": "Banner Health",
            "id": "AZ_TUCSON_BANNER_HEALTH",
            "sites": [
              {
                "address": {
                  "city": "Phoenix",
                  "line": [
                    "567 Cherry Lane"
                  ],
                  "postalCode": "66666"
                },
                "directions": "",
                "displayName": "Banner University Medical Center - Tucson",
                "id": "hpo-site-bannernew",
                "launchDate": "2017-10-02",
                "link": "http://www.example.com/",
                "mayolinkClientNumber": 5678392,
                "notes": "Formerly University of Arizona CATS Research  ",
                "phoneNumber": "666-666-6666",
                "physicalLocationName": "",
                "enrollingStatus": "INACTIVE"
                "siteStatus": "INACTIVE"
              },
              {
	        "timeZoneId": "America/Phoenix"
                "address": {
                  "city": "Tucson",
                  "line": [
                    "1234 Main Street",
                    "Suite 400"
                  ],
                  "postalCode": "55555"
                },
                "adminEmails": [
                  "bob@example.com",
                  "alice@example.com"
                ],
                "directions": "Turn left on south street.",
		"latitude": 32.8851,
		"longitude": -112.045,
                "displayName": "Banner University Medical Center - Tucson",
                "id": "hpo-site-bannertucson",
                "launchDate": "2017-10-02",
                "link": "http://www.example.com/",
                "mayolinkClientNumber": 7035650,
                "notes": "Formerly University of Arizona CATS Research  ",
                "phoneNumber": "555-555-5555",
                "physicalLocationName": "Building 23",
                "enrollingStatus": "ACTIVE",
		"digitalSchedulingStatus": "INACTIVE",
		"schedule_instructions": "Call 555-5555 to schedule",
                "siteStatus": "ACTIVE"
              }
            ]
          }
        ],
        "type": "HPO"
      }
    },
    {
      "fullUrl": "http://localhost:8080/rdr/v1/Awardee/COMM_HEALTH",
      "resource": {
        "displayName": "Community Health Center, Inc",
        "id": "COMM_HEALTH",
        "type": "FQHC"
      }
    }
    ...
  ],
  "resourceType": "Bundle",
  "type": "searchset"
}
```

#### `GET /Awardee/:aid`

Retrieves metadata about an individual awardee, with nested resources for child
organizations and sites within them.

Example response:

```
{
  "displayName": "Arizona",
  "id": "AZ_TUCSON",
  "organizations": [
    {
      "displayName": "Banner Health",
      "id": "AZ_TUCSON_BANNER_HEALTH",
      "sites": [
        {
	  "timeZoneId": "America/Phoenix",
          "address": {
            "city": "Phoenix",
            "line": [
              "567 Cherry Lane"
            ],
            "postalCode": "66666"
          },
          "directions": "",
          "displayName": "Banner University Medical Center - Tucson",
          "id": "hpo-site-bannernew",
          "launchDate": "2017-10-02",
          "link": "http://www.example.com/",
          "mayolinkClientNumber": 5678392,
          "notes": "Formerly University of Arizona CATS Research  ",
          "phoneNumber": "666-666-6666",
          "physicalLocationName": "",
          "siteStatus": "INACTIVE",
          "enrollingStatus": "INACTIVE",
	  "longitude": -110.978,
	  "latitude": 32.238
        },
        {
          "address": {
            "city": "Tucson",
            "line": [
              "1234 Main Street",
              "Suite 400"
            ],
            "postalCode": "55555"
          },
          "adminEmails": [
            "bob@example.com",
            "alice@example.com"
          ],
          "directions": "Turn left on south street.",
          "displayName": "Banner University Medical Center - Tucson",
          "id": "hpo-site-bannertucson",
          "launchDate": "2017-10-02",
          "link": "http://www.example.com/",
          "mayolinkClientNumber": 7035650,
          "notes": "Formerly University of Arizona CATS Research  ",
          "phoneNumber": "555-555-5555",
          "physicalLocationName": "Building 23",
          "enrollingStatus": "ACTIVE",
	  "digitalSchedulingStatus": "ACTIVE",
	  "schedule_instructions": "Schedule through portal",
          "siteStatus": "ACTIVE"
        }
      ]
    }
  ],
  "type": "HPO"
}
```

## Configuration APIs

`POST /CheckPpiData` Non-prod. Verifies data created during tests.

`POST /ImportCodebook` Imports the latest published Codebook (metadata about
Questionnaire contents).

## Export API

#### `POST /ExportTables`

Provides the ability to export the full contents of database tables or views to CSV files in a
specified directory in one of two GCS buckets.

Requests look like:

```
{
  "database": ["rdr" | "cdm" | "voc" ],
  "tables": "table1,table2...",
  "directory": "output_directory_name",
  "deidentify": false
}
```

The results of the export go to the GCS bucket named `<PROJECT>-rdr-export` for the rdr database,
and `<PROJECT>-cdm` for the "cdm" and "voc" databases. Exports with `deidentify` set go to a GCS
bucket named `<PROJECT>-deidentified-export`.

`deidentify` is only usable on a subset of compatible tables, and will obfuscate participant IDs
(consistently across all tables specified).
