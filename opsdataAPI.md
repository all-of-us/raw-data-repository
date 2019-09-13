#ops-data-api

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
    
By default when the '_sync' parameter is passed, records modified 60 seconds before the 
last record in a batch of record will be included in the next batch of records. This 
backfill behavior may be disabled by adding the '_backfill=false' parameter.

    GET /ParticipantSummary?awardee=PITT&_sort=lastModified&_sync=true&_backfill=false

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
{
 "link": [
	{
	    "relation": "sync",
	    "url": "GET /ParticipantSummary?awardee=PITT&_sort=lastModified&_token=WzM1XQ%3D%3D"
	}
 ]
}
```

#### `GET /ParticipantSummary/Modified`

As an alternate method of synchronizing participant summary records, you can use this API call
to return 'participantId' and 'lastModified' values for all records.  This allows you to see 
which records are new and which have changed if you are storing these records in your system.

    GET /ParticipantSummary/Modified
    
For service accounts access, the awardee parameter is required. Only records matching the
awardee will be returned.

    GET /ParticipantSummary/Modified?awardee=PITT

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
