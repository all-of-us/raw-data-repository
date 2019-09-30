
.. _search_participant:

Searching and Filtering Participant Data
************************************************************

.. _ps:

Participant Summary
============================================================

The ParticipantSummary resource represents an aggregated view of relevant participant details, including data from consent (name, contact information), from PPI modules (a status indicating whether the participant has completed each questionnaire), basic demographics (age, gender, race).

Querying the Participant Summary Resource
------------------------------------------------------------
.. note:: This content will be updated with Synchronizing queries as well.

**Request:**

::

  GET /ParticipantSummary?

**Response**:

::

  {

  }

------------------------------------------------------------

Participant Questionnaire Answers
============================================================
Return the participant's answers for the given Codebook module name. Response will include multiple sets of answers if the participant has completed the module questionnaire multiple times. The most recent questionnaire will be first.

Multiple choice answers will be combined into one response column separated by commas, see example below.

Returns a Http 404 error if the participant has not yet submitted the questionnaire.

Additional parameters:

* ``skip_null`` If true, do not return answers with null values.

* ``fields`` A comma separated list of questionnaire answers to return.


**Request:**

::

  GET /Participant/:pid/QuestionnaireAnswers/:module

  GET /Participant/P123456789/QuestionnaireAnswers/TheBasics

  GET /Participant/P123456789/QuestionnaireAnswers/ConsentPII?skip_null=true

  GET /Participant/P123456789/QuestionnaireAnswers/TheBasics?fields=Race_WhatRaceEthnicity,Gender_CloserGenderDescription

**Response**:

::

  [
    {
      "questionnaire_id": 2,
      "questionnaire_response_id": 012345678,
      "created": "2019-02-14T22:34:16",
      "code_id": 1,
      "version": 1,
      "authored": "2019-02-14T22:33:40",
      "language": "en",
      "participant_id": "P123456789",
      "module": "TheBasics",
      "Gender_CloserGenderDescription": null,
      "Race_WhatRaceEthnicity": "WhatRaceEthnicity_Black,WhatRaceEthnicity_White"
    }
  ]

------------------------------------------------------------

Physical Measurements
============================================================

The following queries can be used to search a ``Participant``'s ``Physical Measurements``.


PhysicalMeasurements by ID
------------------------------------------------------------

**Request:**

::

  GET /Participant/:pid/PhysicalMeasurements/:id

**Response**:

::

  {

  }


------------------------------------------------------------

PhysicalMeasurements for a Participant
------------------------------------------------------------

Search for all PhysicalMeasurements available for a given participant. Response body is a Bundle (possibly empty) of documents (that is: a bundle of search results whose entries are bundles of measurements).

**Request:**

::

  GET /Participant/:pid/PhysicalMeasurements

**Response**:

::

  {

  }
