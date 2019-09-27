************************************************************
Creating and Updating Participant Data Workflows
************************************************************

Resource Versioning
============================================================
When updating a resource (via PUT), the RDR API requires a client to provide an HTTP “If-Match” header that points to the current version of a resource. This prevents a client from blindly overwriting data that it hasn’t had the opportunity to see yet. The client uses the value from a resource’s “ETag” header, following RFC7232 (or, for a more readable tutorial, see this `article <https://fideloper.com/etags-and-optimistic-concurrency-control>`_).
Concretely, a request looks like:

::

  PUT /rdr/v1/Participant/P123456789
  If-Match: W/"98172982174921"

  {
    "providerLink": [{
      "primary": true,
      "site": "Organization/PITT",
    }]
  }

And this request either fails (if the supplied ETag does not match the current resource version), or it succeeds in updating the resource and returns a new ETag in a header like like:

::

  ETag: W/"99817291822"


.. note:: *ETag* values are also returned in a ``meta.versionId`` property within each resource, following FHIR’s convention.

Workflows
============================================================

Create Participant Questionnaire Response
------------------------------------------------------------
Create a new QuestionnaireResponse in the RDR. Body is a FHIR DSTU2 QuestionnaireResponse resource which must include:

* ``subject``: a reference to the participant, in the form ``Patient/:pid``. The :pid variable of this refernce must match the participant ID supplied in the POST URL.

.. note:: Note the use of the word "Patient" here, which comes from FHIR.

* ``questionnaire``: a reference to the questionnaire for which this response has been written, in the form ``Questionnaire/:qid`` or ``Questionnaire/:qid/_history/:version`` (the latter indicating the version of the questionnaire in use)
* ``linkId`` for each question, corresponding to a ``linkId`` specified in the questionnaire.


**Request:**

::

  POST /rdr/v1/Participant
  Body:

  {

  }

**Response**:

::

  {

  }


Create Participant Physical Measurements
------------------------------------------------------------
We use the FHIR ``Document`` model to represent a set of physical measurements recorded at a PMI visit. This stores a ``Bundle`` of resources, with a ``Composition`` as the first entry (listing basic metadata for the document, including a document type, creation time, author, and an index of contents), and a set of ``Observation`` as subsequent entries (recording, for example, individual blood pressure or weight measurements).

**Request:**

::

  POST /Participant/:pid/PhysicalMeasurements
  Body:

  {

  }

**Response**:

::

  {

  }


Cancellation/Amending/Restoring Physical Measurements
------------------------------------------------------------

**Cancelled**

::

  {
    "cancelledInfo": {
      "author": {
        "system": "https://www.pmi-ops.org/healthpro-username",
        "value": "name@pmi-ops.org"
      },
      "site": {
        "system": "https://www.pmi-ops.org/site-id",
        "value": "hpo-site-somesitename"
      }
    },
    "reason": "text field for justification",
    "status": "cancelled"
  }


**Restored**

::

  {
    "reason": "Fixed something...",
    "restoredInfo": {
      "author": {
        "system": "https://www.pmi-ops.org/healthpro-username",
        "value": "name@pmi-ops.org"
      },
      "site": {
        "system": "https://www.pmi-ops.org/site-id",
        "value": "hpo-site-monroeville"
      }
    },
    "status": "restored"
  }


**Amendment uses the FHIR amends extension to identify amended measurements**

::

  {
    "extension": [{
      "url": "http://terminology.pmi-ops.org/StructureDefinition/amends",
      "valueReference": {
        "reference": "PhysicalMeasurements/%(physical_measurement_id)s"
      }
    }]
  }


This will change the status to ``CANCELLED/RESTORED/AMENDED`` as appropriate. When syncing against the ``PhysicalMeasurements/_history`` api check for this field specifically. Other fields of interest on edited measurements are:

::
  
  cancelled_username
  cancelled_site_id
  cancelled_time
  reason

An amended PhysicalMeasurement will have an amended_measurement_id that points to the original measurement. These are defined by the enum ``PhysicalMeasurementsStatus``.


Update Participant
------------------------------------------------------------

Update Participant
------------------------------------------------------------

Update Participant
------------------------------------------------------------

Update Participant
------------------------------------------------------------

Update Participant
------------------------------------------------------------
