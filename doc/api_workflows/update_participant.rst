
.. _update_participant:

************************************************************
Creating and Updating Participant Data
************************************************************

* `Resource Versioning`_
* `Workflows`_

  * `Create Participant Questionnaire Response`_
  * `Create Participant Physical Measurements`_
  * `Cancellation/Amending/Restoring Physical Measurements`_

  * `BioBank Orders`_

    * `Create a BioBank Order`_
    * `Edit a BioBank Order`_

Resource Versioning
============================================================
When updating a resource via PUT, the RDR API requires a client to provide an HTTP “If-Match” header that points to the current version of a resource. This prevents a client from blindly overwriting data that it hasn’t had the opportunity to see yet. The client uses the value from a resource’s “ETag” header, following RFC7232 (or, for a more readable tutorial, see this `article <https://fideloper.com/etags-and-optimistic-concurrency-control>`_).
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
Create a new QuestionnaireResponse in the RDR. Body is a `FHIR DSTU2 QuestionnaireResponse <http://hl7.org/fhir/questionnaireresponse.html>`_ resource which must include:

* ``subject``: a reference to the participant, in the form ``Patient/:pid``. The :pid variable of this refernce must match the participant ID supplied in the POST URL.

.. note:: Note the use of the word "Patient" here, which comes from the FHIR spec.

* ``questionnaire``: a reference to the questionnaire for which this response has been written, in the form ``Questionnaire/:qid`` or ``Questionnaire/:qid/_history/:version`` (the latter indicating the version of the questionnaire in use)
* ``linkId`` for each question, corresponding to a ``linkId`` specified in the questionnaire.


**Request:**

::

  POST /rdr/v1/Participant/P999999999/QuestionnaireResponse
  Body:

  {
    "subject": {
      "reference":"Patient/P999999999"
    },
    "questionnaire": {
      "reference": "Questionnaire/33/_history/1"
    },
    linkId: "38622"
  }


Create Participant Physical Measurements
------------------------------------------------------------
We use the FHIR ``Document`` model to represent a set of physical measurements recorded at a PMI visit. This stores a ``Bundle`` of resources, with a ``Composition`` as the first entry (listing basic metadata for the document, including a document type, creation time, author, and an index of contents), and a set of ``Observation`` as subsequent entries (recording, for example, individual blood pressure or weight measurements).

**Request:**
::

  POST /Participant/P999999999/PhysicalMeasurements
  Body:

  {
    "status":"final",
    "code":{
       "text":"text",
       "coding":[
          {
             "code":"62409-8",
             "display":"measurement",
             "system":"http://loinc.org"
          },
          {
             "code":"hip-circumference-1",
             "display":"measurement",
             "system":"http://terminology.pmi-ops.org/CodeSystem/physical-measurements"
          }
       ]
    },
    "resourceType":"Observation",
    "related":{
      "type":"qualified-by",
      "target":{
        "reference":"urn:example:protocol-modifications-hip-circumference"
      }
    },
    "valueQuantity":{
       "value":146.2,
       "code":"cm",
       "system":"http://unitsofmeasure.org",
       "unit":"cm"
    },
    "effectiveDateTime":"2019-05-29T16:51:23.461736",
    "subject":{
       "reference":"Patient/P999999999"
    }
  }


If these measurements are an amendment of previously submitted measurements, that can be indicated in the request body with the following extension:

::

  "extension": [{
  "url": "http://terminology.pmi-ops.org/StructureDefinition/amends",
  "valueReference": {
      "reference": "PhysicalMeasurements/:measurements_id"
    }
  }


Cancellation/Amending/Restoring Physical Measurements
------------------------------------------------------------

**Cancelled**

::

  PATCH /Participant/:pid/PhysicalMeasurements/:id

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

  PATCH /Participant/:pid/PhysicalMeasurements/:id

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

  PATCH /Participant/:pid/PhysicalMeasurements/:id

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


BioBank Orders
------------------------------------------------------------
The BioBank Order API maintains records of orders placed from HealthPro to the Biobank. Each order is a resource as documented here, including:

* ``subject``: a reference to the participant, in the form ``Patient/:pid``.  The :pid variable of this refernce must match the participant ID supplied in the POST URL.

.. note:: Note the use of the word "Patient" here, which comes from FHIR.

* ``identifier``: an array of Identifiers, each with a system and value. These should include the HealthPro identifier for this order as well as the biobank identifier for this order.


Create a BioBank Order
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

Create a new BiobankOrder for a given participant. Request body is a BiobankOrder resource to be created. Response is the resource as stored.

**Request:**

::

  POST /Participant/P124820391/BiobankOrder

  {
    "subject": "Patient/P124820391",
    "identifier": [
      {
        "system": "http://health-pro.org",
        "value": "healthpro-order-id-123"
      },
      {
        "system": "https://orders.mayomedicallaboratories.com",
        "value": "mayolink-order-id-456"
      }
    ],
    "created": "2016-01-04T09:40:21Z",
    "samples": [
      {
        "test": "1ED10",
        "description": "EDTA 10 mL (1)",
        "processingRequired": false,
        "collected": "2016-01-04T09:45:49Z",
        "finalized": "2016-01-04T10:55:41Z"
      },
      {
        "test": "1PST8",
        "description": "Plasma Separator 8 mL",
        "collected": "2016-01-04T09:45:49Z",
        "processingRequired":true,
        "processed": "2016-01-04T10:28:50Z",
        "finalized": "2016-01-04T10:55:41Z"
      },
      {
        <<as above, etc through sample #8>>
      }
    ],
    "notes": {
      "collected": "Only got 7mL in the ED10 tubes",
      "processed": "Centrifuge was not cooled to the proper temperature",
      "finalized": "Prepped samples in 4C fridge, Room 520A"
    }
  }



Edit a BioBank Order
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
Cancel or restore a BiobankOrder by id.

An edited biobank order (cancel/restore/amend) has a payload as follows.

**Request:**

::

  PATCH /Participant/:pid/BiobankOrder
  Body:

  {
    "amendedReason": "Text justification",
    "cancelledInfo": {
      "author": {
        "system": "https://www.pmi-ops.org/healthpro-username",
        "value": "name@pmi-ops.org"
      },
      "site": {
        "system": "https://www.pmi-ops.org/site-id",
        "value": "hpo-site-somesite"
      }
    },
    "status": "cancelled"
  }
