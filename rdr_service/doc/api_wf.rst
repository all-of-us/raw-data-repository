************************************************************
API Workflows
************************************************************
.. figure:: https://ipsumimage.appspot.com/640x360
   :align:  center

   Figure 1, The *All of Us Raw* Data Repository API Workflows

General Workflows
============================================================
.. toctree::
   :maxdepth: 3

   Create Participant Workflow <api_workflows/create_participant>
   Creating and Updating Participant Data Workflows <api_workflows/update_participant>
   Search and Filter Participants <api_workflows/search_participant>
   API Resource Reference <api_workflows/api_resource_ref>


Task Specific Workflows
============================================================

PTC to RDR (Raw Data Repository)
------------------------------------------------------------
* Create and update Participant information
* Create and update Questionnaires and Responses ("PPI", Participant-provided information)
* Read data about a participant

Health Professional Portal to RDR
------------------------------------------------------------
* Search Participants

  * At check-in time, look up an individual by name, date of birth, zip code
  * For a Work Queue, filter participants based on summary information (e.g. age, race)


* Get Participant Summary (by id)
* Update a Participant with a new Medical Record Number
* Insert results from physical measurements
* Insert biospecimen orders

BioBank to RDR
------------------------------------------------------------
  * Google Cloud Storage “add a csv file to bucket”, performed daily

For release management and operational monitoring
------------------------------------------------------------
  * Serving version identifier - no auth required
