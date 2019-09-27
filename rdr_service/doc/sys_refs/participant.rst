Participant System
************************************************************
.. figure:: https://ipsumimage.appspot.com/640x360
   :align:  center
   :alt:    Participant System

   Figure 1, Participant System diagram.



Overview
============================================================
The Participant system is the core system of the RDR.  All other systems integrate with the Participant System either directly or indirectly.  Participants are initially created via the Participant Portal.  Participants are amended and modified with data from the Data Capture system, generally through the HealthPro client by clinicians, though there are exceptions to this which will be discussed below, namely (*EHR* and *Direct Volunteers*).

.. TODO: discuss the exceptions mentioned above

Participant data is aggregated and delivered to analysts by the Reporting system.  Participant data is also retrieved using the Participant Summary API. This is described here (TODO: LINK).



Data Flow
============================================================

.. TODO: Insert Diagram Here
.. figure:: https://via.placeholder.com/350x150
   :align:  center

   Figure 1, General Data Flow for the Participant System

A participant is first created when data is submitted through the Participant Portal client.  A POST request with an empty JSON payload will create a participant record in the `participant` table.

Various questionnaires may be presented to the participant by a clinician via the HealthPro client.  Any data captured here will be associated to the participant in the `questionnaire_response` and `questionnaire_response_answer` tables.


.. _ps_workflows:

Workflows
============================================================
More details on the Participant workflows can be seen here (TODO: add link).

Creating
----------
Participants are created via the Participant Portal.


Updating
----------
Participants are modified through the Data Capture system (TODO: LINK), which associates questionnaire, EHR, and measurement data and updates participant status.


Reading
----------
Commonly viewed via the participant summary (TODO: LINK).
