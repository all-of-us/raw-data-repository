Participant System
************************************************************
.. TODO
   figure:: https://ipsumimage.appspot.com/640x360
   :align:  center
   :alt:    Participant System

   Figure 1, Participant System diagram.



Overview
============================================================
The Participant system is the core system of the RDR.  All other systems integrate with the Participant System either directly or indirectly.  Participants are initially created via the Participant Portal.
Participants are amended and modified with data from the Data Capture system, generally through the HealthPro client or the Participant Portal.


Participant data is aggregated and delivered to analysts by the :ref:`Reporting system <reporting>`.

.. seealso:: Participant data is also retrieved using the :ref:`Participant Summary API. <ps>`



Data Flow
============================================================

.. TODO: Insert Diagram Here
   figure:: https://via.placeholder.com/350x150
   :align:  center

   Figure 1, General Data Flow for the Participant System

A participant is first created when data is submitted through the Participant Portal client.  A POST request with an empty JSON payload will create a participant record in the `participant` table.

Various questionnaires may be presented to the participant by a clinician via the HealthPro client.  Any data captured here will be associated to the participant in the `questionnaire_response` and `questionnaire_response_answer` tables.


.. _ps_workflows:

Workflows
============================================================
.. seealso:: More details on the Participant workflows can be seen :ref:`API Workflows <api_wf>`.

Creating
----------
Participants are created via the Participant Portal.


Updating
----------
Participants are modified through the :ref:`Data Capture system <data_capture>`, which associates questionnaire, EHR, and measurement data and updates participant status.  API workflows can be seen here: :ref:`Creating and Updating Participant Data <update_participant>`.


Reading
----------
Data from the Participant system is commonly read through the :ref:`Participant Summary <ps>`. 
