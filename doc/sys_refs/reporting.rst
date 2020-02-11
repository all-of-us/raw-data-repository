
.. _reporting:

Reporting System
************************************************************
.. todo
   figure:: https://ipsumimage.appspot.com/640x360
   :align:  center
   :alt:    Reporting System

   Figure 1, Reporting System diagram.


Overview
============================================================
The Reporting System is a module that provides caching, aggregation, and filtering reports of RDR data for PMI metrics, the Dashboard, and external analysts.


Components
============================================================

Metrics
------------------------------------------------------------
The Metrics component provides cached reporting for various data of interest to PMI...
This uses the `ParticipantCountsOverTime` API to generate time series entries into the database which can be queried for use in dashboards.
If you have a pmi-ops login you can see an example here: `pmi ops dashboard <https://www.pmi-ops.org/dashboard/total-progress>`_


Participant Summary
------------------------------------------------------------
The Participant Summary, also part of the Participant Core System, is a flat construction of all data related to a participant.
The ``ParticipantSummary`` and ``opsData`` API's are used to retrieve up to date information regarding participants.

.. seealso:: See the reference here :ref:`Participant Summary API. <ps>`

Workflows
============================================================
.. TODO:
