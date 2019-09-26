Reporting System
************************************************************
.. figure:: https://ipsumimage.appspot.com/640x360
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
(TODO: clarify...)


Participant Summary
------------------------------------------------------------
The Participant Summary, also part of the Participant Core System, is a denormalized construction of all data related to a participant.



Workflows
============================================================
.. TODO:


Object Model
============================================================
Below are the most important objects implemented for the Reporting System.

Metrics
------------------------------------------------------------

Metrics Data Model
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
.. automodule:: rdr_service.model.metrics
   :members:
   :undoc-members:
   :exclude-members: MetricsVersion

.. automodule:: rdr_service.model.metrics_cache
   :members:
   :undoc-members:

Metrics Data Access Objects
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
.. automodule:: rdr_service.dao.metrics_dao
   :members: MetricsBucketDao
   :no-inherited-members:
   :exclude-members: get_id, insert_with_session, get_with_children_with_session, get_with_children, insert, get_with_session, from_client_json, to_client_json, update_with_patch

.. automodule:: rdr_service.dao.metrics_cache_dao
   :members:
   :no-inherited-members:
   :exclude-members: get_id, insert_with_session, get_with_children_with_session, get_with_children, insert, get_with_session, from_client_json, to_client_json, update_with_patch

Participant Summary
------------------------------------------------------------

.. _Participant Summary DM:

Participant Summary Data Model
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
.. automodule:: rdr_service.model.participant_summary
   :members:    ParticipantSummary
   :undoc-members:
