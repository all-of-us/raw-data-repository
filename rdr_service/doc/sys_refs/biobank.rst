BioBank System
************************************************************
.. figure:: https://ipsumimage.appspot.com/640x360
   :align:  center
   :alt:    BioBank System

   Figure 1, BioBank System diagram.

Overview
============================================================
The BioBank System is a module that manages the association of a participant to a biospecimen stored at Mayo Clinic's BioBank. For more information regarding the biobank, visit `NIH All of Us, BioBank <https://allofus.nih.gov/about/program-partners/biobank>`_.


.. TODO

TODO: Include details of FHIR supply requests


Components
============================================================

Samples Subsystem
------------------------------------------------------------
(TODO: learn more about this)
Biospecimens are referred to as Samples in the RDR system. Samples are `ordered`, which means they are ...? or `stored`

Mayo has defined a sample manifest format that will be uploaded to the RDR daily. The RDR scans this manifest and uses it to populate `BiobankSamples` resources. Once these are created, a client can query for available samples.



Direct Volunteer Subsystem
------------------------------------------------------------



Workflows
============================================================
.. TODO

TODO: Include details of FHIR supply requests


Object Models
============================================================
Below are the most important objects implemented for the BioBank System.


Samples Subsystem
------------------------------------------------------------

BioBank Ordered Sample
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

BiobankOrder Data Model
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: rdr_service.model.biobank_order
   :members:
   :exclude-members: BiobankOrderHistory, BiobankOrderIdentifierHistory, BiobankOrderedSampleHistory


BiobankOrder Access Objects
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: rdr_service.dao.biobank_order_dao
   :members: BiobankOrderDao
   :no-inherited-members:
   :exclude-members: get_id, insert_with_session, get_with_children_with_session, get_with_children, insert, get_with_session, from_client_json, to_client_json, update_with_patch


BioBank Stored Sample
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

Biobank Stored Sample Data Model
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: rdr_service.model.biobank_stored_sample
   :members:


Biobank Stored Sample Data Access Objects
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: rdr_service.dao.biobank_stored_sample_dao
   :members: BiobankStoredSampleDao
   :no-inherited-members:
   :exclude-members: get_id, insert_with_session, get_with_children_with_session, get_with_children, insert, get_with_session, from_client_json, to_client_json, update_with_patch


Direct Volunteer Subsystem
------------------------------------------------------------

Direct Volunteer Data Model
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
.. automodule:: rdr_service.model.biobank_dv_order
   :members:


Direct Volunteer Data Access Objects
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
.. automodule:: rdr_service.dao.dv_order_dao
   :members: DvOrderDao
   :no-inherited-members:
   :exclude-members: get_id, insert_with_session, get_with_children_with_session, get_with_children, insert, get_with_session, from_client_json, to_client_json, update_with_patch
