BioBank System
************************************************************
.. TODO
   figure:: https://ipsumimage.appspot.com/640x360
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
Biospecimens are referred to as Samples in the RDR system. Samples are `ordered`, which means they are ...? or `stored`

Mayo has defined a sample manifest format that will be uploaded to the RDR daily. The RDR scans this manifest and uses it to populate `BiobankSamples` resources. Once these are created, a client can query for available samples.



Direct Volunteer Subsystem
------------------------------------------------------------



Workflows
============================================================
.. TODO

TODO: Include details of FHIR supply requests
