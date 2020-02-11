BioBank System
************************************************************
.. TODO
   figure:: https://ipsumimage.appspot.com/640x360
   :align:  center
   :alt:    BioBank System

   Figure 1, BioBank System diagram.

Overview
============================================================
The BioBank System is a module that manages the association of a participant to a biospecimen stored at Mayo Clinic's BioBank. For more information regarding the biobank,
visit `NIH All of Us, BioBank <https://allofus.nih.gov/about/program-partners/biobank>`_.





Components
============================================================

Samples Subsystem
------------------------------------------------------------
Biospecimens are referred to as Samples in the RDR system. Samples are `ordered`, which means they have been entered into the HPRO portal
and sent to the biobank.
Samples are also `stored` which means the Biobank has received and processed the order.

Mayo has defined a sample manifest format that will be uploaded to the RDR daily. The RDR scans this manifest and uses it to populate `BiobankSamples` resources.
Once these are created, a client can query for available stored samples.



Direct Volunteer Subsystem
------------------------------------------------------------
Using the `supplyDelivery` and `supplyRequest` api's the Direct Volunteer portal, ran by Care Evolution, can create and update Biobank samples that are sent to
Direct Volunteer via the USPS and GenoTek api's.


