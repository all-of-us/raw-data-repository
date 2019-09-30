Healthcare Provider Organization (HPO) System
************************************************************
.. TODO
   figure:: https://ipsumimage.appspot.com/640x360
   :align:  center
   :alt:    HPO System

   Figure 1, HPO System diagram.


Overview
============================================================
The HPO System is a module that manages the association of a participant to a particular site through a healthcare provider organization (HPO).  HPOs are are referred to as "Awardee" in the API. Awardees have organizations as children, and organizations have sites as children. A participant will be assigned a site when their data is entered or modified via the HealthPro client.


Components
============================================================


HPO
------------------------------------------------------------
The HPO object is a representation of a Healthcare Provider Organization.  This object is referred to as Awardee in the API.  A participant will be assigned a hpo_id when data is entered through HealthPro.


Organization
------------------------------------------------------------
The Organization object is a child of Awardee/HPO and a parent of Sites.  A participant will be assigned an organization_id when data is entered through HealthPro.


Site
------------------------------------------------------------
The Site object represents a physical location where a Healthcare Organization entered participant data.  The site object has a number of fields that relate to the physical location, including address and coordinates, as well as other data related to the particular site.



Workflows
============================================================
.. TODO Is there a system like the codebook in place to update awardee/sites/etc.?

Retrieve Metadata for HPO Heirarchy
------------------------------------------------------------
To retrieve metadata about all awardees, organizations, and sites, the following API workflow can be used:

``GET /Awardee``

The returned JSON will have nested resources for children and will represent the full hierarchy of Awardees (HPO) > Organizations > Sites

Additionally, an awardee_id can be supplied to retrieve metadata about an individual awardee:

``GET /Awardee/:aid``
