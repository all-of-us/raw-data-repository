.. RDR Systems Documentation documentation master file, created by
   sphinx-quickstart on Wed Sep 25 08:20:19 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

+++++++++++++++++++++++++++++++++++++++++
The *All of Us* Raw Data Repository (RDR)
+++++++++++++++++++++++++++++++++++++++++

.. figure:: img/aou_flow_chart.jpg
   :align:  center
   :alt:    All of Us flowchart

   Figure 1, the RDR as it fits within the context of the *All of Us* research program.


Purpose
================================================================================
The main purpose of this documentation is to provide a high-level description of the core systems and extended modules of the *All of Us* Raw Data Repository (RDR).  A secondary purpose is to provide some lower-level details of certain fundamental objects that provide the functionality of the RDR core and modular systems.
The target audience of this documentation are non-RDR developers and other All of Us personnel that wish to have a better understanding of the core and extended systems of the RDR.


Table of Contents
================================================================================
.. Table of Contents section

.. toctree::
   :maxdepth: 3

   sys_ref
   api_wf


General Concepts
================================================================================

Terminology
--------------------------------------------------------------------------------
Explain System, Components, DAO, Data Model, Object Model, API, api client.

The above concepts are illustrated in Figure 2:

.. figure:: https://ipsumimage.appspot.com/640x360
   :align:  center
   :alt:    General Concepts illustrated.

   Figure 2, General Concepts of the RDR, illustrated.


API Workflows
--------------------------------------------------------------------------------
The following are the API workflows that the RDR supports and are covered in this documentation:

   *  PTC to RDR (Raw Data Repository)
       *  Create and update Participant information
       *  Create and update Questionnaires and Responses ("PPI", Participant-provided information)
       *  Read data about a participant
   *  Health Professional Portal to RDR
       *  Search Participants
          *  At check-in time, look up an individual by name, date of birth, zip code
          *  For a Work Queue, filter participants based on summary information (e.g. age, race)
       *  Get Participant Summary (by id)
       *  Update a Participant with a new Medical Record Number
       *  Insert results from physical measurements
       *  Insert biospecimen orders
   *  BioBank to RDR
       *  Google Cloud Storage “add a csv file to bucket”, performed daily
   *  For release management and operational monitoring
       *  Serving version identifier - no auth required




Further Reading
================================================================================
* `All of Us <https://www.joinallofus.org/en/about>`_
* `RDR on GitHub <https://github.com/all-of-us/raw-data-repository>`_
