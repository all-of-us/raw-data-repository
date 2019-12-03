************************************************************
Reporting APIs
************************************************************

Participant Counts Over Time
============================================================
Metrics provide a high-level overview of participants counts by date and stratification for a variety of metrics in real time or a historical cache. The date range limit is 100 days for real time data (default). Passing in ``history=true`` can provide historical data for a maximum range of 600 days.

.. note:: ParticipantCountsOverTime returns a list of objects.

**Resource:**

::

  GET /rdr/v1/ParticipantCountsOverTime

Parameters
------------------------------------------------------------

``startDate``
++++++++++++++++++++++++++++++++++++++++++++++++++
Required for certain stratifications

Passed as a string. Date is in ``YYYY-MM-DD`` format, e.g. ``2019-02-12``.

``endDate`` (required)
++++++++++++++++++++++++++++++++++++++++++++++++++
Passed as a string. Date is in ``YYYY-MM-DD`` format, e.g. ``2019-02-19``.

``stratification`` (required)
++++++++++++++++++++++++++++++++++++++++++++++++++
Passed as a string. Can be one of the values from the table below.

====================    ========================================
Stratification          Description
====================    ========================================
TOTAL                   Awardee `TOTAL` count by date.
ENROLLMENT_STATUS       Enrollment status count by date.
GENDER_IDENTITY         Gender identity count by date.
AGE_RANGE               Age range bucket counts by date.
RACE                    Race classification counts by date.
GEO_STATE               Participant count by US state code.
GEO_CENSUS              Participant count by census region.
GEO_AWARDEE             Participant count by awardee.
LIFECYCLE               Participant count by lifecycle phase.
====================    ========================================

``awardee`` (optional)
++++++++++++++++++++++++++++++++++++++++++++++++++
Passed as a string. Comma-separated list of valid awardee codes, e.g. ``PITT,VA``.


``version`` (optional)
++++++++++++++++++++++++++++++++++++++++++++++++++
Passed as a number, e.g. ``2``.

====================    ========================================================
Version Number          Description                                            ====================    ========================================================
1                       return 3 tiers participant status
2                       return 4 tiers participant status for `ENROLLMENT_STATUS`, `GEO_STATE`, `GEO_CENSUS` and `GEO_AWARDEE`; return retention modules for `LIFECYCLE` stratification
====================    ========================================================


``enrollmentStatus`` (optional)
++++++++++++++++++++++++++++++++++++++++++++++++++

Passed as a string. Comma-separated list of valid enrollment statuses.

**When version=1 or version not present:**

====================   =========================================================
Enrollment Status      Description
====================   =========================================================
INTERESTED             Correlates to the `registered` tier.
MEMBER                 Correlates to the `consented` tier.
FULL_PARTICIPANT       Correlates to the `core` participant tier.
====================   =========================================================

**When version=2:**

====================   =========================================================
Enrollment Status      Description
====================   =========================================================
REGISTERED             Correlates to the `registered` tier.
PARTICIPANT            Correlates to the `participant` tier.
FULLY_CONSENTED        Correlates to the `fully consented` tier.
CORE_PARTICIPANT       Correlates to the `core participant` tier.
====================   =========================================================

``history`` (optional)
++++++++++++++++++++++++++++++++++++++++++++++++++
Passed as a boolean. Defaults to ``TRUE``. Determines whether the counts returned are historical or "real-time."

============================================================

Metrics
============================================================
