************************************************************
Patient API Reference
************************************************************

Overview
========

The Patient endpoint is used to update a the contact information for a participant.
The endpoint can be accessed using the url path */rdr/v1/Patient*.

The API expects POST requests to be sent, and the body of the request should be a JSON structure that
adheres to the `FHIR Patient v4 definition <http://hl7.org/fhir/R4/patient.html>`_.

The `id` field of the FHIR payload must be the participant id value to set the values for.
The following information can be sent to update a participant's summary data in the RDR:

*   first name
*   middle name
*   last name
*   phone number
*   email address
*   birthdate
*   address (lines 1 and 2, city, state, zip code)
*   preferred language

The following shows the full expected structure, highlighting where each data element should appear::

    {
        "id": "P123123123",                                 // Participant id for the participant to update
        "name": [{
            "given": [
                "Peter",                                    // first name
                "Walter"                                    // middle name
            ],
            "family": "Bishop"                              // last name
        }],
        "telecom": [
            {
                "system": "phone",
                "value": "1234567890"                       // phone number
            },
            {
                "system": "email",
                "value": "test@example.org"                 // email address
            }
        ],
        "birthdate": "1980-01-21",                          // birthdate
        "address": [
            {
                "line": [                                   // lines 1 and 2 of residence address
                    "123 Main St.",
                    "Apt C"
                ],
                "city": "New Haven",                        // city of residence
                "state": "CA",                              // state of residence
                "postalCode": "12345"                       // zip code of residence
            }
        ],
        "communication": [
            {
                "preferred": true,
                "language": {
                    "coding": [
                        {
                            "code": "es"                    // preferred language
                        }
                    ]
                }
            }
        ]
    }

Further Details
===============

Specific data elements (such as address or name) can be updated individually without having to send any others that
would remain unchanged. For example, suppose a participant P111222333 currently has their first name set to "Sam"
and their last name set to "Doe". The following data can be sent to update their last name to "Smith"::

    {
        "id": "P111222333",
        "name": [{
            "family": "Smith"
        }]
    }

In the above example, P111222333's last name would be updated from "Doe" to "Smith" while their first name of "Sam"
(as well as any other data, such as phone number and address) would remain unchanged.

Additionally, if for any reason a field needs to be cleared, that field can be cleared by sending an empty string value.
Similar to our above example, the following example JSON would clear the participant's last name::

    {
        "id": "P111222333",
        "name": [{
            "family": ""
        }]
    }

The following sections describe the nuances specific to some parts of the payload.

given name
----------
The given name lists a participant's first and middle name. In order to specify the middle name, the first name must be
present.

address line 2
--------------
Similar to a participant's middle name, line 1 of the address must be provided to be able to reference
line 2 of the address.
