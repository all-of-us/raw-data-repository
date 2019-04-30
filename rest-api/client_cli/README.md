# API Client Tools

This is the replacement for the older client programs located in the 
`raw-data-repository/rdr_client` directory. The new tool design replaces 
the `run_client.sh` bash shell program with the python client command 
launcher `rdr.py`.

The benefits of the new command launcher are; 

* Integrated help for each client command.
* Easy debugging, the client commands are written completely in python.
* Integrated GCP service account key management.  Manually creating and 
downloading IAM keys is not necessary.

### Running the tool

Depending on your OS, the python interpreter might need to be specified 
before the `rdr.py` program. The examples shown will omit the python 
interpreter. 

```
python rdr.py [tool name] OR rdr.py [tool name]
```


To get get a list of available commands run: 

```
rdr.py --help
```

To get help for a specific command run:

```
rdr.py [tool name] --help
```

#### Environment Variables

These environment variables can be set before running the launcher to remove 
the need to set program parameters.

##### RDR_ACCOUNT

Setting this environment variable, before running the client launcher, allows 
you to not need to pass the program argument `--account`. On unix like systems 
this can be done by the running the following command. 

```
export RDR_ACCOUNT=xxxx.xxxx.@pmi-ops.org
```

##### RDR_SERVICE_ACCOUNT

Setting this environment variable, before running the client launcher, allows 
you to not need to pass the program argument `--service-account`. On unix like systems 
this can be done by the running the following command. 

```
export RDR_SERVICE_ACCOUNT=xxxx@xxxx.iam.gserviceaccount.com
```

## Client Tool Commands

### Verify Environment

For first time users, a tool to verify the local environment is provided. 
This tool should be run to make sure the local environment is correctly
configured for the other client tool commands. If there are errors in the 
output of this command, those errors should be corrected before running
any other commands. 

For a simple test run:

```
rdr.py verify
```

To run extended tests, project a valid GCP project and authorized account:

```
rdr.py verify --project all-of-us-rdr-[xxxx] --account xxxx.xxxx@pmi-ops.org 
```

If you need to use a service account, run:

```
rdr.py verify --project all-of-us-rdr-[xxxx] --account xxxx.xxxx@pmi-ops.org --service-account xxxx@xxxx.iam.gserviceaccount.com   
```

### Random Participant Data Generator

This tool creates a number of randomly generated participants. 

```
rdr.py random-gen
``` 

#### Required Program Arguments

*--num_participants [Integer]*: The number of of participants to create.

#### Optional Program Arguments

*--include_biobank_orders*: Create BioBank orders for the participants.

*--hpo*: The name of a HPO to use for the participants.

*--create_biobank_samples*: Create BioBank samples for the participants.

*--create_samples_from_file [Filename]*: Specify a file with samples.

### Spec Participant Data Generator

This tool creates very specific test participants. Everything about the test
participant may be specified, including:

* Questionnaire modules
* Answers to specific questionnaire module questions
* Physical Measurements for participant
* Biobank orders
* Biobank samples
* Sending Biobank orders to Mayolink 


#### Required Program Arguments

*--src-csv [Filename|Google Doc ID]*: The filename for a local CSV 
file or a Google spreadsheet doc ID.

#### Source Spreadsheet Specifications

The first column of the source spreadsheet must contain identifiers.  Each additional
column specifies the data to use to create an individual test participant.

Note: Special identifiers are prefixed with an underscore. Identifiers without
an underscore prefix are assumed to be questionnaire question IDs found in the code
book.   


*_HPO*: HPO name

*_HPOSite*: HPO site name (overrides _HPO if _HPO value is also given) 

*_PM*: Add participant physical measurements (requires 'ConsentPII' module)

*_BIOOrder*: Create BioBank order, one sample test ID per line.

*_PPIModule*: Create answers to questionnaire, one module name per line. 


##### Example Of Spec Participants Data

```
+-------------------------------------------------------------+
|         A         |          B         |          C         |
|-------------------|--------------------|--------------------|
| _HPO              | Test               |                    |
| _HPOSite          |                    | hpo-site-tester    |
| _BIOOrder         | 1SAL2              | 1SAL2              |
| _BIOOrder         | 1ED04              | 1ED04              |
| _PPIModule        | ConsentPII         | ConsentPII         |
| _PPIModule        | TheBasics          | TheBasics          |
| _PM               | yes                | no                 |
| PIIName_First     | John               | Jane               |
| PIIName_Middle    | C                  |                    |
| PIIName_Last      | Doe                | Doe                |
| StateOfResidence  | SOR_DC             | SOR_TX             |
+-------------------------------------------------------------+
```

