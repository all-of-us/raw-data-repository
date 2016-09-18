DROP TABLE participant;
DROP TABLE evaluation;
DROP TABLE question;
DROP TABLE questionnaire_group;
DROP TABLE questionnaire;


CREATE TABLE participant
(
    participant_id VARCHAR(200) NULL,
    drc_internal_id VARCHAR(200) NOT NULL,
    biobank_id VARCHAR(200) NULL,
    first_name VARCHAR(2048) NULL,
    middle_name VARCHAR(2048) NULL,
    last_name VARCHAR(2048) NULL,
    zip_code VARCHAR(2048) NULL,
    date_of_birth DATETIME(6) NULL,
    enrollment_status INT NULL,
    physical_exam_status INT NULL,

    PRIMARY KEY (drc_internal_id),
    CONSTRAINT unique_participant_id UNIQUE (participant_id),
    CONSTRAINT unique_biobank_id UNIQUE (biobank_id)
);

CREATE TABLE evaluation
(
    evaluation_id VARCHAR(200) NOT NULL,
    participant_drc_id VARCHAR(200) NOT NULL,
    completed DATETIME(6) NULL,
    evaluation_version VARCHAR(100) NULL,
    evaluation_data MEDIUMTEXT NULL,

    PRIMARY KEY (participant_drc_id, evaluation_id)
);

create table question
(
    questionnaire_id VARCHAR(200) NOT NULL,
    question_id VARCHAR(200) NOT NULL,
    parent_id VARCHAR(200) NOT NULL,
    ordinal INTEGER NOT NULL,
    linkId MEDIUMTEXT NULL,
    concept MEDIUMTEXT NULL,
    text MEDIUMTEXT NULL,
    type MEDIUMTEXT NULL,
    required BOOLEAN NULL,
    repeats BOOLEAN NULL,
    options MEDIUMTEXT NULL,
    option_col MEDIUMTEXT NULL,
    extension MEDIUMTEXT NULL,
    
    PRIMARY KEY (questionnaire_id, question_id)
);

create table questionnaire_group
(
    questionnaire_id VARCHAR(200) NOT NULL,
    questionnaire_group_id VARCHAR(200) NOT NULL,
    parent_id VARCHAR(200) NOT NULL,
    ordinal INTEGER NOT NULL,
    linkId MEDIUMTEXT NULL,
    title MEDIUMTEXT NULL,
    concept MEDIUMTEXT NULL,
    text MEDIUMTEXT NULL,
    type MEDIUMTEXT NULL,
    required BOOLEAN NULL,
    repeats BOOLEAN NULL,

    PRIMARY KEY (questionnaire_id, questionnaire_group_id, parent_id)
);


create table questionnaire
(
    resourceType TEXT NULL,
    id VARCHAR(200) NOT NULL,
    identifier MEDIUMTEXT NULL,
    version MEDIUMTEXT NULL,
    status MEDIUMTEXT NULL,
    date MEDIUMTEXT NULL,
    publisher MEDIUMTEXT NULL,
    telecom MEDIUMTEXT NULL,
    subjectType MEDIUMTEXT NULL,
    text MEDIUMTEXT NULL,
    contained MEDIUMTEXT NULL,
    extension MEDIUMTEXT NULL,

    PRIMARY KEY (id)
);

create table questionnaire_response
(
    resourceType TEXT NULL,
    id VARCHAR(200) NOT NULL,
    meta MEDIUMTEXT NULL,
    implicitRules MEDIUMTEXT NULL,
    language MEDIUMTEXT NULL,
    text MEDIUMTEXT NULL,
    contained MEDIUMTEXT NULL,
    identifier MEDIUMTEXT NULL,
    questionnaire MEDIUMTEXT NULL,
    status MEDIUMTEXT NULL,
    subject MEDIUMTEXT NULL,
    author MEDIUMTEXT NULL,
    authored DATETIME(6) NULL,
    source MEDIUMTEXT NULL,
    encounter MEDIUMTEXT NULL,
    extension MEDIUMTEXT NULL,

    PRIMARY KEY (id)
);

create table answer
(
    answer_id VARCHAR(200) NOT NULL,
    questionnaire_response_id VARCHAR(200) NOT NULL,
    parent_id VARCHAR(200) NOT NULL,
    ordinal INTEGER NOT NULL,
    valueBoolean BOOLEAN NULL,
    valueDecimal DOUBLE NULL,
    valueInteger INTEGER NULL,
    valueDate VARCHAR(640) NULL,
    valueDateTime VARCHAR(640) NULL,
    valueInstant VARCHAR(640) NULL,
    valueTime VARCHAR(640) NULL,
    valueString MEDIUMTEXT NULL,
    valueUri MEDIUMTEXT NULL,
    valueAttachment LONGTEXT NULL,
    valueCoding MEDIUMTEXT NULL,
    valueQuantity MEDIUMTEXT NULL,
    valueReference MEDIUMTEXT NULL,

    PRIMARY KEY (questionnaire_response_id, answer_id) 
);


create table question_response
(
  question_response_id VARCHAR(200) NOT NULL,
  parent_id VARCHAR(200) NOT NULL,
  questionnaire_response_id VARCHAR(200) NOT NULL,
  ordinal INTEGER NOT NULL,
  linkId MEDIUMTEXT NULL,
  text MEDIUMTEXT NULL,

  PRIMARY KEY (questionnaire_response_id, question_response_id)
);

create table questionnaire_response_group
(
  questionnaire_response_group_id VARCHAR(200) NOT NULL,
  parent_id VARCHAR(200) NOT NULL,
  questionnaire_response_id VARCHAR(200) NOT NULL,
  ordinal INTEGER NOT NULL,
  linkId MEDIUMTEXT NULL,
  title MEDIUMTEXT NULL,
  text MEDIUMTEXT NULL,
  subject MEDIUMTEXT NULL,

  PRIMARY KEY (questionnaire_response_id, questionnaire_response_group_id)
);
