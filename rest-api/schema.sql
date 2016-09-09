DROP TABLE participant;
DROP TABLE evaluation;
DROP TABLE question;
DROP TABLE questionnaire_group;
DROP TABLE questionnaire;


CREATE TABLE participant
(
    participant_id VARCHAR(200) NOT NULL,
    name VARCHAR(2048) NULL,
    address VARCHAR(2048) NULL,
    date_of_birth DATETIME(6) NULL,
    enrollment_status INT NULL,
    physical_exam_status INT NULL,

    PRIMARY KEY (participant_id)
);

CREATE TABLE evaluation
(
    evaluation_id VARCHAR(200) NOT NULL,
    participant_id VARCHAR(200) NOT NULL,
    completed DATETIME(6) NULL,
    evaluation_version VARCHAR(100) NULL,
    evaluation_data MEDIUMTEXT NULL,

    PRIMARY KEY (participant_id, evaluation_id)
);

create table question
(
    question_id VARCHAR(200) NOT NULL,
    questionnaire_id VARCHAR(200) NOT NULL,
    linkId MEDIUMTEXT NULL,
    concept MEDIUMTEXT NULL,
    text MEDIUMTEXT NULL,
    type MEDIUMTEXT NULL,
    required BOOLEAN NULL,
    repeats BOOLEAN NULL,
    options MEDIUMTEXT NULL,
    option_col MEDIUMTEXT NULL,

    PRIMARY KEY (questionnaire_id, question_id)
);

create table questionnaire_group
(
    questionnaire_group_id VARCHAR(200) NOT NULL,
    questionnaire_id VARCHAR(200) NOT NULL,
    linkId MEDIUMTEXT NULL,
    concept MEDIUMTEXT NULL,
    text MEDIUMTEXT NULL,
    type MEDIUMTEXT NULL,
    required BOOLEAN NULL,
    repeats BOOLEAN NULL,

    PRIMARY KEY (questionnaire_id, questionnaire_group_id)
);


create table questionnaire
(
    id VARCHAR(200) NOT NULL,
    identifier MEDIUMTEXT NULL,
    version MEDIUMTEXT NULL,
    status MEDIUMTEXT NULL,
    date MEDIUMTEXT NULL,
    publisher MEDIUMTEXT NULL,
    telecom MEDIUMTEXT NULL,
    subjectType MEDIUMTEXT NULL,

    PRIMARY KEY (id)
);
