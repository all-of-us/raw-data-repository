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
