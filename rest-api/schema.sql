CREATE TABLE participant
(
    id VARCHAR(200) NOT NULL,
    name VARCHAR(2048) NULL,
    address VARCHAR(2048) NULL,
    date_of_birth DATETIME(6) NULL,
    enrollment_status INT NULL,
    physical_exam_status INT NULL,
    PRIMARY KEY (id)
);
