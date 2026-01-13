CREATE TABLE IF NOT EXISTS study_tasks (
    id serial PRIMARY KEY,
    study_acc VARCHAR ( 255 ) NOT NULL,
    task_name VARCHAR ( 255 ) NOT NULL,
    last_request_time TIMESTAMP NOT NULL,
    last_request_executed TIMESTAMP NOT NULL,
    last_execution_time TIMESTAMP NOT NULL,
    last_execution_status VARCHAR ( 255 ) NOT NULL,
    last_execution_message text,
    UNIQUE (study_acc, task_name)
);
