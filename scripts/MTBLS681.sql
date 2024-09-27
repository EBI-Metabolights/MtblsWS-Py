alter table studies add reserved_accession varchar(50);
alter table studies add reserved_submission_id varchar(50);
update studies set reserved_accession = accession, reserved_submission_id = "REQ" || to_char(submissiondate, 'YYYYMMDD') || id ;
update studies set acc = reserved_submission_id where status = 0 or status > 3;

alter sequence hibernate_sequence cache 1;
alter sequence hibernate_sequence increment by 1;