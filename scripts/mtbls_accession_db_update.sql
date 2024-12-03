alter table studies add reserved_accession varchar(50);
alter table studies add reserved_submission_id varchar(50);
alter table studies add first_public_date timestamp;

update studies set reserved_submission_id = 'REQ' || to_char(submissiondate, 'YYYYMMDD') || id where reserved_submission_id is null;
update studies set reserved_accession = acc where acc like 'MTBLS%';
update studies set first_public_date = releasedate where status = 3;

-- update studies set acc = reserved_submission_id where status = 0 or status > 3;

alter sequence hibernate_sequence cache 1;
alter sequence hibernate_sequence increment by 1;