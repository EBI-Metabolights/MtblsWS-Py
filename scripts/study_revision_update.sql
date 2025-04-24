create table study_revisions
(
    id                      bigint          not null primary key,
    accession_number        varchar(255)    not null,
    revision_number         bigint          not null,
    revision_datetime       timestamp       not null,
    revision_comment        varchar(1024)   not null,
    created_by              varchar(255)    not null,
    status                  bigint          not null default 0,
    task_started_at         timestamp,
    task_completed_at       timestamp,
    task_message            text,
    unique (accession_number, revision_number)
);
alter table study_revisions owner to isatab;

create sequence study_revisions_id_seq;
alter sequence study_revisions_id_seq owner to isatab;

grant select, update, usage on sequence study_revisions_id_seq to wpk8smtblsrw;
grant delete, insert, references, select, trigger, truncate, update on study_revisions to wpk8smtblsrw;



alter table studies add revision_number bigint default 0;
update studies set revision_number = 1 where status = 3;
alter table studies alter column revision_number set not null;

alter table studies add revision_datetime timestamp;