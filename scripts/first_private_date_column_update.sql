-- add first_private_date column in studies table to track first validated and private (in curation) date of a study.
-- keep its value null for new (submitted) and  dormant studies.

alter table studies add first_private_date timestamp;

-- update first_private_date values from status date for in curation studies;
update studies set first_private_date=status_date where status = 1;


-- update first_private_date values from submission date for in review - no way to know when it was first private (in curation)
update studies set first_private_date=submissiondate where status = 2;

-- update first_private_date values from submission date for public studies - no way to know when it was first private (in curation)
update studies set first_private_date=submissiondate where status = 3;
