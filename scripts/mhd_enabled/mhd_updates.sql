ALTER TABLE studies ADD study_category bigint NOT NULL DEFAULT 0;
ALTER TABLE studies ADD template_version varchar(50) NOT NULL DEFAULT '1.0';
ALTER TABLE studies ADD mhd_accession varchar(50);
ALTER TABLE studies ADD mhd_model_version varchar(50);
ALTER TABLE study_revisions ADD mhd_share_status bigint NOT NULL DEFAULT 0;

ALTER TABLE studies ALTER COLUMN template_version SET DEFAULT '2.0';
