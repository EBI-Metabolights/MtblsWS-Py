from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Numeric,
    Sequence,
    String,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()
metadata = Base.metadata

study_revisions_id_seq = Sequence("study_revisions_id_seq")


class StudyRevision(Base):
    __tablename__ = "study_revisions"

    id = Column(
        BigInteger,
        study_revisions_id_seq,
        server_default=study_revisions_id_seq.next_value(),
        primary_key=True,
    )
    accession_number = Column(String(255), nullable=False)
    revision_number = Column(BigInteger, nullable=False)
    revision_datetime = Column(DateTime, nullable=False)
    revision_comment = Column(String(1024), nullable=False)
    created_by = Column(String(255), nullable=False)
    status = Column(BigInteger, nullable=False, default=0)
    task_started_at = Column(DateTime, nullable=True)
    task_completed_at = Column(DateTime, nullable=True)
    task_message = Column(Text, nullable=True)
    mhd_share_status = Column(BigInteger, nullable=False, default=0)


class StudyTask(Base):
    __tablename__ = "study_tasks"

    id = Column(BigInteger, primary_key=True)
    study_acc = Column(String(255), nullable=False)
    task_name = Column(String(255), nullable=False)
    last_request_time = Column(DateTime, nullable=False)
    last_request_executed = Column(DateTime, nullable=False)
    last_execution_time = Column(String(255), nullable=False)
    last_execution_status = Column(String(255), nullable=False)
    last_execution_message = Column(Text)
    (UniqueConstraint("study_acc", "task_name"),)
    Index("ref_xref_acc_task", "study_acc", "task_name", unique=True)


class RegisteredUserView(Base):
    __tablename__ = "registered_users_view"

    id = Column(BigInteger, primary_key=True)
    apitoken = Column(String(255), unique=True)
    role = Column(BigInteger, nullable=False)
    status = Column(BigInteger, nullable=False)
    username = Column(String(255), unique=True)
    password = Column(String(255))


t_study_user = Table(
    "study_user",
    metadata,
    Column("userid", ForeignKey("users.id"), primary_key=True, nullable=False),
    Column("studyid", ForeignKey("studies.id"), primary_key=True, nullable=False),
)
hibernate_sequence = Sequence("hibernate_sequence", metadata=metadata)


class User(Base):
    __tablename__ = "users"

    id = Column(
        BigInteger,
        hibernate_sequence,
        primary_key=True,
        server_default=hibernate_sequence.next_value(),
    )
    address = Column(String(255))
    affiliation = Column(String(255))
    affiliationurl = Column(String(255))
    apitoken = Column(String(255), unique=True)
    email = Column(String(255))
    firstname = Column(String(255))
    joindate = Column(DateTime)
    lastname = Column(String(255))
    password = Column(String(255))
    role = Column(BigInteger, nullable=False)
    partner = Column(BigInteger, nullable=False, default=0)
    status = Column(BigInteger, nullable=False)
    username = Column(String(255), unique=True)
    orcid = Column(String(255))
    metaspace_api_key = Column(String(255))

    studies = relationship("Study", secondary="study_user", back_populates="users")


class StudySummaryView(Base):
    __tablename__ = "study_summary_view"

    id = Column(BigInteger, primary_key=True)
    acc = Column(String(255), unique=True)
    obfuscationcode = Column(String(255), unique=True)
    status = Column(BigInteger, nullable=False)


class Study(Base):
    __tablename__ = "studies"

    id = Column(BigInteger, primary_key=True)
    acc = Column(String(255), unique=True)
    obfuscationcode = Column(String(255), unique=True)
    releasedate = Column(DateTime, nullable=False)
    status = Column(BigInteger, nullable=False)
    studysize = Column(Numeric(38, 0))
    updatedate = Column(
        DateTime,
        nullable=False,
        server_default=text("('now'::text)::timestamp without time zone"),
    )
    submissiondate = Column(
        DateTime,
        nullable=False,
        server_default=text("('now'::text)::timestamp without time zone"),
    )
    validations = Column(Text)
    studytype = Column(String(1000))
    curator = Column(String)
    override = Column(String)
    species = Column(String)
    sample_rows = Column(BigInteger)
    assay_rows = Column(BigInteger)
    maf_rows = Column(BigInteger)
    biostudies_acc = Column(String)
    placeholder = Column(String)
    validation_status = Column(String)
    status_date = Column(DateTime)
    number_of_files = Column(BigInteger)
    comment = Column(String)
    curation_request = Column(BigInteger, nullable=False)
    reserved_accession = Column(Text, nullable=True)
    reserved_submission_id = Column(Text, nullable=True)
    first_public_date = Column(DateTime, nullable=True)
    first_private_date = Column(DateTime, nullable=True)
    dataset_license = Column(String)
    revision_number = Column(BigInteger, nullable=False, default=0)
    revision_datetime = Column(DateTime, nullable=True)
    sample_type = Column(String, default="minimum")
    data_policy_agreement = Column(BigInteger, nullable=False, default=0)
    study_category = Column(BigInteger, nullable=False, default=0)
    template_version = Column(String(50), nullable=False, default="2.0")
    mhd_accession = Column(String(50))
    mhd_model_version = Column(String(50))

    users = relationship("User", secondary="study_user", back_populates="studies")


class MetabolightsParameter(Base):
    __tablename__ = "metabolights_parameters"

    name = Column(String(512), primary_key=True)
    value = Column(String(4000), nullable=False)


class MlStat(Base):
    __tablename__ = "ml_stats"

    id = Column(BigInteger, primary_key=True)
    page_section = Column(String(20), nullable=False)
    str_name = Column(String(200), nullable=False)
    str_value = Column(String(200), nullable=False)
    sort_order = Column(BigInteger)


class RefAttribute(Base):
    __tablename__ = "ref_attribute"

    id = Column(BigInteger, primary_key=True)
    attribute_def_id = Column(ForeignKey("ref_attribute_def.id"), nullable=True)
    spectra_id = Column(ForeignKey("ref_met_spectra.id"), nullable=True)
    value = Column(String(4000))
    pathway_id = Column(ForeignKey("ref_met_pathways.id"), nullable=True)

    attribute_definition = relationship("RefAttributeDef")


class RefAttributeDef(Base):
    __tablename__ = "ref_attribute_def"

    id = Column(BigInteger, primary_key=True)
    name = Column(String(500))
    description = Column(String(500))


class RefDb(Base):
    __tablename__ = "ref_db"

    id = Column(Numeric(38, 0), primary_key=True)
    db_name = Column(String(50), nullable=False, unique=True)


class RefMetabolite(Base):
    __tablename__ = "ref_metabolite"

    id = Column(Numeric(38, 0), primary_key=True)
    acc = Column(String(2000), nullable=False, unique=True)
    name = Column(String(2000), nullable=False)
    description = Column(String(2000))
    inchi = Column(String(2000))
    temp_id = Column(String(20))
    created_date = Column(
        DateTime, server_default=text("('now'::text)::timestamp without time zone")
    )
    updated_date = Column(
        DateTime, server_default=text("('now'::text)::timestamp without time zone")
    )
    iupac_names = Column(String(2000), comment="Iupac names separated by pipe: |")
    formula = Column(String(100))
    has_species = Column(BigInteger, server_default=text("0"))
    has_pathways = Column(BigInteger, server_default=text("0"))
    has_reactions = Column(BigInteger, server_default=text("0"))
    has_nmr = Column(BigInteger, server_default=text("0"))
    has_ms = Column(BigInteger, server_default=text("0"))
    has_literature = Column(BigInteger, server_default=text("0"))
    inchikey = Column(String(2000))
    met_species_index = relationship("RefMetSpecies", viewonly=True)
    met_species = relationship(
        "RefSpecy", secondary="ref_met_to_species", overlaps="met_species_index"
    )
    ref_xref = relationship(
        "RefXref", secondary="ref_met_to_species", overlaps="met_species"
    )

    met_spectras = relationship("RefMetSpectra", backref="met")
    met_pathways = relationship("RefMetPathway", backref="met")


class RefMetSpecies(Base):
    __tablename__ = "ref_met_to_species"

    id = Column(BigInteger, primary_key=True)
    met_id = Column(ForeignKey("ref_metabolite.id", ondelete="CASCADE"), nullable=False)
    species_id = Column(
        ForeignKey("ref_species.id", ondelete="CASCADE"), nullable=False
    )
    ref_xref_id = Column(ForeignKey("ref_xref.id", ondelete="CASCADE"), nullable=False)

    species = relationship(
        "RefSpecy", backref="met_species", overlaps="met_species", viewonly=True
    )
    cross_reference = relationship("RefXref", overlaps="ref_xref")


class RefSpeciesGroup(Base):
    __tablename__ = "ref_species_group"

    name = Column(String(512), nullable=False, unique=True)
    id = Column(BigInteger, primary_key=True)
    parent_id = Column(ForeignKey("ref_species_group.id"))

    parent = relationship("RefSpeciesGroup", remote_side=[id])


class Stableid(Base):
    __tablename__ = "stableid"

    id = Column(BigInteger, primary_key=True)
    prefix = Column(String(255))
    seq = Column(BigInteger)


t_studies_temp = Table(
    "studies_temp",
    metadata,
    Column("dates", Date),
    Column("studies_created", BigInteger),
    Column("public_studies", BigInteger),
    Column("private_studies", BigInteger),
    Column("inreview", BigInteger),
    Column("curation", BigInteger),
    Column("users", BigInteger),
)

t_studies_temp_can_be_deleted = Table(
    "studies_temp_can_be_deleted",
    metadata,
    Column("id", BigInteger),
    Column("acc", String(255)),
    Column("obfuscationcode", String(255)),
    Column("releasedate", DateTime),
    Column("status", BigInteger),
    Column("studysize", Numeric(38, 0)),
    Column("updatedate", DateTime),
    Column("submissiondate", DateTime),
    Column("validations", Text),
    Column("studytype", String(1000)),
    Column("curator", String),
    Column("override", String),
    Column("species", String),
    Column("sample_rows", BigInteger),
    Column("assay_rows", BigInteger),
    Column("maf_rows", BigInteger),
    Column("biostudies_acc", String),
    Column("placeholder", String),
    Column("validation_status", String),
    Column("status_date", DateTime),
)

t_study_file_info = Table(
    "study_file_info",
    metadata,
    Column("file_size", String),
    Column("file_name", String),
    Column("file_type", String),
)


class RefMetSpectra(Base):
    __tablename__ = "ref_met_spectra"

    id = Column(BigInteger, primary_key=True)
    name = Column(String(2000), nullable=False)
    path_to_json = Column(String(150), nullable=False, unique=True)
    spectra_type = Column(String(10), nullable=False)
    met_id = Column(ForeignKey("ref_metabolite.id", ondelete="CASCADE"), nullable=False)
    attributes = relationship("RefAttribute", backref="spectra")
    # met = relationship('RefMetabolite')


class RefSpeciesMember(Base):
    __tablename__ = "ref_species_members"

    id = Column(BigInteger, primary_key=True)
    group_id = Column(ForeignKey("ref_species_group.id"), nullable=False)
    parent_member_id = Column(BigInteger)
    taxon = Column(String(512), nullable=False, unique=True)
    taxon_desc = Column(String(512))

    group = relationship("RefSpeciesGroup")


class RefXref(Base):
    __tablename__ = "ref_xref"

    id = Column(BigInteger, primary_key=True)
    acc = Column(String(512), nullable=False)
    db_id = Column(ForeignKey("ref_db.id"), nullable=False)

    db = relationship("RefDb")


class RefSpecy(Base):
    __tablename__ = "ref_species"

    id = Column(BigInteger, primary_key=True)
    species = Column(String(512), nullable=False)
    description = Column(String(4000))
    taxon = Column(String(512), unique=True)
    final_id = Column(ForeignKey("ref_species.id"), index=True)
    species_member = Column(ForeignKey("ref_species_members.id"))

    ref_species_member = relationship("RefSpeciesMember")


class RefMetPathway(Base):
    __tablename__ = "ref_met_pathways"

    id = Column(BigInteger, primary_key=True)
    pathway_db_id = Column(ForeignKey("ref_db.id"), nullable=False)
    met_id = Column(ForeignKey("ref_metabolite.id", ondelete="CASCADE"), nullable=False)
    name = Column(String(512), nullable=False)
    path_to_pathway_file = Column(String(4000))
    species_id = Column(ForeignKey("ref_species.id"), nullable=False)
    attributes = relationship("RefAttribute", backref="pathway")
    # met = relationship('RefMetabolite')
    database = relationship("RefDb")
    species = relationship("RefSpecy")


t_curation_log_temp = Table(
    "curation_log_temp",
    metadata,
    Column("acc", String(255)),
    Column("studysize", BigInteger),
    Column("status", Text),
    Column("releasedate", Text),
    Column("submissiondate", Text),
    Column("username", Text),
    Column("studytype", String(1000)),
    Column("acc_short", Text),
    Column("studyid", BigInteger),
    Column("updatedate", Text),
    Column("nmr_size", BigInteger),
    Column("ms_size", BigInteger),
    Column("relmonth", Text),
    Column("submonth", Text),
    Column("curator", String),
    Column("override", String),
    Column("species", String),
    Column("sample_rows", BigInteger),
    Column("assay_rows", BigInteger),
    Column("maf_rows", BigInteger),
    Column("biostudies_acc", String),
    Column("placeholder", String),
    Column("country", Text),
    Column("validation_status", Text),
    Column("status_date", DateTime),
    Column("maf_known", Text),
    Column("number_of_files", BigInteger),
)

t_ebi_reporting = Table(
    "ebi_reporting",
    metadata,
    Column("ms_nmr", String(3)),
    Column("submonth", String(6)),
    Column("size", String(100)),
)

t_maf_info = Table(
    "maf_info",
    metadata,
    Column("acc", String),
    Column("database_identifier", String),
    Column("metabolite_identification", String),
    Column("database_found", String),
    Column("metabolite_found", String),
)
