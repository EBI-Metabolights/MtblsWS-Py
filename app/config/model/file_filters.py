
from pydantic import BaseModel


class FileFilters(BaseModel):
    compressed_files_list: list[str] = [
        ".7z",
        ".arj",
        ".bz2",
        ".g7z",
        ".gz",
        ".rar",
        ".tar",
        ".war",
        ".z",
        ".zip",
        ".zipx",
    ]

    derived_data_folder_list: list[str] = [
        "chebi_pipeline_annotations",
    ]

    derived_files_list: list[str] = [
        ".cdf",
        ".cef",
        ".cnx",
        ".dx",
        ".imzml",
        ".mgf",
        ".msp",
        ".mzdata",
        ".mzml",
        ".mzxml",
        ".nmrml",
        ".peakml",
        ".scan",
        ".smp",
        ".wiff",
        ".xlsx",
        ".xml",
        ".xy",
        ".jdx",
    ]

    empty_exclusion_list: list[str] = [
        "_chroms.inf",
        "format.temp",
        "metexplore_mapping.json",
        "msactualdefs.xml",
        "msmasscal.bin",
        "msprofile.bin",
        "prosol_history",
        "synchelper",
        "tcc_1.xml",
        "tempbase",
        "title",
    ]

    folder_exclusion_list: list[str] = [
        ".d",
        ".raw",
        "/audit",
        "/backup",
        "/chebi",
        "/chebi_pipeline_annotations",
        "/metaspace",
        "/old",
        "audit",
        "backup",
        "chebi",
        "chebi_pipeline_annotations",
        "metaspace",
        "old",
    ]

    ignore_file_list: list[str] = [
        "_chroms",
        "_extern",
        "_func",
        "_header",
        "_history",
        "_inlet",
        "acqmethod",
        "base_info",
        "binpump",
        "checksum.xml",
        "clevels",
        "defaultmasscal",
        "devices.xml",
        "format.temp",
        "fq1list",
        "gpnam",
        "info.xml",
        "isopump",
        "metexplore_mapping",
        "msactualdefs",
        "msmasscal",
        "msperiodicactuals",
        "msprofile",
        "msts.xml",
        "outd",
        "output",
        "pdata",
        "precom",
        "prosol_history",
        "pulseprogram",
        "scon2",
        "settings",
        "shimvalues",
        "specpar",
        "stanprogram",
        "synchelper",
        "tcc_1",
        "tdaspec",
        "tempbase",
        "title",
        "tofdataindex",
        "uxnmr",
        "validation_files",
    ]

    internal_mapping_list: list[str] = [
        "INTERNAL_FILES/logs",
    ]

    raw_files_list: list[str] = [
        ".abf",
        ".cdf",
        ".cdf.cmp",
        ".cmp",
        ".d",
        ".dat",
        ".hr",
        ".ibd",
        ".jpf",
        ".lcd",
        ".mgf",
        ".qgd",
        ".raw",
        ".scan",
        ".wiff",
        ".xps",
    ]

    rsync_exclude_list: list[str] = [
        "audit",
        "files-all.json",
        "metexplore_mapping.json",
        "missing_files.txt",
        "validation_files.json",
        "validation_report.json",
    ]
