import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Callable, Dict

import pandas as pd
import requests

from app.config import get_settings
from app.tasks.worker import MetabolightsTask, celery, send_email
from app.utils import current_time
from app.ws.chebi.search.chebi_search_manager import ChebiSearchManager
from app.ws.chebi.search.curated_metabolite_table import CuratedMetaboliteTable
from app.ws.chebi.wsproxy import get_chebi_ws_proxy
from app.ws.chebi_pipeline_utils import clean_comp_name, run_chebi_pipeline
from app.ws.mtblsWSclient import WsClient
from app.ws.redis.redis import get_redis_server
from app.ws.utils import read_tsv, write_tsv

logger = logging.getLogger(__name__)


def init_chebi_search_manager():
    settings = get_settings()
    # These code completes WsClient initialization using flask app context
    if not WsClient.default_search_manager:
        chebi_proxy = get_chebi_ws_proxy()
        curation_table_file_path = (
            settings.chebi.pipeline.assigned_metabolite_list_file_location
        )
        curation_table = CuratedMetaboliteTable.get_instance(curation_table_file_path)
        chebi_search_manager = ChebiSearchManager(
            ws_proxy=chebi_proxy, assigned_metabolite_table=curation_table
        )
        WsClient.default_search_manager = chebi_search_manager


@celery.task(
    bind=True,
    base=MetabolightsTask,
    max_retries=3,
    soft_time_limit=60 * 60 * 24,
    name="app.tasks.common_tasks.curation_tasks.chebi_pipeline.maf_post_curation_task",
)
def maf_post_curation_task(
    self,
    study_id: str,
    chebi_identifier_search: bool = True,
    refmet_identifier_search: bool = True,
):
    settings = get_settings()
    study_metadata_location = os.path.join(
        settings.study.mounted_paths.study_metadata_files_root_path, study_id
    )
    results = []
    current_task_time = current_time(True).strftime("%Y-%m-%d %H:%M:%S")
    for maf_file in Path(study_metadata_location).glob(
        "*_maf.tsv", case_sensitive=False
    ):
        maf_df = None

        try:
            maf_df = read_tsv(str(maf_file))
        except Exception as ex:
            message = f"Error while reading {maf_file.name} MAF file: {ex}"
            logger.error(message)
            results.append(message)
            continue
        last_default_column = "smallmolecule_abundance_std_error_sub"
        if last_default_column not in maf_df.columns:
            message = (
                f"{last_default_column} column is missing in {maf_file.name} MAF file"
            )
            logger.error(message)
            results.append(message)
            continue

        if "metabolite_identification" not in maf_df.columns:
            message = f"metabolite_identification column is missing in {maf_file.name} MAF file"
            logger.error(message)
            results.append(message)
            continue
        if len(maf_df) == 0:
            message = f"{maf_file.name} MAF file is empty"
            logger.error(message)
            results.append(message)
            continue
        assigned_chebi_identifier = "assigned_chebi_identifier"
        assigned_refmet_identifier = "assigned_refmet_identifier"
        chebi_identifier_search_status = "chebi_identifier_search_status"
        chebi_identifier_search_time = "chebi_identifier_search_time"
        refmet_identifier_search_status = "refmet_identifier_search_status"
        refmet_identifier_search_time = "refmet_identifier_search_time"

        last_column = last_default_column
        for column in [
            assigned_chebi_identifier,
            assigned_refmet_identifier,
            chebi_identifier_search_status,
            chebi_identifier_search_time,
            refmet_identifier_search_status,
            refmet_identifier_search_time,
        ]:
            if column not in maf_df.columns:
                idx = maf_df.columns.get_loc(last_column) + 1
                maf_df.insert(idx, column, "")
                last_column = column

        for index, row in maf_df.iterrows():
            logger.debug("Row: %s", index)
            if chebi_identifier_search:
                search_compound_identifier(
                    search_chebi_identifier,
                    current_task_time,
                    maf_df,
                    index,
                    assigned_chebi_identifier,
                    chebi_identifier_search_status,
                    chebi_identifier_search_time,
                )
            if refmet_identifier_search:
                search_compound_identifier(
                    search_refmet_identifier,
                    current_task_time,
                    maf_df,
                    index,
                    assigned_refmet_identifier,
                    refmet_identifier_search_status,
                    refmet_identifier_search_time,
                )

        write_tsv(maf_df, str(maf_file))


def search_compound_identifier(
    fn: Callable,
    current_task_time: str,
    maf_df: pd.DataFrame,
    row_index: int,
    assigned_identifier_column: str,
    identifier_search_status_column: str,
    identifier_search_time_column: str,
):
    value: str = maf_df.loc[row_index]["metabolite_identification"] or ""
    if not value:
        return True, ""
    values = value.split("|")
    assigned_values = []
    failed = False
    for identifier in values:
        success, ref_id = fn(identifier)
        if not success:
            failed = True
            break
        if ref_id:
            assigned_values.append(ref_id)
        else:
            assigned_values.append("")
    maf_df.at[row_index, identifier_search_time_column] = current_task_time
    if failed:
        maf_df.at[row_index, identifier_search_status_column] = "failed"
        return
    current_value = maf_df.at[row_index, assigned_identifier_column]

    new_value = "|".join(assigned_values)
    if not current_value:
        if not new_value:
            maf_df.at[row_index, identifier_search_status_column] = "not found"
        else:
            maf_df.at[row_index, identifier_search_status_column] = "first assignment"
    else:
        if not new_value:
            maf_df.at[row_index, identifier_search_status_column] = "deleted"
        elif current_value != new_value:
            maf_df.at[row_index, identifier_search_status_column] = "updated"
        else:
            maf_df.at[row_index, identifier_search_status_column] = "same"
    maf_df.at[row_index, assigned_identifier_column] = new_value


def search_chebi_identifier(search_term):
    search_term = clean_compound_name(search_term)

    if not search_term:
        return ""
    chebi_ws2_url = get_settings().chebi.service.connection.chebi_ws_wsdl
    chebi_es_search_url = f"{chebi_ws2_url}/public/es_search"
    params = {"term": search_term, "page": 1, "size": 5}

    chebi_id = ""
    success = False
    try:
        logger.debug(f"-- Search chebi id {search_term}")
        resp = requests.get(chebi_es_search_url, params=params, timeout=5)
        if resp.status_code == 200:
            json_resp = resp.json()
            results = json_resp["results"]

            if results:
                for result in results:
                    source = result["_source"]
                    name = source["name"]
                    if name.lower() == search_term:
                        chebi_id = source["chebi_accession"]
                        break
            success = True
    except Exception as e:
        logger.error(" -- Error querying ChEBI ws2. Error " + str(e), mode="error")

    return success, chebi_id


def search_refmet_identifier(search_term):
    search_term = clean_compound_name(search_term)

    if not search_term:
        return ""
    search_url = (
        f"https://www.metabolomicsworkbench.org/rest/refmet/match/{search_term}"
    )

    refmet_id = ""
    success = False
    try:
        logger.debug(f"-- Search refmet id {search_term}")
        resp = requests.get(search_url, timeout=5)
        if resp.status_code == 200:
            json_resp = resp.json()
            refmet_id = json_resp["refmet_id"] or ""
            if refmet_id and refmet_id.replace("-", "").strip():
                refmet_id = refmet_id
            success = True
            return success, refmet_id
    except Exception as e:
        logger.error(" -- Error querying REFMET ID. Error " + str(e), mode="error")

    return success, refmet_id


def clean_compound_name(input_str: str):
    if not input_str:
        return ""
    comp_name = input_str.strip().lower()

    comp_name = comp_name.replace("Î´", "delta").replace("?", "").replace("*", "")
    if "[" in comp_name:
        comp_name = comp_name.replace("[U]", "").replace("[S]", "")
        comp_name = re.sub(re.escape(r"[iso\d]"), "", comp_name)

    comp_name = clean_comp_name(comp_name)
    return comp_name


if __name__ == "__main__":
    from app.tasks.utils import set_basic_logging_config

    set_basic_logging_config(logging.DEBUG)
    init_chebi_search_manager()
    maf_post_curation_task("MTBLS1")


@celery.task(
    bind=True,
    base=MetabolightsTask,
    max_retries=1,
    soft_time_limit=60 * 60 * 24,
    name="app.tasks.common_tasks.curation_tasks.chebi_pipeline.run_chebi_pipeline_task",
)
def run_chebi_pipeline_task(
    self,
    study_id: str,
    user_token: str,
    annotation_file_name: str,
    email: str,
    classyfire_search: bool = True,
    update_study_maf: bool = False,
):
    output: Dict[str, Any] = {}
    start = current_time()
    status = "initiated"
    output["study_id"] = study_id
    output["input_maf_file"] = annotation_file_name
    output["start_time"] = start.strftime("%Y-%m-%d %H:%M:%S")
    output["executed_on"] = os.uname().nodename
    output["task_id"] = str(self.request.id)
    output["status"] = status
    task_name = f"CHEBI pipeline task for {study_id}: {self.request.id}"
    try:
        key = f"chebi_pipeline:{study_id}"
        get_redis_server().set_value(key, self.request.id)
        body_intro = f"CHEBI pipeline task is started for {study_id} {annotation_file_name} file. <p>"
        body = body_intro + json.dumps(output, indent=4)
        output["start_time"] = (start.strftime("%Y-%m-%d %H:%M:%S"),)
        output["executed_on"] = (os.uname().nodename,)
        send_email(
            f"CHEBI pipeline task is started for study {study_id}.",
            body,
            None,
            email,
            None,
        )
        init_chebi_search_manager()
        output["result"] = run_chebi_pipeline(
            study_id,
            user_token,
            annotation_file_name,
            run_silently=False,
            classyfire_search=classyfire_search,
            update_study_maf=update_study_maf,
            run_on_cluster=False,
            email=email,
            task_name=task_name,
        )
        status = "success"
    except Exception as ex:
        status = "failed"
        output["Failure reason"] = f"{str(ex)}"
        raise ex
    finally:
        end = current_time()
        time_difference = end - start
        hours, remainder = divmod(time_difference.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        time_format = "{:02}:{:02}:{:02} [HH:MM:ss]".format(
            int(hours), int(minutes), int(seconds)
        )
        output["elapsed_time"] = time_format
        output["end_time"] = (end.strftime("%Y-%m-%d %H:%M:%S"),)
        output["status"] = status

        body_intro = f"You can see the result of your CHEBI pipeline task {str(self.request.id)}.<p>"
        result_str = body_intro + json.dumps(output, indent=4)
        result_str = result_str.replace("\n", "<p>")

        get_redis_server().delete_value(key=key)

        send_email(
            f"Result of the CHEBI pipeline task - {study_id}",
            result_str,
            None,
            email,
            None,
        )
    return output
