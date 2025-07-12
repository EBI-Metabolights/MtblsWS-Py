import logging

from app.config import get_settings

from app.utils import MetabolightsException, current_time
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import StudyTask
from app.ws.db.types import StudyStatus, StudyTaskName, StudyTaskStatus
from app.ws.db.wrappers import update_study_model_from_directory
from app.ws.study.study_service import StudyService
from app.ws.study.user_service import UserService
from atproto import Client, client_utils
from app.tasks.worker import MetabolightsTask, celery


logger = logging.getLogger("wslog")


@celery.task(
    base=MetabolightsTask,
    bind=True,
    name="app.tasks.common_tasks.basic_tasks.bluesky.create_bluesky_post_for_public_study",
    default_retry_delay=10,
    max_retries=3,
)
def create_bluesky_post_for_public_study(self, user_token, study_id):
    UserService.get_instance().validate_user_has_curator_role(user_token)

    study_folders = get_settings().study.mounted_paths.study_metadata_files_root_path
    try:
        m_study = StudyService.get_instance().get_public_study_from_db(
            study_id=study_id
        )
        if StudyStatus.from_name(m_study.studyStatus) != StudyStatus.PUBLIC:
            raise MetabolightsException("Study is not public")
        update_study_model_from_directory(
            m_study, study_folders, title_and_description_only=True
        )
    except Exception as e:
        logger.error(f"Error while reading title of study for {study_id}")
        raise e
    if not m_study.title:
        logger.error(f"Title is not read or valid for {study_id}")
        raise MetabolightsException("Study title is not found")
    url = "https://www.ebi.ac.uk/metabolights"

    # bluesky_message_lines = []
    bluesky_connection = get_settings().bluesky.connection
    # total_length = sum([len(x) for x in bluesky_message_lines])
    trimmed_title_length = 150
    short_title = (
        m_study.title
        if len(m_study.title) <= trimmed_title_length
        else m_study.title[:trimmed_title_length] + "..."
    )
    title_message = f"🧪 {study_id}: {short_title}"

    text_builder = client_utils.TextBuilder()
    text_builder.text("📢 New Public on MetaboLights!\n\n")

    text_builder.text(f"{title_message}\n🔗 ")
    text_builder.link(f"{url}/{study_id}", f"{url}/{study_id}")
    text_builder.text("\n\n")
    text_builder.tag("#MetaboLights", "MetaboLights")
    text_builder.text(" ")
    text_builder.tag(f"#{study_id}", study_id)
    text_builder.text(" ")
    text_builder.tag("#Metabolomics", "Metabolomics")
    text_builder.text(" ")
    text_builder.tag("#OpenScience", "OpenScience")

    try:
        client = Client()
        text = text_builder.build_text()
        facets = text_builder.build_facets()
        client.login(bluesky_connection.handle, bluesky_connection.app_password)

        task_name = StudyTaskName.SEND_SOCIAL_POST
        tasks = StudyService.get_instance().get_study_tasks(
            study_id=study_id, task_name=task_name.value
        )
        post = None
        with DBManager.get_instance().session_maker() as db_session:
            try:
                if tasks:
                    task = tasks[0]
                else:
                    now = current_time()
                    task = StudyTask()
                    task.study_acc = study_id
                    task.task_name = task_name
                    task.last_request_time = now
                    task.last_execution_time = now
                    task.last_request_executed = now
                    task.last_execution_status = StudyTaskStatus.NOT_EXECUTED
                    task.last_execution_message = (
                        "Task is initiated to create a social post."
                    )
                if (
                    task.last_execution_status
                    == StudyTaskStatus.EXECUTION_SUCCESSFUL.value
                ):
                    logger.error(f"Post is already created for {study_id}")
                    raise MetabolightsException(
                        f"Post is already created for {study_id}"
                    )
                task.last_execution_status = StudyTaskStatus.EXECUTING
                task.last_execution_time = now
                db_session.add(task)
                db_session.commit()
                logger.info(f"Creating social post for {study_id}")
                post = client.send_post(text=text, facets=facets)
                parts = post.uri.replace("at://", "").split("/")
                post_id = parts[-1]
                handle = bluesky_connection.handle
                post_url = f"https://bsky.app/profile/{handle}/post/{post_id}"
                task.last_execution_status = StudyTaskStatus.EXECUTION_SUCCESSFUL
                task.last_execution_time = task.last_request_time
                task.last_execution_message = post_url

                db_session.add(task)
                db_session.commit()
            except Exception as e:
                db_session.rollback()
                raise e
        return {"status": "success", "uri": post.uri, "cid": post.cid, "url": post_url}
    except Exception as e:
        return {"status": "error", "message": str(e)}
