from app.config import get_settings


def get_private_ftp_relative_root_path():
    settings = get_settings()
    if settings.ftp_server.private.configuration.mount_type.lower() == "mounted":
        full_path = settings.study.mounted_paths.private_ftp_root_path
        user_home = settings.study.private_ftp_user_home_path
    else:
        full_path = (
            settings.hpc_cluster.datamover.mounted_paths.cluster_private_ftp_root_path
        )
        user_home = settings.hpc_cluster.datamover.cluster_private_ftp_user_home_path

    return full_path.replace(user_home, "", 1)


def get_host_internal_url():
    service_settings = get_settings().server.service
    return f"http://{service_settings.mtbls_ws_host}:{service_settings.mtbls_ws_host}"
