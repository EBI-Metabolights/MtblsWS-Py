import datetime
import logging
from typing import Annotated, Literal

import globus_sdk
from pydantic import BaseModel, Field

from app.config.model.globus import GlobusConfiguration

logger = logging.getLogger(__name__)


class AclRule(BaseModel):
    data_type: Annotated[str, Field(alias="DATA_TYPE")]
    create_time: None | str = None
    expiration_date: None | str = None
    id_: Annotated[str, Field(alias="id")]
    path: str
    permissions: str
    principal: None | str = None
    principal_type: None | str = None
    role_id: None | str = None
    role_type: None | str = None
    file_manager_url: None | str = None
    collection_name: None | str = None


class Identity(BaseModel):
    email: None | str = None
    name: None | str = None
    organization: None | str = None
    status: None | str = None
    username: None | str = None
    identity_type: None | str = None
    identity_provider: None | str = None
    id_: Annotated[str, Field(alias="id")]


class GlobusPermission(BaseModel):
    permissions: list[AclRule]
    identity: Identity


class GlobusIdentityError(Exception): ...


class GlobusClient:
    TRANSFER_SCOPE = "urn:globus:auth:scope:transfer.api.globus.org:all"
    AUTH_SCOPE = "urn:globus:auth:scope:auth.globus.org:view_identities"

    def __init__(
        self,
        collection_id: str,
        client_id: str,
        client_secret: str,
        config: GlobusConfiguration,
    ):
        self.collection_id = collection_id
        self.client_id = client_id
        self.client_secret = client_secret
        self._transfer_client: None | globus_sdk.TransferClient = None
        self._auth_client: None | globus_sdk.AuthClient = None
        self._app: None | globus_sdk.ConfidentialAppAuthClient = None
        self._transfer_authorizer: None | globus_sdk.ClientCredentialsAuthorizer = None
        self._auth_authorizer: None | globus_sdk.ClientCredentialsAuthorizer = None
        self.config = config
        self.collection_name: str = ""

    @property
    def app(self) -> globus_sdk.ConfidentialAppAuthClient:
        if self._app:
            return self._app
        self._app = globus_sdk.ConfidentialAppAuthClient(
            self.client_id, self.client_secret
        )
        return self._app

    @property
    def transfer_authorizer(self) -> globus_sdk.ClientCredentialsAuthorizer:
        if self._transfer_authorizer:
            return self._transfer_authorizer
        self._transfer_authorizer = globus_sdk.ClientCredentialsAuthorizer(
            self.app, self.TRANSFER_SCOPE
        )
        return self._transfer_authorizer

    @property
    def auth_authorizer(self) -> globus_sdk.ClientCredentialsAuthorizer:
        if self._auth_authorizer:
            return self._auth_authorizer
        self._auth_authorizer = globus_sdk.ClientCredentialsAuthorizer(
            self.app, self.AUTH_SCOPE
        )
        return self._auth_authorizer

    @property
    def transfer_client(self) -> globus_sdk.TransferClient:
        if self._transfer_client:
            return self._transfer_client
        self._transfer_client = globus_sdk.TransferClient(
            authorizer=self.transfer_authorizer
        )
        self.collection_name = self._transfer_client.get_endpoint(
            self.collection_id
        ).get("display_name", "")
        return self._transfer_client

    @property
    def auth_client(self) -> globus_sdk.AuthClient:
        if self._auth_client:
            return self._auth_client
        self._auth_client = globus_sdk.AuthClient(authorizer=self.auth_authorizer)
        return self._auth_client

    def normalize_folder_path(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        if not path.endswith("/"):
            path = path + "/"
        return path

    def find_matching_acl_rules(
        self, folder_path: str, identity_id: str
    ) -> list[AclRule]:
        matches = []
        for rule in self.transfer_client.endpoint_acl_list(self.collection_id):
            if (
                rule.get("principal_type") == "identity"
                and rule.get("principal") == identity_id
                and rule.get("path") == folder_path
            ):
                matches.append(self.convert_to_acl_rule(rule))
        return matches

    def convert_to_acl_rule(self, rule: dict):
        acl_rule = AclRule.model_validate(rule)
        acl_rule.collection_name = self.collection_name or ""
        acl_rule.file_manager_url = (
            f"{self.config.file_manager_url.rstrip('/')}?"
            + "&".join(
                [
                    f"origin_id={self.collection_id}",
                    f"origin_path={acl_rule.path}",
                ]
            )
        )
        return acl_rule

    def find_folder_acl_rules(self, folder_path: str) -> list[AclRule]:
        matches: list[AclRule] = []
        for rule in self.transfer_client.endpoint_acl_list(self.collection_id):
            if (
                rule.get("principal_type") == "identity"
                and rule.get("path") == folder_path
            ):
                matches.append(self.convert_to_acl_rule(rule))
        return matches

    def create_acl_rule(
        self,
        folder_path: str,
        identity_id: str,
        permissions: str,
        notify_email: str | None = None,
    ):
        data = {
            "DATA_TYPE": "access",
            "principal_type": "identity",
            "principal": identity_id,
            "path": folder_path,
            "permissions": permissions,
        }

        if notify_email:
            data["notify_email"] = notify_email
            data["notify_message"] = (
                f"You have been granted {permissions} access to {folder_path}"
            )

        res = self.transfer_client.add_endpoint_acl_rule(self.collection_id, data)
        return res["access_id"]

    def get_globus_identities_by_email(self, user_email: str) -> list[Identity]:
        identities = self.auth_client.get_identities(usernames=[user_email])
        return [Identity.model_validate(x) for x in identities]

    def get_globus_identities_by_id(self, id_: str) -> list[Identity]:
        identities = self.auth_client.get_identities(ids=[id_])
        return [Identity.model_validate(x) for x in identities]

    def update_folder_permission(
        self,
        folder_path: str,
        user_email: str,
        permission: Literal["r", "rw"] = "rw",
        notify_email: None | bool = None,
        force: None | bool = None,
    ) -> list[GlobusPermission]:
        folder_path = self.normalize_folder_path(folder_path)
        # Find rules for the same user + path
        result = self.auth_client.get_identities(usernames=[user_email])
        identities = [Identity.model_validate(x) for x in result]
        if not identities:
            raise GlobusIdentityError(f"{user_email} is not valid globus account")
        # final_permissions: dict[str, GlobusPermission] = {}
        permissions_map: dict[str, GlobusPermission] = {}
        for identity in identities:
            matching_rules = self.find_matching_acl_rules(
                folder_path=folder_path, identity_id=identity.id_
            )

            folder_path_msg = (
                folder_path[:25] + "..." if len(folder_path) > 25 else folder_path
            )
            permission_msg = "READ" if permission == "r" else "UPDATE"
            # Delete old rules if permission or expiration date is not same

            for acl_rule in matching_rules:
                expired = False
                if acl_rule.expiration_date:
                    try:
                        exp_date = datetime.datetime.fromisoformat(
                            acl_rule.expiration_date
                        )
                        if exp_date < datetime.datetime.now():
                            expired = True
                    except:
                        expired = True

                permission_update = permission != acl_rule.permissions

                if permission_update or expired or force:
                    access_id = acl_rule.id_
                    current_permission_msg = (
                        "READ" if acl_rule.permissions == "r" else "UPDATE"
                    )
                    self.transfer_client.delete_endpoint_acl_rule(
                        self.collection_id, access_id
                    )
                    logger.info(
                        f"Deleted Globus collection ACL rule: {access_id} "
                        f"(permissions={current_permission_msg}) "
                        f"(email={user_email}) "
                        f"(folder={folder_path_msg}) "
                    )
                else:
                    if identity.id_ not in permissions_map:
                        permissions_map[identity.id_] = GlobusPermission(
                            identity=identity, permissions=[]
                        )
                    permissions_map[identity.id_].permissions.append(acl_rule)
            current_permissions = permissions_map.get(identity.id_)
            if current_permissions and current_permissions.permissions:
                logger.info(
                    f"An Globus collection ACL rule "
                    f"({current_permissions.permissions[0].id_}) "
                    f"exists for {user_email} "
                    f"on {folder_path_msg} with {permission_msg} permission"
                )
                continue
            current_permissions = GlobusPermission(identity=identity, permissions=[])
            permissions_map[identity.id_] = current_permissions
            # Create the new rule
            rule_data = {
                "DATA_TYPE": "access",
                "principal_type": "identity",
                "principal": identity.id_,
                "path": folder_path,
                "permissions": permission,
            }

            if notify_email:
                rule_data["notify_email"] = user_email
                rule_data["notify_message"] = (
                    f"You have been granted {permission_msg} access to {folder_path}"
                )

            result = self.transfer_client.add_endpoint_acl_rule(
                self.collection_id, rule_data
            )
            new_access_id = result["access_id"]
            new_acl_rule = self.transfer_client.get_endpoint_acl_rule(
                self.collection_id, new_access_id
            )
            logger.info(
                f"Created new Globus collection ACL rule: {new_access_id} "
                f"(permissions={permission_msg}) "
                f"(email={user_email}) "
                f"(folder={folder_path_msg}) "
            )
            current_permissions.permissions.append(
                self.convert_to_acl_rule(new_acl_rule.data)
            )

        return list(permissions_map.values())

    def remove_folder_permission(
        self,
        folder_path: str,
        user_emails: None | str | list[str],
    ) -> list[AclRule]:
        if not folder_path:
            return []
        if not user_emails:
            user_emails = [None]
        if isinstance(user_emails, str):
            user_emails = user_emails.split(",")
        matched_permissions = self.get_folder_permissions(
            folder_path=folder_path, user_emails=user_emails
        )
        for match in matched_permissions:
            deleted_rules = []
            for acl_rule in match.permissions:
                access_id = acl_rule.id_
                current_permission_msg = (
                    "READ" if acl_rule.permissions == "r" else "UPDATE"
                )
                self.transfer_client.delete_endpoint_acl_rule(
                    self.collection_id, access_id
                )
                folder_path_msg = (
                    folder_path[:25] + "..." if len(folder_path) > 25 else folder_path
                )
                logger.info(
                    f"Deleted Globus collection ACL rule: {access_id} "
                    f"(permissions={current_permission_msg}) "
                    f"(email={match.identity.username}) "
                    f"(folder={folder_path_msg}) "
                )
                deleted_rules.append(acl_rule)

        return matched_permissions

    def get_folder_permissions(
        self,
        folder_path: str,
        user_emails: None | str | list[str],
    ) -> list[GlobusPermission]:
        matched_permissions: dict[str, GlobusPermission] = {}
        if not folder_path:
            return []
        folder_path = self.normalize_folder_path(folder_path)
        if isinstance(user_emails, str):
            user_emails = user_emails.split(",")

        if user_emails:
            result = self.auth_client.get_identities(usernames=user_emails)
            identities = [Identity.model_validate(x) for x in result]
            if not identities:
                return []
            for identity in identities:
                matching_rules = self.find_matching_acl_rules(
                    folder_path=folder_path, identity_id=identity.id_
                )
                matched_permissions[identity.id_] = GlobusPermission(
                    identity=identity, permissions=matching_rules
                )

        else:
            matching_rules = self.find_folder_acl_rules(folder_path=folder_path)

            for acl_rule in matching_rules:
                result = self.auth_client.get_identities(ids=[acl_rule.principal])
                identities = [Identity.model_validate(x) for x in result]

                identity = identities[0] if identities else None
                if identity:
                    if identity.id_ not in matched_permissions:
                        matched_permissions[identity.id_] = GlobusPermission(
                            identity=identity, permissions=[]
                        )
                    matched_permissions[identity.id_].permissions.append(acl_rule)

        return list(matched_permissions.values())
