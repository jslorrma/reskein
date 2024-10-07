#!/usr/bin/env python3
"""
yarn/models/submission.py
=========================

Implementation of YARN's `ApplicationSubmissionContext` message and its related
sub-messages used to submit an application via [YARN's ResourceManager
Applications API][1]. This context is used to provide the ResourceManager with
details about the application you're submitting.

[1]: https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html#Cluster_New_Application_API
"""

from __future__ import annotations

__author__ = "jslorrma"
__maintainer__ = "jslorrma"
__email__ = "jslorrma@gmail.com"

import re
from typing import TYPE_CHECKING, Annotated, Any, Literal

import msgspec

from . import ResourceManagerMessage

if TYPE_CHECKING:
    from skein import model as skein_model


def _todash(name: str):
    """Rename snake case spec attribute names to '-'-seperated"""
    return name.replace("_", "-")


class ResourceDetails(ResourceManagerMessage, rename=_todash, omit_defaults=True, kw_only=True):
    """
    ResourceDetails is a class that defines the specifications for a resource
    to be localized.

    Parameters
    ----------
    resource : str
        Location of the resource to be localized.
    type : Literal['ARCHIVE', 'FILE', 'PATTERN']
        Type of the resource; options are `'ARCHIVE'`, `'FILE'`, and `'PATTERN'`.
    visibility : Literal['PUBLIC', 'PRIVATE', 'APPLICATION']
        Visibility of the resource to be localized; options are `'PUBLIC'`,
        `'PRIVATE'`, and `'APPLICATION'`.
    size : int
        Size of the resource to be localized.
    timestamp : int
        Timestamp of the resource to be localized.
    """

    resource: str | msgspec.UnsetType = msgspec.UNSET
    type: Literal["ARCHIVE", "FILE", "PATTERN"]
    visibility: Literal["PUBLIC", "PRIVATE", "APPLICATION"]
    size: Annotated[int, msgspec.Meta(ge=0)]
    timestamp: Annotated[int, msgspec.Meta(ge=0)]

    _source: (
        Annotated[str, msgspec.Meta(min_length=1, pattern=r"^(file://|hdfs://).*$")] | None
    ) = msgspec.field(name="source", default=None)

    @ResourceManagerMessage.compute_field
    def resource_(self) -> str:
        return self._source


class LocalResource(ResourceManagerMessage, rename=_todash, omit_defaults=True):
    """
    LocalResource is a class that defines a local resource for the
    ResourceManager.

    The object is a collection of key-value pairs. The key is an
    identifier for the resources to be localized and the value is the
    details of the resource.

    Parameters
    ----------
    key : str
        Resource identifier. It should have a minimum length of `1`.
    value : ResourceDetails
        The details of the resource.
    """

    key: Annotated[str, msgspec.Meta(min_length=1)]
    value: ResourceDetails


class LocalResources(ResourceManagerMessage, rename=_todash, omit_defaults=True, kw_only=True):
    """
    LocalResources is a class that defines a collection of resources to be
    localized.

    Parameters
    ----------
    entry : List[LocalResource], optional
        A list of resources to be localized.
    """

    entry: list[LocalResource] | msgspec.UnsetType = msgspec.UNSET

    _files: dict[str, dict[str, Any]] | None = msgspec.field(name="files", default=None)

    @ResourceManagerMessage.compute_field
    def entry_(self) -> list[LocalResource]:
        return [
            msgspec.convert({"key": file, "value": details}, type=LocalResource)
            for file, details in self._files.items()
        ]


class Commands(ResourceManagerMessage, rename=_todash, omit_defaults=True, kw_only=True):
    """
    Commands is a class that defines the commands for launching your container.

    Parameters
    ----------
    command : List[str]
        The commands for launching your container, in the order in which they
        should be executed.
    """

    command: Annotated[str, msgspec.Meta(min_length=1)] | msgspec.UnsetType = msgspec.UNSET

    _command: str | list | None = msgspec.field(name="master_command", default=None)

    @ResourceManagerMessage.compute_field
    def command_(self) -> str:
        if isinstance(self._command, list | tuple | set):
            return "\n".join(self._command)
        else:
            return self._command

    @classmethod
    def create(cls, command: str | list[str]) -> Commands:
        return msgspec.convert({"master_command": command}, type=Commands)


class ApplicationACLs(ResourceManagerMessage, rename=_todash, omit_defaults=True, kw_only=True):
    """
    ApplicationACLs is a class that defines the ACLs for your application.

    Parameters
    ----------
    VIEW_APP : bool, optional
        The list of users and groups with `VIEW` permissions.
    MODIFY_APP : float, optional
        The list of users and groups with `MODIFY` permissions.
    """

    VIEW_APP: set[str] = msgspec.field(default_factory=set)
    MODIFY_APP: set[str] = msgspec.field(default_factory=set)

    _enable: bool | None = msgspec.field(name="enable", default=False)
    _view_users: list[str] | None = msgspec.field(name="view_users", default_factory=list)
    _view_groups: list[str] | None = msgspec.field(name="view_groups", default_factory=list)
    _modify_users: list[str] | None = msgspec.field(name="modify_users", default_factory=list)
    _modify_groups: list[str] | None = msgspec.field(name="modify_groups", default_factory=list)

    @ResourceManagerMessage.compute_field
    def VIEW_APP_(self) -> set[str]:
        return set(self._view_users + self._view_groups) if self._enable else set()

    @ResourceManagerMessage.compute_field
    def MODIFY_APP_(self) -> set[str]:
        return set(self._modify_users + self._modify_groups) if self._enable else set()


class EnvVar(ResourceManagerMessage, rename=_todash, omit_defaults=True):
    """
    EnvVar is a class that defines an environment variable entry.

    Parameters
    ----------
    key : str
        The environment variable name.
    value : str
        The environment variable value.
    """

    key: str
    value: str


class Environment(ResourceManagerMessage, rename=_todash, omit_defaults=True, kw_only=True):
    """ "
    Environment is a class that defines the environment variables for an
    application.

    Parameters
    ----------
    entry : list[EnvVar], optional
        A list of environment variable entries.
    """

    entry: list[EnvVar] | msgspec.UnsetType = msgspec.UNSET

    _env: dict[str, str] | None = msgspec.field(name="env", default=None)

    @ResourceManagerMessage.compute_field
    def entry_(self) -> list[EnvVar]:
        return [
            msgspec.convert({"key": file, "value": var}, type=EnvVar)
            for file, var in self._env.items()
        ]


class Credential(ResourceManagerMessage, rename=_todash, omit_defaults=True):
    """
    Credential is a class that defines a credential entry.

    Parameters
    ----------
    key : str
        An identifier for the token.
    value : str
        The actual token or the base-64 encoding of the secret.
    """

    key: str
    value: str


class CredentialEntries(ResourceManagerMessage, rename=_todash, omit_defaults=True, kw_only=True):
    """
    CredentialEntries is a class that defines a collection of credential entries.

    Parameters
    ----------
    entry : list[Credential], optional
        A list of credential entries.
    """

    entry: list[Credential] | msgspec.UnsetType = msgspec.UNSET

    _entries: dict[str, str] = msgspec.field(name="entries", default_factory=dict)

    @ResourceManagerMessage.compute_field
    def entry_(self) -> list[Credential]:
        return [
            msgspec.convert({"key": file, "value": credential}, type=Credential)
            for file, credential in self._entries.items()
        ]


class Credentials(ResourceManagerMessage, rename=_todash, omit_defaults=True):
    """
    Credentials is a class that defines the credentials for an application.

    The credentials object should be used to pass data required for the
    application to authenticate itself such as delegation-tokens and secrets.

    Parameters
    ----------
    tokens : CredentialEntries, optional
        Tokens that you wish to pass to your application, specified as key-value
        pairs. The key is an identifier for the token and the value is the
        token (which should be obtained using the respective web-services), by
        default `{}`
    secrets : CredentialEntries, optional
        Secrets that you wish to use in your application, specified as key-value
        pairs. They key is an identifier and the value is the base-64 encoding
        of the secret, by default `{}`
    """

    tokens: (
        CredentialEntries | msgspec.UnsetType
    ) = msgspec.UNSET  # will be converted to CredentialEntries
    secrets: (
        CredentialEntries | msgspec.UnsetType
    ) = msgspec.UNSET  # will be converted to CredentialEntries

    @classmethod
    def create(
        cls, tokens: dict[str, str] | None = None, secrets: dict[str, str] | None = None
    ) -> Credentials:
        return msgspec.convert(
            {"tokens": {"entries": tokens}}
            if tokens
            else {} | {"secrets": {"entries": secrets}}
            if secrets
            else {},
            type=Credentials,
        )


class AMContainer(ResourceManagerMessage, rename=_todash, omit_defaults=True, kw_only=True):
    """
    AMContainer is a class that provides the container launch context for the
    application master.

    Parameters
    ----------
    commands : Commands, optional
        The commands for launching your container.
    local_resources : LocalResources, optional
        Object describing the resources that need to be localized.
    application_acls : ApplicationACLs, optional
        ACLs for your application; the key can be `'VIEW_APP'` or `'MODIFY_APP'`,
        the value is the list of users with the permissions.
    environment : Environment, optional
        Environment variables for your containers, specified as key value pairs.
    credentials : Credentials, optional
        The credentials required for your application to run.
    """

    commands: Commands | msgspec.UnsetType = msgspec.UNSET
    local_resources: LocalResources | msgspec.UnsetType = msgspec.UNSET
    application_acls: ApplicationACLs | msgspec.UnsetType = msgspec.UNSET
    environment: Environment | msgspec.UnsetType = msgspec.UNSET
    credentials: Credentials | msgspec.UnsetType = msgspec.UNSET

    _master: dict[str, Any] = msgspec.field(name="master", default_factory=dict)
    _acls: dict[str, Any] = msgspec.field(name="acls", default_factory=dict)

    @ResourceManagerMessage.compute_field
    def local_resources_(self) -> LocalResources:
        return msgspec.convert(self._master, type=LocalResources)

    @ResourceManagerMessage.compute_field
    def application_acls_(self) -> ApplicationACLs:
        return msgspec.convert(self._acls, type=ApplicationACLs)

    @ResourceManagerMessage.compute_field
    def environment_(self) -> Environment:
        return msgspec.convert(self._master, type=Environment)

    def with_commands(self, command: str | list[str]) -> AMContainer:
        self.commands = Commands.create(command)
        return self

    def with_credentials(
        self,
        tokens: dict[str, str] | None = None,
        secrets: dict[str, str] | None = None,
    ) -> AMContainer:
        self.credentials = Credentials.create(tokens=tokens, secrets=secrets)
        return self

    @classmethod
    def from_skein(cls, spec: skein_model.ApplicationSpec) -> AMContainer:
        return msgspec.convert(spec.to_dict(), type=AMContainer)


class Resource(ResourceManagerMessage, rename=_todash, kw_only=True):
    """
    Resource is a class that defines the resources required for each container.

    Parameters
    ----------
    memory : int, optional
        Memory required for each container. Default is `512` (MiB).
    vCores : int, optional
        Virtual cores required for each container. Default is `1`.
    """

    memory: Annotated[int, msgspec.Meta(ge=1)] | None = None
    vCores: Annotated[int, msgspec.Meta(ge=1)] | None = None

    _resources: dict[str, Any] = msgspec.field(name="resources", default_factory=dict)

    @ResourceManagerMessage.compute_field
    def memory_(self) -> int:
        if self._resources:
            return self._resources.get("memory", 512)

    @ResourceManagerMessage.compute_field
    def vCores_(self) -> int:
        if self._resources:
            return self._resources.get("vcores", 1)

    @classmethod
    def from_skein(cls, spec: skein_model.ApplicationSpec) -> Resource:
        return msgspec.convert(spec.to_dict(), type=Resource)


class LogAggregationContext(ResourceManagerMessage, rename=_todash, omit_defaults=True):
    """
    LogAggregationContext is a class that represents all of the information
    needed by the NodeManager to handle
    the logs for this application.

    Parameters
    ----------
    log_include_pattern : str, optional
        The log files which match the defined regex include pattern will be
        uploaded when the application finishes. Default is `None`.
    log_exclude_pattern : str, optional
        The log files which match the defined regex exclude pattern will not be
        uploaded when the application finishes. Default is `None`.
    rolled_log_include_pattern : str, optional
        The log files which match the defined regex include pattern will be
        aggregated in a rolling fashion. Default is `None`.
    rolled_log_exclude_pattern : str, optional
        The log files which match the defined regex exclude pattern will not be
        aggregated in a rolling fashion. Default is `None`.
    log_aggregation_policy_class_name : str, optional
        The policy which will be used by NodeManager to aggregate the logs.
        Default is `None`.
    log_aggregation_policy_parameters : str, optional
        The parameters passed to the policy class. Default is `None`.
    """

    log_include_pattern: str | None = None
    log_exclude_pattern: str | None = None
    rolled_log_include_pattern: str | None = None
    rolled_log_exclude_pattern: str | None = None
    log_aggregation_policy_class_name: str | None = None
    log_aggregation_policy_parameters: str | None = None

    @classmethod
    def create(  # noqa: PLR0913
        cls,
        log_include_pattern: str | list[str] | None = None,
        log_exclude_pattern: str | list[str] | None = None,
        rolled_log_include_pattern: str | list[str] | None = None,
        rolled_log_exclude_pattern: str | list[str] | None = None,
        log_aggregation_policy_class_name: str | list[str] | None = None,
        log_aggregation_policy_parameters: str | list[str] | None = None,
    ) -> LogAggregationContext:
        return cls(
            log_include_pattern=(
                "|".join(log_include_pattern)
                if isinstance(log_include_pattern, list | tuple | set)
                else log_include_pattern
            ),
            log_exclude_pattern=(
                "|".join(log_exclude_pattern)
                if isinstance(log_exclude_pattern, list | tuple | set)
                else log_exclude_pattern
            ),
            rolled_log_include_pattern=(
                "|".join(rolled_log_include_pattern)
                if isinstance(rolled_log_include_pattern, list | tuple | set)
                else rolled_log_include_pattern
            ),
            rolled_log_exclude_pattern=(
                "|".join(rolled_log_exclude_pattern)
                if isinstance(rolled_log_exclude_pattern, list | tuple | set)
                else rolled_log_exclude_pattern
            ),
            log_aggregation_policy_class_name=log_aggregation_policy_class_name,
            log_aggregation_policy_parameters=log_aggregation_policy_parameters,
        )


class ApplicationTag(ResourceManagerMessage, rename=_todash, omit_defaults=True):
    """
    ApplicationTag is a class that represents a tag for an application.

    Parameters
    ----------
    tag : str, optional
        A tag string. Default is `None`.
    """

    tag: str | None = None


class ApplicationSubmissionContext(
    ResourceManagerMessage, rename=_todash, omit_defaults=True, kw_only=True
):
    """
    ApplicationSubmissionContext is a class that defines the context for
    submitting a YARN application.

    Note
    ----
    This model spec contains just the relevant attributes for skein application
    deployment.

    Parameters
    ----------
    application_id : str
        The application id
    application_name : str
        The application name
    queue : str
        The name of the queue to which the application should be submitted
    am_container_spec : AMContainer
        The application master container launch context
    resource: Resource, optional
        The resources the application master requires
    log_aggregation_context : LogAggregationContext, optional
        Represents all of the information needed by the NodeManager to handle
        the logs for this application
    priority : int, optional
        The priority of the application, by default `0`.
    max_app_attempts : int, optional
        The max number of attempts for this application, by default `1`
    application_type : str, optional
        The application type, by default `'skein'`
    application_tags : list[ApplicationTag], optional
        List of application tags, by default `[]` will skip setting
        `application-tags`.
    unmanaged_AM : bool, optional
        Indicates if the application uses an unmanaged application master, by
        default `False`.

    Note
    ----
    This model spec contains just the relevant attributes for skein application
    deployment.
    """

    application_id: (
        Annotated[str, msgspec.Meta(pattern=r"application_\d{13}_\d{4}")] | msgspec.UnsetType
    ) = msgspec.UNSET
    application_name: Annotated[str, msgspec.Meta(min_length=1)] | msgspec.UnsetType = msgspec.UNSET
    queue: Annotated[str, msgspec.Meta(min_length=1)]
    am_container_spec: AMContainer | msgspec.UnsetType = msgspec.UNSET
    resource: Resource | msgspec.UnsetType = msgspec.UNSET
    log_aggregation_context: LogAggregationContext | None = None
    priority: Annotated[int, msgspec.Meta(ge=0)] | msgspec.UnsetType = msgspec.UNSET
    max_app_attempts: Annotated[int, msgspec.Meta(ge=1)] | msgspec.UnsetType = msgspec.UNSET
    application_type: Annotated[str, msgspec.Meta(min_length=1)] | msgspec.UnsetType = msgspec.UNSET
    application_tags: list[ApplicationTag] | msgspec.UnsetType = msgspec.UNSET
    unmanaged_AM: bool | msgspec.UnsetType = msgspec.UNSET

    _master: dict[str, Any] = msgspec.field(name="master", default_factory=dict)
    _acls: dict[str, Any] = msgspec.field(name="acls", default_factory=dict)
    _tags: list[str] = msgspec.field(name="tags", default_factory=list)
    _name: Annotated[str, msgspec.Meta(min_length=1)] | None = msgspec.field(
        name="name", default=None
    )
    _max_app_attempts: Annotated[int, msgspec.Meta(ge=1)] = msgspec.field(
        name="max_attempts", default=1
    )

    @ResourceManagerMessage.compute_field
    def application_name_(self) -> str:
        return self._name

    @ResourceManagerMessage.compute_field
    def priority_(self) -> int:
        if isinstance(self.priority, msgspec.UnsetType):
            return 0

    @ResourceManagerMessage.compute_field
    def application_type_(self) -> int:
        if isinstance(self.application_type, msgspec.UnsetType):
            return "skein"

    @ResourceManagerMessage.compute_field
    def resource_(self) -> Resource:
        if self._master:
            return msgspec.convert(self._master, type=Resource)

    @ResourceManagerMessage.compute_field
    def am_container_spec_(self) -> AMContainer:
        if self._master:
            return msgspec.convert({"master": self._master, "acls": self._acls}, type=AMContainer)

    @ResourceManagerMessage.compute_field
    def application_tags_(self) -> list[ApplicationTag]:
        if self._tags:
            return msgspec.convert([{"tag": ", ".join(self._tags)}], type=list[ApplicationTag])

    def encode(self) -> bytes:
        """
        Serialize the object to bytes using JSON encoding.

        Returns
        -------
        bytes
            The serialized object as bytes.
        """
        return msgspec.json.encode(self.to_dict())

    def with_application_id(self, application_id: str) -> ApplicationSubmissionContext:
        """
        Set the application ID.

        Parameters
        ----------
        application_id : str
            The application ID.

        Returns
        -------
        ApplicationSubmissionContext
            The updated ApplicationSubmissionContext object.

        Raises
        ------
        ValueError
            If 'application_id' does not match the pattern 'application_\d{13}_\d{4}'.

        """
        if not re.match(r"application_\d{13}_\d{4}", application_id):
            raise ValueError("'application_id' must match pattern 'application_\d{13}_\d{4}'")
        self.application_id = application_id
        return self

    def with_commands(self, command: str | list[str]) -> ApplicationSubmissionContext:
        """
        Set the commands for launching your container, in the order in which they
        should be executed.

        Parameters
        ----------
        command : str | list[str]
            The commands for launching your container, in the order in which they
            should be executed.

        Returns
        -------
        ApplicationSubmissionContext
            The updated ApplicationSubmissionContext object.
        """
        self.am_container_spec.with_commands(command)
        return self

    def with_credentials(
        self, tokens: dict[str, str] | None = None, secrets: dict[str, str] | None = None
    ) -> ApplicationSubmissionContext:
        """
        Set the credentials for the application.

        Parameters
        ----------
        tokens : dict[str, str] | None, optional
            The tokens for the application, by default None.
        secrets : dict[str, str] | None, optional
            The secrets for the application, by default None.

        Returns
        -------
        ApplicationSubmissionContext
            The updated ApplicationSubmissionContext object.
        """
        self.am_container_spec.with_credentials(tokens=tokens, secrets=secrets)
        return self

    def with_log_aggregation_context(
        self, log_aggregation_context: LogAggregationContext
    ) -> ApplicationSubmissionContext:
        """
        Set the log aggregation context for the application.

        Parameters
        ----------
        log_aggregation_context : LogAggregationContext
            The log aggregation context.

        Returns
        -------
        ApplicationSubmissionContext
            The updated ApplicationSubmissionContext object.
        """
        self.log_aggregation_context = log_aggregation_context
        return self

    def with_priority(self, priority: int) -> ApplicationSubmissionContext:
        """
        Set the priority of the application.

        Parameters
        ----------
        priority : int
            The priority of the application.

        Returns
        -------
        ApplicationSubmissionContext
            The updated ApplicationSubmissionContext object.
        """
        self.priority = priority
        return self

    def with_application_type(self, application_type: str) -> ApplicationSubmissionContext:
        """
        Set the application type.

        Parameters
        ----------
        application_type : str
            The application type.

        Returns
        -------
        ApplicationSubmissionContext
            The updated ApplicationSubmissionContext object.
        """
        self.application_type = application_type
        return self

    def with_unmanaged_AM(self, unmanaged_AM: bool) -> ApplicationSubmissionContext:
        """
        Set the application type.

        Parameters
        ----------
        unmanaged_AM : bool
            The application type.

        Returns
        -------
        ApplicationSubmissionContext
            The updated ApplicationSubmissionContext object.
        """
        self.unmanaged_AM = unmanaged_AM
        return self

    @classmethod
    def from_skein(
        cls, application_id: str, spec: skein_model.ApplicationSpec, **kwargs
    ) -> ApplicationSubmissionContext:
        """Create an instance from a `skein.ApplicationSpec`

        Parameters
        ----------
        application_id : str
            The application id.
        spec : ApplicationSpec
            An instance of a `skein.ApplicationSpec`
        tokens : dict[str, str], optional
            Tokens that you wish to pass to your application, specified as
            key-value pairs. The key is an identifier for the token and the
            value is the token (which should be obtained using the respective
            services)
        secrets : dict[str, str], optional
            Secrets that you wish to use in your application, specified as
            key-value pairs. They key is an identifier and the value is the
            base-64 encoding of the secret
        log_include_pattern : str | list[str], optional
            The log files which match the defined regex include pattern will be
            uploaded when the applicaiton finishes
        log_exclude_pattern : str | list[str], optional
            The log files which match the defined regex exclude pattern will not
            be uploaded when the applicaiton finishes
        rolled_log_include_pattern : str | list[str], optional
            The log files which match the defined regex include pattern will be
            aggregated in a rolling fashion
        rolled_log_exclude_pattern : str | list[str], optional
            The log files which match the defined regex exclude pattern will not
            be aggregated in a rolling fashion
        log_aggregation_policy_class_name : str, optional
            The policy which will be used by NodeManager to aggregate the logs
        log_aggregation_policy_parameters : str, optional
            The parameters passed to the policy class
        priority : int, optional
            The priority of the application, by default `0`
        unmanaged_AM : bool, optional
            Is the application using an unmanaged application master, by default
            `False`.
        application_type : str, optional
            The application type, by default `'skein'`

        Returns
        -------
        ApplicationSubmissionContext
            A ApplicationSubmissionContext definition to submit a YARN application
        """
        return (
            msgspec.convert(spec.to_dict(), type=ApplicationSubmissionContext)
            .with_application_id(application_id)
            .with_credentials(tokens=kwargs.get("tokens"), secrets=kwargs.get("secrets"))
            .with_log_aggregation_context(
                LogAggregationContext.create(
                    kwargs.get("log_include_pattern"),
                    kwargs.get("log_exclude_pattern"),
                    kwargs.get("rolled_log_include_pattern"),
                    kwargs.get("rolled_log_exclude_pattern"),
                    kwargs.get("log_aggregation_policy_class_name"),
                    kwargs.get("log_aggregation_policy_parameters"),
                )
            )
            .with_priority(kwargs.get("priority") or 0)
            .with_application_type(kwargs.get("application_type") or "skein")
            .with_unmanaged_AM(kwargs.get("unmanaged_AM") or False)
        )
