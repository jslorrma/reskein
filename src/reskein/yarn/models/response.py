#!/usr/bin/env python3
"""
yarn/models/messages.py
=======================

This module defines the message models used in the YARN REST API.

The message models provide base methods for decoding messages, converting them
to `Skein` models, converting them to protobuf, and converting them to
dictionaries.

The module includes the following classes:
- *ResourceManagerMessage*: Base class for handling messages from the YARN REST API.
- *ResourceUsageReport*: Resource usage reports from the YARN REST API.
- *Resources: Resources* from the YARN REST API.
- *ResourceUsagesByPartition*: Resource usages by partition from the YARN REST API.
- *ResourceInfo*: Resource info from the YARN REST API.
- *ApplicationReport*: Application report message from the YARN REST API.
- *NodeReport*: Report of node status from the YARN REST API.
- *Queue*: Information about a specific YARN queue from the YARN REST API.

For more details, refer to the docstrings of each class.
"""

from __future__ import annotations

__author__ = "jslorrma"
__maintainer__ = "jslorrma"
__email__ = "jslorrma@gmail.com"


from typing import Annotated, ClassVar, Literal

import msgspec
from skein import model as skein_model

from . import ResourceManagerMessage


class ResourceUsageReport(ResourceManagerMessage):
    """
    Resource usage reports from the YARN REST API.

    Attributes
    ----------
    memory_seconds : int
        The total memory usage in seconds.
    vcore_seconds : int
        The total vcore usage in seconds.
    num_used_containers : int
        The number of used containers.
    needed_resources : Resources
        The resources needed.
    reserved_resources : Resources
        The resources reserved.
    used_resources : Resources
        The resources used.
    """

    _skein_cls: ClassVar[type[skein_model.ResourceUsageReport]] = skein_model.ResourceUsageReport

    memory_seconds: Annotated[int, msgspec.Meta(ge=-1)] | msgspec.UnsetType = msgspec.UNSET
    vcore_seconds: Annotated[int, msgspec.Meta(ge=-1)] | msgspec.UnsetType = msgspec.UNSET
    num_used_containers: Annotated[int, msgspec.Meta(ge=-1)] | msgspec.UnsetType = msgspec.UNSET
    needed_resources: Resources | msgspec.UnsetType = msgspec.UNSET
    reserved_resources: Resources | msgspec.UnsetType = msgspec.UNSET
    used_resources: Resources | None = None

    @ResourceManagerMessage.compute_field
    def num_used_containers_(self) -> int:
        return max(self.num_used_containers, 0)


class Resources(ResourceManagerMessage):
    """
    Resources from the YARN REST API.

    Attributes
    ----------
    memory : int
        The total memory.
    vcores : int
        The total number of virtual cores.
    gpus : int
        The total number of gpus, default is `0`.
    fpgas : int
        The total number of fpgas, default is `0`.
    """

    _skein_cls: ClassVar[type[skein_model.Resources]] = skein_model.Resources

    # message fields
    memory: Annotated[int, msgspec.Meta(ge=-1)] | msgspec.UnsetType = msgspec.UNSET
    vcores: int | msgspec.UnsetType = msgspec.UNSET
    gpus: Annotated[int, msgspec.Meta(ge=-1)] = 0
    fpgas: Annotated[int, msgspec.Meta(ge=-1)] = 0

    # internal fields for message decoding, will be used by converters
    _vCores: Annotated[int, msgspec.Meta(ge=-1)] = msgspec.field(name="vCores", default=0)

    @ResourceManagerMessage.compute_field
    def vcores_(self) -> int:
        return self._vCores

    @ResourceManagerMessage.compute_field
    def memory_(self) -> int:
        return max(self.memory, 0)


class ResourceUsagesByPartition(ResourceManagerMessage):
    """
    Resource usages by partition from the YARN REST API.

    Attributes
    ----------
    used : Resources
        The resources used.
    """

    used: Resources


class ResourceInfo(ResourceManagerMessage):
    """
    Resource info from the YARN REST API.

    Attributes
    ----------
    resourceUsagesByPartition : list[ResourceUsagesByPartition]
        The list of resource usages by partition.
    """

    resourceUsagesByPartition: list[ResourceUsagesByPartition]


class ApplicationReport(ResourceManagerMessage, omit_defaults=True, kw_only=True):
    """
    Application report message from the YARN REST API.

    Attributes
    ----------
    id : str
        The application ID.
    name : str
        The application name.
    user : str
        The user who submitted the application.
    queue : str
        The queue the application was submitted to.
    state : Literal[skein_model.ApplicationState._values]
        The state of the application.
    progress : float
        The progress of the application.
    diagnostics : str
        The diagnostics for the application.
    start_time : int
        The start time of the application.
    finish_time : int
        The finish time of the application.
    host : str
        The host of the application master.
    port : int
        The port of the application master.
    tracking_url : str
        The tracking URL for the application.
    final_status : Literal[skein_model.FinalStatus._values]
        The final status of the application.
    tags : set[str]
        The tags for the application.
    usage : ResourceUsageReport | None
        The resource usage report for the application.
    """

    _skein_cls: ClassVar[type[skein_model.ApplicationReport]] = skein_model.ApplicationReport

    # message fields
    id: (
        Annotated[str, msgspec.Meta(pattern=r"application_\d{13}_\d{4}")] | msgspec.UnsetType
    ) = msgspec.UNSET
    name: str | msgspec.UnsetType = msgspec.UNSET
    user: Annotated[str, msgspec.Meta(min_length=1)] | msgspec.UnsetType = msgspec.UNSET
    queue: str | msgspec.UnsetType = msgspec.UNSET
    state: Literal[skein_model.ApplicationState._values] | msgspec.UnsetType = msgspec.UNSET  # type: ignore[Literal]
    final_status: Literal[skein_model.FinalStatus._values] | msgspec.UnsetType = msgspec.UNSET  # type: ignore[Literal]
    progress: Annotated[float, msgspec.Meta(ge=0.0)] | msgspec.UnsetType = msgspec.UNSET
    diagnostics: str | msgspec.UnsetType = msgspec.UNSET
    tracking_url: str | msgspec.UnsetType = msgspec.UNSET
    start_time: Annotated[int, msgspec.Meta(ge=0)] | msgspec.UnsetType = msgspec.UNSET
    finish_time: Annotated[int, msgspec.Meta(ge=0)] | msgspec.UnsetType = msgspec.UNSET
    host: str | msgspec.UnsetType = msgspec.UNSET
    port: int | msgspec.UnsetType = msgspec.UNSET
    tags: set[str] = set()  # noqa: RUF012
    usage: ResourceUsageReport | msgspec.UnsetType = msgspec.UNSET

    # internal fields for message decoding, will be used by converters
    _address: Annotated[str, msgspec.Meta(pattern=r".*:\d+")] | None = msgspec.field(
        name="amRPCAddress", default=None
    )
    _tags: str | None = msgspec.field(name="applicationTags")
    _memory_seconds: Annotated[int, msgspec.Meta(ge=0)] | None = msgspec.field(
        name="memorySeconds", default=None
    )
    _vcore_seconds: Annotated[int, msgspec.Meta(ge=0)] | None = msgspec.field(
        name="vcoreSeconds", default=None
    )
    _num_used_containers: Annotated[int, msgspec.Meta(ge=-1)] | None = msgspec.field(
        name="runningContainers", default=None
    )
    _allocated_mb: Annotated[int, msgspec.Meta(ge=-1)] | None = msgspec.field(
        name="allocatedMB", default=None
    )
    _allocated_vcores: Annotated[int, msgspec.Meta(ge=-1)] | None = msgspec.field(
        name="allocatedVCores", default=None
    )
    _reserved_mb: Annotated[int, msgspec.Meta(ge=-1)] | None = msgspec.field(
        name="reservedMB", default=None
    )
    _reserved_vcores: Annotated[int, msgspec.Meta(ge=-1)] | None = msgspec.field(
        name="reservedVCores", default=None
    )
    _resource_info: ResourceInfo | None = msgspec.field(name="resourceInfo", default=None)
    _final_status: Literal[skein_model.FinalStatus._values] | None = msgspec.field(  # type: ignore[Literal]
        name="finalStatus", default=None
    )
    _tracking_url: str | None = msgspec.field(name="trackingUrl", default=None)
    _start_time: Annotated[int, msgspec.Meta(ge=0)] | None = msgspec.field(
        name="startedTime", default=None
    )
    _finish_time: Annotated[int, msgspec.Meta(ge=0)] | None = msgspec.field(
        name="finishedTime", default=None
    )

    @ResourceManagerMessage.compute_field
    def host_(self) -> str:
        return self._address.split(":")[0] if self._address else ""

    @ResourceManagerMessage.compute_field
    def port_(self) -> str:
        return int(self._address.split(":")[1]) if self._address else 0

    @ResourceManagerMessage.compute_field
    def tags_(self) -> str:
        if self._tags:
            return {tag.strip() for tag in self._tags.split(",")}

    @ResourceManagerMessage.compute_field
    def final_status_(self) -> Literal[skein_model.FinalStatus._values]:  # type: ignore[Literal]
        return self._final_status

    @ResourceManagerMessage.compute_field
    def tracking_url_(self) -> str:
        return self._tracking_url

    @ResourceManagerMessage.compute_field
    def start_time_(self) -> int:
        return self._start_time

    @ResourceManagerMessage.compute_field
    def finish_time_(self) -> str:
        return self._finish_time

    @ResourceManagerMessage.compute_field
    def usage_(self) -> ResourceUsageReport:
        return ResourceUsageReport(
            memory_seconds=self._memory_seconds,
            vcore_seconds=self._vcore_seconds,
            num_used_containers=self._num_used_containers,
            needed_resources=Resources(self._allocated_mb, self._allocated_vcores),
            reserved_resources=Resources(self._reserved_mb, self._reserved_vcores),
            used_resources=(
                self._resource_info.resourceUsagesByPartition[0].used
                if self._resource_info
                else None
            ),
        )


class NodeReport(ResourceManagerMessage, kw_only=True):
    """Report of node status.

    Attributes
    ----------
    id : str
        The node id.
    http_address : str
        The http address to the node manager.
    rack_name : str
        The rack name for this node.
    labels : set
        Node labels for this node.
    state : NodeState
        The node's current state.
    health_report : str
        The diagnostic health report for this node.
    total_resources : Resources
        Total resources available on this node.
    used_resources : Resources
        Used resources available on this node.
    """

    _skein_cls: ClassVar[type[skein_model.NodeReport]] = skein_model.NodeReport

    # message fields
    id: Annotated[str, msgspec.Meta(min_length=1)] | msgspec.UnsetType = msgspec.UNSET
    state: Literal[skein_model.NodeState._values] | msgspec.UnsetType = msgspec.UNSET  # type: ignore[Literal]
    http_address: (
        Annotated[str, msgspec.Meta(pattern=r".*:\d+")] | msgspec.UnsetType
    ) = msgspec.UNSET
    rack_name: Annotated[str, msgspec.Meta(min_length=1)] | msgspec.UnsetType = msgspec.UNSET
    health_report: str | msgspec.UnsetType = msgspec.UNSET
    labels: set[str] = set()  # noqa: RUF012
    total_resources: Resources | msgspec.UnsetType = msgspec.UNSET
    used_resources: Resources | msgspec.UnsetType = msgspec.UNSET

    # internal fields for message decoding, will be used by converters
    _used_memory: Annotated[int, msgspec.Meta(ge=-1)] | None = msgspec.field(
        name="usedMemoryMB", default=None
    )
    _available_memory: Annotated[int, msgspec.Meta(ge=-1)] | None = msgspec.field(
        name="availMemoryMB", default=None
    )
    _used_vcores: Annotated[int, msgspec.Meta(ge=-1)] | None = msgspec.field(
        name="usedVirtualCores", default=None
    )
    _available_vcores: Annotated[int, msgspec.Meta(ge=-1)] | None = msgspec.field(
        name="availableVirtualCores", default=None
    )
    _http_address: Annotated[str, msgspec.Meta(pattern=r".*:\d+")] | None = msgspec.field(
        name="nodeHTTPAddress", default=None
    )
    _rack_name: Annotated[str, msgspec.Meta(min_length=1)] | None = msgspec.field(
        name="rack", default=None
    )
    _health_report: str | None = msgspec.field(name="healthReport", default=None)

    @ResourceManagerMessage.compute_field
    def total_resources_(self) -> str:
        return Resources(
            memory=self._available_memory + self._used_memory,
            vcores=self._available_vcores + self._used_vcores,
        )

    @ResourceManagerMessage.compute_field
    def used_resources_(self) -> str:
        return Resources(memory=self._used_memory, vcores=self._used_vcores)

    @ResourceManagerMessage.compute_field
    def http_address_(self) -> str:
        return self._http_address

    @ResourceManagerMessage.compute_field
    def rack_name_(self) -> str:
        return self._rack_name

    @ResourceManagerMessage.compute_field
    def health_report_(self) -> str:
        return self._health_report


class Queue(ResourceManagerMessage, kw_only=True):
    """Information about a specific YARN queue.

    Attributes
    ----------
    name : str
        The name of the queue.
    state : str
        The state of the queue.
    capacity : float
        The queue's capacity as a percentage. For the capacity scheduler, the
        queue is guaranteed access to this percentage of the parent queue's
        resources (if sibling queues are running over their limit, there may be
        a lag accessing resources as those applications scale down). For the
        fair scheduler, this number is the percentage of the total cluster this
        queue currently has in its fair share (this will shift dynamically
        during cluster use).
    max_capacity : float
        The queue's max capacity as a percentage. For the capacity scheduler,
        this queue may elastically expand to use up to this percentage of its
        parent's resources if its siblings aren't running at their capacity.
        For the fair scheduler this is always 100%.
    percent_used : float
        The percent of this queue's capacity that's currently in use. This may
        be over 100% if elasticity is in effect.
    node_labels : set
        A set of all accessible node labels for this queue. If all node labels
        are accessible this is the set ``{"*"}``.
    default_node_label : str
        The default node label for this queue. This will be used if the
        application doesn't specify a node label itself.
    """

    _skein_cls: ClassVar[type[skein_model.Queue]] = skein_model.Queue

    # message fields
    name: Annotated[str, msgspec.Meta(min_length=1)] | msgspec.UnsetType = msgspec.UNSET
    state: Literal[skein_model.QueueState._values] | None = None  # type: ignore[Literal]
    capacity: Annotated[float, msgspec.Meta(ge=0.0)] | msgspec.UnsetType = msgspec.UNSET
    max_capacity: Annotated[float, msgspec.Meta(ge=0.0)] | msgspec.UnsetType = msgspec.UNSET
    percent_used: Annotated[float, msgspec.Meta(ge=0.0)] | msgspec.UnsetType = msgspec.UNSET
    node_labels: set[str] | msgspec.UnsetType = msgspec.UNSET
    default_node_label: str = ""

    _name: Annotated[str, msgspec.Meta(min_length=1)] | None = msgspec.field(
        name="queueName", default=None
    )
    _max_capacity: Annotated[float, msgspec.Meta(ge=0.0)] | None = msgspec.field(
        name="maxCapacity", default=None
    )
    _percent_used: Annotated[float, msgspec.Meta(ge=0.0)] | None = msgspec.field(
        name="usedCapacity", default=None
    )
    _node_labels: set[str] = msgspec.field(name="nodeLabels", default_factory=lambda: set("*"))

    @ResourceManagerMessage.compute_field
    def name_(self) -> str:
        return self._name

    @ResourceManagerMessage.compute_field
    def max_capacity_(self) -> float:
        return self._max_capacity

    @ResourceManagerMessage.compute_field
    def percent_used_(self) -> float:
        return self._percent_used

    @ResourceManagerMessage.compute_field
    def node_labels_(self) -> set[str]:
        return self._node_labels
