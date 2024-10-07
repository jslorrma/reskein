#!/usr/bin/env python3
"""
yarn/resourcemanager.py
=======================

Python wrapper for YARN's RessourceManager web services [REST API[1]

[1]: https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
"""

from __future__ import annotations

__author__ = "jslorrma"
__maintainer__ = "jslorrma"
__email__ = "jslorrma@gmail.com"

# imports
import os
import pathlib
from typing import Any
from urllib.parse import urljoin, urlparse

import msgspec
import requests
from requests import auth
from skein.model import ApplicationState, FinalStatus, NodeState

from .. import PathType, logger
from ..common.request import make_request
from ..hadoop.config import HadoopConfig

# module imports


class ResourceManager:
    """
    YARN RessourceManager web services REST API wrapper.

    Parameters
    ----------
    address : str, optional
        ResourceManager HTTP(S) address, by default `None`. If `None` resourcemanager
        service address is either read from `SKEIN_DRIVER_RM` environment varibale or
        from the YARN configuration file `yarn-site.xml`. The config directory path that
        has the `yarn-site.xml` can be set with `YARN_CONF_DIR` or `HADOOP_CONF_DIR`
        environment variables. Alternatively this config path can be set with the
        `HadoopConfig().set_config_dir` method.
    auth : requests.auth.AuthBase, optional
        The hadoop cluster authentication methodEither '`kerberos'`
        or `'simple'` or `None`, by default `None`. If `None` the hadoop cluster
        authentication is either read from `SKEIN_DRIVER_AUTH` environment variable or
        from the HADOOP cluster configurationfile `core-site.xml`. The config directory
        path that has the `core-site.xml` can be set with `HADOOP_CONF_DIR`
        environment variable. Alternatively this config path can be set with the
        `HadoopConfig().set_config_dir` method.
    timeout : int, optional
        How many seconds to wait for the server to send data before giving up, by
        default `90`
    verify : bool
        Either a boolean, in which case it controls whether we verify the server's TLS
        certificate, or a string, in which case it must be a path to a CA bundle to use,
        by default to `True`
    proxies : dict[str, str], optional
        Dictionary mapping protocol to the URL of the proxy, by default to `None`
    """

    def __init__(
        self,
        address: str,
        auth: auth.AuthBase | None = None,
        timeout: int | (float | None) = None,
        verify: bool | (PathType | None) = None,
        proxies: dict[str, str] | None = None,
    ):
        self._address = urljoin(address, "/ws/v1/cluster/")
        self._timeout = timeout or 90
        self._verify = verify if verify is not None else True
        self._proxies = proxies
        self._auth = auth

        # setup request session
        self._session = requests.Session()
        self._session.verify = (
            str(self._verify) if isinstance(self._verify, pathlib.Path) else self._verify
        )
        self._session.proxies = self._proxies
        self._session.auth = self._auth

    def _request(
        self,
        api_path: str,
        method: str = "GET",
        timeout: int | (float | None) = None,
        full_response: bool = False,
        **kwargs,
    ) -> dict[str, Any] | (str | requests.Response):
        """Execute request and handle response

        Parameters
        ----------
        api_path : str, optional
            The path of the API endpoint to request.
        method : str, optional
            The HTTP method to use for the request. One of {`'GET'`, `'POST'`, `'PUT'`,
             `'DELETE'`}, by default `'GET'`.
        timeout : int or float, optional
            The number of seconds to wait for the server's response before giving up,
            defaults to `None` (no timeout).
        full_response : bool, optional
            Get the full request response instead of just its content.
        **kwargs
            Additional keyword arguments to pass to the `Session.request` method.

        Returns
        -------
        dict or str or requests.Response
            The response from the server, either as a JSON dictionary or raw string.

        Raises
        ------
        HTTPError
            If the server returns a non-2xx status code.
        """
        # construct url
        # Note: If 'address' is given explicitly we use this instead of
        # the rm node address
        address = kwargs.pop("address", None) or self._address

        # extract params from kwargs and drop entries with value `None`
        params = (
            {k: v for k, v in kwargs.pop("params").items() if v is not None}
            if "params" in kwargs
            else None
        )
        # extract data from kwargs and drop entries with value `None`
        data = kwargs.get("json") or kwargs.get("data")
        kwargs.pop("json", None)
        kwargs.pop("data", None)

        response = make_request(
            session=self._session,
            url=address,
            path=api_path,
            method=method,
            timeout=timeout or self._timeout,
            params=params,
            data=msgspec.json.encode(data) if data else None,
            **kwargs,
        )
        if full_response:
            return response
        try:
            return msgspec.json.decode(response.content)
        except (msgspec.DecodeError, TypeError):
            return response.text

    def cluster(self, **kwargs) -> dict[str, Any]:
        """
        Get cluster information

        The cluster information resource provides overall information about the
        cluster.

        Returns
        -------
        dict[str, Any]
            API response content with JSON data.
        """
        path = "info"
        return self._request(path, **kwargs)

    def metrics(self, **kwargs) -> dict[str, Any]:
        """
        Get cluster metrics.

        Provides some overall metrics about the cluster. More detailed metrics should
        be retrieved from the jmx interface.

        Returns
        -------
        dict[str, Any]
            API response content with JSON data.
        """

        path = "metrics"
        return self._request(path, **kwargs)

    def scheduler(self, **kwargs) -> dict[str, Any]:
        """
        Get cluster scheduler details

        A scheduler resource contains information about the current scheduler configured
        in a cluster. It currently supports both the Fifo and Capacity Scheduler. You
        will get different information depending on which scheduler is configured so be
        sure to look at the type information.

        Returns
        -------
        dict[str, Any]
            API response content with JSON data.
        """

        path = "scheduler"
        return self._request(path, **kwargs)

    def delegation_token(self, renewer: str | None = None, **kwargs) -> dict[str, Any]:
        """
        Get ResourceManager delegation token

        The Delegation Tokens API can be used to create, renew and cancel YARN
        ResourceManager delegation tokens. All delegation token requests must be carried
        out on a Kerberos authenticated connection(using SPNEGO). Carrying out operations
        on a non-kerberos connection will result in a FORBIDDEN response. In case of
        renewing a token, only the renewer specified when creating the token can renew
        the token. Other users(including the owner) are forbidden from renewing tokens.
        It should be noted that when cancelling or renewing a token, the token to be
        cancelled or renewed is specified by setting a header.

        Parameters
        ----------
        renewer : str, optional
            The user who is allowed to renew the delegation token.

        Returns
        -------
        dict[str, Any]
            API response content with JSON data.
        """
        path = "delegation-token"
        data = {"renewer": renewer or os.getenv("USER") or os.getenv("USERNAME")}
        return self._request(path, "POST", json=data, **kwargs)

    def applications(  # noqa: PLR0913
        self,
        states: list[str] | None = None,
        final_status: str | None = None,
        user: str | None = None,
        queue: str | None = None,
        started_time_begin: int | None = None,
        started_time_end: int | None = None,
        finished_time_begin: int | None = None,
        finished_time_end: int | None = None,
        application_types: list[str] | None = None,
        application_tags: list[str] | None = None,
        name: str | None = None,
        limit: int | None = None,
        de_selects: list[str] | None = None,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Get cluster applications.

        With the Applications API, you can obtain a collection of resources,
        each of which represents an application.

        All parameters are matching parameters and only correspoinding applications are
        returned.

        Parameters
        ----------
        states : List[str], optional
            The application states, by default `None`
        final_status : str, optional
            The final status of the application - reported by the application itself, by
            default `None`
        user : str, optional
            The user name, by default `None`
        queue : str, optional
            The queue name, by default `None`
        started_time_begin : int, optional
            Start time beginning with this time, specified in ms since epoch, by default
            `None`
        started_time_end : int, optional
            Start time ending with this time, specified in ms since epoch, by default
            `None`
        finished_time_begin : int, optional
            Finish time beginning with this time, specified in ms since epoch, by
            default `None`
        finished_time_end : int, optional
            Finish time ending with this time, specified in ms since epoch, by default
            `None`
        application_types : List[str], optional
            Application types, specified as a list, by default `None`
        application_tags : List[str], optional
            Application tags, specified as a list, by default `None`
        name : str, optional
            Aame of the application, by default `None`
        limit : str, optional
            Total number of app objects to be returned, by default `None`
        de_selects : List[str], optional
            Alist of generic fields which will be skipped in the result, by default
            `None`

        Returns
        -------
        dict[str, Any]
            API response content with JSON data.
        """
        path = "apps"

        params = {
            "states": (
                ",".join(str(ApplicationState(s)) for s in states)
                if isinstance(states, list | tuple)
                else (str(ApplicationState(states)) if states else None)
            ),
            "finalStatus": str(FinalStatus(final_status)) if final_status else None,
            "user": user,
            "queue": queue,
            "limit": limit,
            "startedTimeBegin": started_time_begin,
            "startedTimeEnd": started_time_end,
            "finishedTimeBegin": finished_time_begin,
            "finishedTimeEnd": finished_time_end,
            "applicationTypes": (
                ",".join(application_types)
                if isinstance(application_types, list | tuple)
                else application_types
            ),
            "applicationTags": (
                ",".join(application_tags)
                if isinstance(application_tags, list | tuple)
                else application_tags
            ),
            "name": name,
            "deSelects": (
                ",".join(de_selects) if isinstance(de_selects, list | tuple) else de_selects
            ),
        }

        return self._request(path, params=params, **kwargs)

    def application_statistics(
        self,
        states: list[str] | None = None,
        application_types: list[str] | None = None,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Get application statistics.

        With the Application Statistics API, you can obtain a collection of
        triples, each of which contains the application type, the application
        state and the number of applications of this type and this state in
        ResourceManager context.

        This method only works in Hadoop > 2.0.0

        Parameters
        ----------
        states : List[str], optional
            The application states, by default `None`
        application_types : List[str], optional
            Application types, specified as a list, by default `None`

        Returns
        -------
        dict[str, Any]
            API response content with JSON data.
        """

        path = "appstatistics"

        params = {
            "states": (
                ",".join(str(ApplicationState(s)) for s in states)
                if isinstance(states, list | tuple)
                else (str(ApplicationState(states)) if states else None)
            ),
            "application_types": (
                ",".join(application_types)
                if isinstance(application_types, list | tuple)
                else application_types
            ),
        }

        return self._request(path, params=params, **kwargs)

    def application(self, application_id: str, **kwargs) -> dict[str, Any]:
        """
        Get information about application with `application_id`

        An application resource contains information about a particular
        application that was submitted to a cluster.

        Parameters
        ----------
        application_id : str
            The application Id

        Returns
        -------
        dict[str, Any]
            API response content with JSON data.
        """

        path = f"apps/{application_id}"
        return self._request(path, **kwargs)

    def application_attempts(self, application_id: str, **kwargs) -> dict[str, Any]:
        """
        Get information about application with `application_id`

        With the application attempts API, you can obtain a collection of
        resources that represent an application attempt.

        Parameters
        ----------
        application_id : str
            The application Id

        Returns
        -------
        dict[str, Any]
            API response content with JSON data.
        """

        path = f"apps/{application_id}/appattempts"
        return self._request(path, **kwargs)

    def application_attempt(self, application_id: str, attempt_id: str, **kwargs) -> dict[str, Any]:
        """
        Get information about application with `application_id` for `attempt_id`

        With the application attempts API, you can obtain an information
        about container related to an application attempt.

        Parameters
        ----------
        application_id : str
            The application Id
        attempt_id : str
            The application Id

        Returns
        -------
        dict[str, Any]
            API response content with JSON data.
        """

        path = f"apps/{application_id}/appattempts/{attempt_id}"
        return self._request(path, **kwargs)

    def application_attempt_containers(
        self, application_id: str, attempt_id: str, **kwargs
    ) -> dict[str, Any]:
        """
        Get information about application with `application_id` for `attempt_id`

        With the application attempts API, you can obtain an information
        about container related to an application attempt.

        Parameters
        ----------
        application_id : str
            The application Id
        attempt_id : str
            The application Id

        Returns
        -------
        dict[str, Any]
            API response content with JSON data.
        """

        path = f"apps/{application_id}/appattempts/{attempt_id}/containers"
        return self._request(path, **kwargs)

    def application_attempt_container(
        self, application_id: str, attempt_id: str, container_id: str, **kwargs
    ) -> dict[str, Any]:
        """
        Get information about an application container with `application_id` and
        `container_id` for `attempt_id`

        With the application attempts API, you can obtain an information
        about container related to an application attempt.

        Parameters
        ----------
        application_id : str
            The application Id
        attempt_id : str
            The application Id
        attempt_id : str
            The container id

        Returns
        -------
        dict[str, Any]
            API response content with JSON data.
        """

        path = f"apps/{application_id}/appattempts/{attempt_id}/containers{container_id}"
        return self._request(path, **kwargs)

    def new_application(self, **kwargs) -> dict[str, Any]:
        """
        Submit an cluster application.

        With the New Application API, you can obtain an application-id which
        can then be used as part of the Submit Applications API to
        submit applications. The response also includes the maximum resource
        capabilities available on the cluster.

        Returns
        -------
        dict[str, Any]
            API response content with JSON data.
        """

        path = "apps/new-application"

        return self._request(path, "POST", **kwargs)

    def submit_application(self, spec: PathType | dict[str, Any], **kwargs) -> dict[str, Any]:
        """
        Submit an cluster application.

        With the Submit Applications API you can submit applications. The response also
        includes the maximum resource capabilities available on the cluster.

        For spec definition refer to [YARN's Submit Application API][1]

        Parameters
        ----------
        spec : pathlib.Path | str | bytes | dict[str, Any]
            A description of the application to run. Can be a path to a yaml/json
            file, a dictionary or bytes encodes dictionary description of an
            application specification.

        Returns
        -------
        dict[str, Any]
            API response content with JSON data.

        [1]: https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html#Cluster_Applications_API.28Submit_Application.29
        """

        path = "apps"

        try:
            if isinstance(spec, PathType) and pathlib.Path(spec).is_file():
                with pathlib.Path(spec).open("r") as _spec_file:
                    try:
                        spec = msgspec.yaml.encode(_spec_file.read())
                    except msgspec.DecodeError:
                        spec = msgspec.json.encode(_spec_file.read())
        except msgspec.DecodeError:
            pass

        return self._request(
            path,
            "POST",
            json=spec if not isinstance(spec, bytes | str) else None,
            data=spec if isinstance(spec, bytes | str) else None,
            **kwargs,
        )

    def move_application(self, application_id: str, queue: str, **kwargs) -> dict[str, Any]:
        """
        Move an cluster application.

        Move a running app to another queue using a PUT request specifying the
        target queue. To perform the PUT operation, authentication has to be
        setup for the RM web services. In addition, you must be authorized to
        move the app. Currently you can only move the app if you are using the
        Capacity scheduler or the Fair scheduler.
        Please note that in order to move an app, you must have an authentication
        filter setup for the HTTP interface. The functionality requires that a
        username is set in the HttpServletRequest. If no filter is setup, the response
        will be an “UNAUTHORIZED” response.

        Parameters
        ----------
        application_id : str
            The application id
        queue : str
            The queue name

        Returns
        -------
        dict[str, Any]
            API response content with JSON data.
        """
        data = {"queue": queue}
        path = f"apps/{application_id}/queue"
        return self._request(path, "PUT", json=data, **kwargs)

    def kill_application(self, application_id: str, **kwargs) -> dict[str, Any]:
        """
        Kill cluster application with `application_id`

        With the application kill API, you can kill an application
        that is not in FINISHED or FAILED state.

        Parameters
        ----------
        application_id : str
            The application Id

        Returns
        -------
        dict[str, Any]
            API response content with JSON data.
        """
        data = {"state": "KILLED"}
        path = f"apps/{application_id}/state"
        return self._request(path, "PUT", json=data, **kwargs)

    def nodes(self, states: list[str] | None = None, **kwargs) -> dict[str, Any]:
        """
        Get cluster nodes.

        With the Nodes API, you can obtain a collection of resources, each of
        which represents a node.


        Parameters
        ----------
        states : List[str], optional
            The nodes states, by default `None`

        Returns
        -------
        dict[str, Any]
            API response content with JSON data.
        """

        path = "nodes"
        params = {
            "states": (
                ",".join(str(NodeState(s)) for s in states)
                if isinstance(states, list | tuple)
                else (str(NodeState(states)) if states else None)
            ),
        }
        return self._request(path, params=params, **kwargs)

    def node(self, node_id: str | None, **kwargs) -> dict[str, Any]:
        """
        Get cluster node details.

        A node resource contains information about a node in the cluster.

        Parameters
        ----------
        node_id : str
            The node id

        Returns
        -------
        dict[str, Any]
            API response content with JSON data.
        """

        path = f"nodes/{node_id}"
        return self._request(path, **kwargs)

    def queues(self, **kwargs) -> dict[str, Any]:
        """
        Get all cluster queues.

        Returns
        -------
        dict[str, Any]
            API response content with JSON data about cluster queues.
        """
        from collections import deque

        scheduler = self.scheduler(**kwargs)
        scheduler_info = scheduler["scheduler"]["schedulerInfo"]

        queues = {}
        bfs_deque = deque([scheduler_info])
        while bfs_deque:
            queue = bfs_deque.popleft()
            queues[queue["queueName"]] = queue
            if "queues" in queue:
                for q in queue["queues"]["queue"]:
                    bfs_deque.append(q)

        return queues

    def queue(self, queue: str, **kwargs) -> dict[str, Any] | None:
        """
        Get cluster queue.

        Given a queue name, this function tries to locate the given queue in
        the object returned by scheduler endpoint.

        Parameters
        ----------
        queue : str
            The queue name

        Returns
        -------
        dict[str, Any]
            API response content with JSON data about queue with `queue` name. `None`
            if no queue with `queue` name exsits.
        """
        return self.queues(**kwargs).get(queue)

    def child_queues(self, queue: str, **kwargs) -> list[dict[str, Any]]:
        """Get cluster children queues.

        Given a parent queue name, this function tries to gather all children queues in
        the object returned by scheduler endpoint.

        Parameters
        ----------
        queue : str
            The queue name

        Returns
        -------
        dict[str, Any]
            API response content with JSON data about queue with `queue` name. `None`
            if no queue with `queue` name exsits.
        """
        return [
            q
            for q in self.queues(**kwargs).values()
            if "queuePath" in q and f"{queue}." in q.get("queuePath")
        ]

    def application_logs(self, application_id: str, **kwargs) -> dict[str, str]:
        """
        Get application logs.

        Parameters
        ----------
        application_id : str
            The id of the application.

        Returns
        -------
        ApplicationLogs : logs
            A mapping of ``yarn_container_id`` to ``logs`` for each container.
        """
        import re

        def _get_logs(url: str, path: str) -> dict[str, str]:
            _resp = self._request(path, address=url, full_response=True)
            path = urlparse(_resp.url).path
            url = _resp.url.replace(path, "")
            _tracking_HTML = _resp.text

            # application master log urls
            _log_urls = re.findall(
                r"(?<=href\=\").*?(?=/\?start=-4096)",
                _tracking_HTML,
            )
            _log_urls += re.findall(
                r"(?<=href\=\").*?(?=/\?start=0)",
                _tracking_HTML,
            )
            _logs = {}
            # parse application master log html and extract applicationmaster log
            for _url in _log_urls:
                _path = urlparse(_url).path + "?start=0"
                _log = f"{urlparse(_url).path.rsplit('/', 1)[1]} (attempt: {i+1})"
                _log_resp = self._request(_path, address=url)
                _logs[_log] = (
                    (_log_resp.partition("<pre>")[2].partition("</pre>")[0])
                    if _log_resp.count("<pre>") < 2  # noqa: PLR2004
                    else (
                        _log_resp.partition("<pre>")[2].partition("<pre>")[2].partition("</pre>")[0]
                    )
                )

            return _logs

        _logs = {}
        # parse application web UI HTML for log links
        _app_attempts = self.application_attempts(application_id, **kwargs)["appAttempts"][
            "appAttempt"
        ]
        for i, _app_attempt in enumerate(_app_attempts):
            if _app_attempt["finishedTime"] == 0:
                _containers = sorted(
                    self.application_attempt_containers(
                        application_id, _app_attempt["appAttemptId"]
                    )["container"],
                    key=lambda x: x["containerId"],
                )
                for _container in _containers:
                    _logs_url_path = urlparse(_container["logUrl"]).path
                    _logs_url_address = _container["logUrl"].replace(_logs_url_path, "")

                    _logs.update(_get_logs(_logs_url_address, _logs_url_path))

            else:
                _logs_url_path = urlparse(_app_attempt["logsLink"]).path
                _logs_url_address = _app_attempt["logsLink"].replace(_logs_url_path, "")
                _logs.update(_get_logs(_logs_url_address, _logs_url_path))

            return _logs

    def get_config(self, out_path: PathType | None = None, name: str | None = None, **kwargs):
        """
        Get hadoop config ('core-site.xml', 'yarn-site.xml', 'hdfs-site.xml')
        and set the Hadoop config path to the output path.

        Parameters
        ----------
        out_path : str | pathlib.Path, optional
            Path to download the configuration files to. By default, it's `None`. If `None`,
            configuration files will be downloaded into `'/.skein/hadoop_conf'`.
        name : str, optional
            The name of the cluster. By default, it's `None`.
        """
        import xml.etree.ElementTree as ET
        from io import StringIO

        _name = f"{name}/" if name else ""
        _path = out_path or (pathlib.Path.cwd() / f".skein/hadoop_conf/{_name}")

        # Create XML file with key, value dictionnary
        def _to_file(filename, attributes):
            tree = ET.Element("configuration")
            for key, v in attributes.items():
                prop = ET.SubElement(tree, "property")
                name = ET.SubElement(prop, "name")
                value = ET.SubElement(prop, "value")
                name.text, value.text = key, v

            root = tree = ET.ElementTree(tree)
            ET.indent(root, space="  ", level=0)
            _path.mkdir(parents=True, exist_ok=True)
            root.write(str(_path / filename), encoding="utf-8")
            logger.info(f"'{filename}' successfully created in '{_path}'")

        # Build hdfs-site.xml file
        def _create(config, service):
            config.seek(0)
            filename = f"{service}-site.xml"
            defaultname = f"{service}-default.xml"

            tree = ET.parse(config)

            attributes = {
                e[0].text: e[1].text
                for e in [
                    entry
                    for entry in tree.findall(".property")
                    if entry[3].text in (filename, defaultname)
                ]
            }
            _to_file(filename, attributes)

        try:
            config = StringIO(self._request("/conf", **kwargs))
            # Generating core-site.xml
            _create(config, "core")

            # Generating yarn-site.xml
            _create(config, "yarn")

            # Generating hdfs-site.xml
            _create(config, "hdfs")

            # set config path
            HadoopConfig().set_config_dir(out_path)

        except Exception as exc:
            logger.info(f"Downloading hadoop config files failed with error: {exc.args}")
