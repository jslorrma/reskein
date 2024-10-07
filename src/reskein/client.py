#!/usr/bin/env python3
"""
client.py
=========

This module redefines certain functions and methods in `skein.core` to allow the
`skein.core.Client` to use the Python `Driver` implementation provided by this
module instead of the Java-based one provided by the [Skein][1] package.

> **Note** Unlike the original Java-based `Skein` implementation, no global
`driver` running in a separate thread or process is necessary. Instead, the
`Driver` is initialized inside the `Client`. This enables a more efficient usage
of the `skein.core.Client`, and removes the requirement for Java setup or
initiating a Java Virtual Machine (JVM), thereby enhancing efficiency and ease
of use.

Example
-------

```python
from reskein.client import Client

# Create a Client object
client = Client()

# Use the client object to interact with the Hadoop cluster
app_report = client.application_report(app_id='application_1234567890123_0001')
```

you can also use the `Client` object as a context manager:

```python
from reskein.client import Client

# Use the Client object as a context manager
with Client() as client:
    # Use the client object to interact with the Hadoop cluster
    app_report = client.application_report(app_id='application_1234567890123_0001')
```

[1]: https://github.com/jcrist/skein
"""

from __future__ import annotations

__author__ = "jslorrma"
__maintainer__ = "jslorrma"
__email__ = "jslorrma@gmail.com"

import time

import msgspec
from skein import model as skein_model

# skein imports
from skein.core import ApplicationClient, _ClientBase
from skein.exceptions import ApplicationNotRunningError
from skein.utils import datetime_to_millis

# module imports
from . import PathType, get_logger
from .common.exceptions import DriverError, context
from .common.utils import as_iterable
from .common.utils import parse_datetime as _parse_datetime
from .yarn.models.response import ApplicationReport, NodeReport, Queue

# init logger
logger = get_logger()


def has_started(report: skein_model.ApplicationReport):
    """`Skein` application has started"""
    return report.state in (
        skein_model.ApplicationState.RUNNING,
        skein_model.ApplicationState.FINISHED,
        skein_model.ApplicationState.FAILED,
        skein_model.ApplicationState.KILLED,
    )


def has_completed(report: skein_model.ApplicationReport):
    """`Skein` application has completed"""
    return report.state in (
        skein_model.ApplicationState.FINISHED,
        skein_model.ApplicationState.FAILED,
        skein_model.ApplicationState.KILLED,
    )


class Client(_ClientBase):
    """
    Connect to and schedule applications on the YARN cluster via the `Driver`.

    Note: The `Driver` is the `Client`'s proxy for all interactions with `YARN`'s resource
    manager and the `WebHDFS` REST API. It gathers information about the cluster and applications,
    and manages necessary file system operations. Unlike the original Java-based `Skein`
    implementation, we do not have a global driver running in a separate thread or process.
    Instead, the `Driver` is initialized inside the `Client`.

    All parameters provided to the `Client` will be forwarded to the `Driver`.

    Parameters
    ----------
    `rm_address` : str, optional
        The ResourceManager HTTP(S) service address. If not provided, the address is read from
        the `SKEIN_DRIVER_RM` environment variable or the `yarn-site.xml` configuration file.
    `hdfs_address` : str, optional
        The (Web)HDFS HTTP(S) service address. If not provided, the address is read from
        the `SKEIN_DRIVER_HDFS` environment variable or the `hdfs-site.xml` configuration file.
    `auth` : str, optional
        The authentication method used by the `Driver` to authenticate with the Hadoop cluster.
        It can be `'kerberos'`, `'simple'`, or `None`. If not provided, the authentication
        method is read from the `SKEIN_DRIVER_AUTH` environment variable or the `core-site.xml`
        configuration file.
    `timeout` : int, optional
        The number of seconds to wait for the server to send data before giving up. Default is
        `90`.
    `verify` : bool or str, optional
        Controls whether we verify the server's TLS certificate, or a path to a CA bundle to
        use. Default is `True`.
    `proxies` : dict, optional
        A dictionary mapping protocol to the URL of the proxy. Default is `None`.
    `keytab` : str or pathlib.Path, optional
        The path to a keytab file for kerberos authentication of the `Driver` and the `Skein`
        application. Can be `None` for non-secure clusters. Default is `None`.
    `principal` : str, optional
        The principal to use for kerberos authentication. If `None`, the principal will be take
        from the first entry in the keytab file. Default is `None`.
    `user` : str, optional
        The username for simple authentication. If not given, the value of `SKEIN_USER_NAME`
        environment variable or the current system user's username is used. Default is `None`.
    `log` : str or bool, optional
        Sets the logging behavior. Values may be a path for logs to be written to, `True` to
        log to stdout/stderr, or `False` to turn off logging completely. Default is `False`.
    `log_level` : str or int, optional
        The `Driver` log level. One of `{'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}`
        (from most to least verbose). Default is `'INFO'`.
    `**kwargs`
        Additional keyword arguments to pass to simple or kerberos `requests`-AuthHandler.

    Notes
    -----
    The configuration parameters (such as `rm_address`, `hdfs_address`, `auth`, etc.) can be
    specified in multiple ways, and the order of precedence is as follows:
    1. Parameter value passed directly to the function. For example
       `start_global_driver(rm_address='http://localhost:8088')`.
    2. Value of the corresponding environment variable. For example, if the `SKEIN_DRIVER_RM`
       environment variable is set, its value will be used as the `rm_address`.
    3. Value from the corresponding Hadoop configuration file (`yarn-site.xml` for `rm_address`,
       `hdfs-site.xml` for `hdfs_address`, `core-site.xml` for `auth`). The directory path
       containing these configuration files can be set with the `YARN_CONF_DIR` or
       `HADOOP_CONF_DIR` environment variables, or with the `HadoopConfig().set_config_dir` method.

    Examples
    --------
    ```pycon
    >>> with Client() as client:
    ...     app_id = client.submit("spec.yaml")
    ...
    ```
    """

    def __init__(  # noqa: PLR0913
        self,
        rm_address: str | None = None,
        hdfs_address: str | None = None,
        auth: str | None = None,
        timeout: int | (float | None) = None,
        verify: bool | (PathType | None) = None,
        proxies: dict[str, str] | None = None,
        keytab: PathType | None = None,
        principal: str | None = None,
        user: str | None = None,
        log: PathType | (bool | None) = None,
        log_level: str | (int | None) = None,
        **kwargs,  # will be passed to request aut handler
    ):
        from .driver import Driver

        logger.debug("Initializing Client...")
        # initialize driver
        self._driver = Driver(
            rm_address=rm_address,
            hdfs_address=hdfs_address,
            auth=auth,
            timeout=timeout,
            verify=verify,
            proxies=proxies,
            keytab=keytab,
            principal=principal,
            user=user,
            log=log,
            log_level=log_level,
            **kwargs
        )



    @classmethod
    def from_global_driver(cls):
        """
        Establish a connection to the global driver.

        If no global driver is available, this method will initialize a new one.

        Returns
        -------
        Client
            A new `Client` instance connected to the global driver.


        Examples
        --------
        ```pycon
        >>> client = Client.from_global_driver()
        ```
        """
        return cls()

    @staticmethod
    def start_global_driver(  # noqa: PLR0913
        rm_address: str | None = None,
        hdfs_address: str | None = None,
        auth: str | None = None,
        timeout: int | None = None,
        verify: bool | PathType | None = None,
        proxies: dict[str, str] | None = None,
        keytab: PathType | None = None,
        principal: str | None = None,
        user: str | None = None,
        log: PathType | bool | None = None,
        log_level: str | int | None = None,
        **kwargs,  # will be passed to request aut handler
    ):
        """
        Initialize and start the "global driver" by setting up connections, testing them, and
        writing out the driver initialization arguments.

        Note: In this Python-based implementation, unlike the original Java-based `Skein`
        implementation, we do not have a global driver running in a separate thread or process.
        Instead, the `Driver` is initialized as part of the `Client` as a singleton object, that
        can be accessed by multiple `Client` instances.

        Parameters
        ----------
        rm_address : str, optional
            The ResourceManager HTTP(S) service address. If not provided, the address is read from
            the `SKEIN_DRIVER_RM` environment variable or the `yarn-site.xml` configuration file.
        hdfs_address : str, optional
            The (Web)HDFS HTTP(S) service address. If not provided, the address is read from
            the `SKEIN_DRIVER_HDFS` environment variable or the `hdfs-site.xml` configuration file.
        auth : str, optional
            The authentication method used by the `Driver` to authenticate with the Hadoop cluster.
            It can be `'kerberos'`, `'simple'`, or `None`. If not provided, the authentication
            method is read from the `SKEIN_DRIVER_AUTH` environment variable or the `core-site.xml`
            configuration file.
        timeout : int, optional
            The number of seconds to wait for the server to send data before giving up. Default is
            `90`.
        verify : bool or str, optional
            Controls whether we verify the server's TLS certificate, or a path to a CA bundle to
            use. Default is `True`.
        proxies : dict, optional
            A dictionary mapping protocol to the URL of the proxy. Default is `None`.
        keytab : str or pathlib.Path, optional
            The path to a keytab file for kerberos authentication of the `Driver` and the `Skein`
            application. Can be `None` for non-secure clusters. Default is `None`.
        principal : str, optional
            The principal to use for kerberos authentication. If `None`, the principal will be take
            from the first entry in the keytab file. Default is `None`.
        user : str, optional
            The username for simple authentication. If not given, the value of `SKEIN_USER_NAME`
            environment variable or the current system user's username is used. Default is `None`.
        log : str or bool, optional
            Sets the logging behavior. Values may be a path for logs to be written to, `True` to
            log to stdout/stderr, or `False` to turn off logging completely. Default is `False`.
        log_level : str or int, optional
            The `Driver` log level. One of `{'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}`
            (from most to least verbose). Default is `'INFO'`.
        **kwargs
            Additional keyword arguments to pass to simple or kerberos `requests`-AuthHandler.

        Notes
        -----
        The configuration parameters (such as `rm_address`, `hdfs_address`, `auth`, etc.) can be
        specified in multiple ways, and the order of precedence is as follows:
        1. Parameter value passed directly to the function. For example
           `start_global_driver(rm_address='http://localhost:8088')`.
        2. Value of the corresponding environment variable. For example, if the `SKEIN_DRIVER_RM`
           environment variable is set, its value will be used as the `rm_address`.
        3. Value from the corresponding Hadoop configuration file (`yarn-site.xml` for `rm_address`,
           `hdfs-site.xml` for `hdfs_address`, `core-site.xml` for `auth`). The directory path
           containing these configuration files can be set with the `YARN_CONF_DIR` or
           `HADOOP_CONF_DIR` environment variables, or with the `HadoopConfig().set_config_dir` method.


        Examples
        --------
        ```pycon
        >>> start_global_driver(
        ...     rm_address="http://localhost:8088", auth="simple", timeout=100
        ... )
        ```
        """
        from .driver import Driver

        logger.debug("Starting global driver...")
        return Driver.start_global_driver(
            rm_address=rm_address,
            hdfs_address=hdfs_address,
            auth=auth,
            timeout=timeout,
            verify=verify,
            proxies=proxies,
            keytab=keytab,
            principal=principal,
            user=user,
            log=log,
            log_level=log_level,
            **kwargs,
        )

    @staticmethod
    def stop_global_driver(force: bool = False):
        """
        Terminate the global driver.

        As, in this Python-based implementation, the driver is not running in a
        separate thread or process, this method does not actually stop the
        global driver, but rather removes the pickled driver file and the init
        args file for the driver if `force` is `True`.

        Parameters
        ----------
        force : bool, optional
            This parameter is ignored in this Python-based implementation.

        Examples
        --------
        ```pycon
        >>> Client.stop_global_driver()
        ```
        """
        from .driver import Driver

        # remove "global driver"
        logger.debug("Stopping global driver...")
        Driver.stop_global_driver()

    def __reduce__(self):
        """
        Support for pickling the `Client` object.

        Returns
        -------
        tuple
            A tuple containing the class type and a tuple of the `ResourceManager` address, `HDFS`
            address, and `authentication` method.
        """
        return (
            type(self),
            (self._driver._rm_address, self._driver._hdfs_address, self._driver._auth),
        )

    def __repr__(self):
        """
        Return a string representation of the `Client` object.

        Returns
        -------
        str
            A string representation of the `Client` object, including the `ResourceManager` address,
            `HDFS` address, and `authentication` method.

        Examples
        --------
        ```pycon
        >>> repr(client)
        'Client<RM:localhost:8088, HDFS:localhost:9000, Auth:simple>'
        ```
        """
        if hasattr(self, "_driver"):
            return (
                f"Client<RM:{self._driver._rm_address}, "
                f"HDFS:{self._driver._hdfs_address}, Auth:{self._driver._auth}>"
            )
        else:
            return "Client<Not connected>"

    def close(self):
        """
        Close the `Client` object.

        This method currently does nothing, but is included to provide a consistent API with the
        original `Skein` implementation.
        """

    def __enter__(self):
        """
        Support for `with` statement.

        Returns
        -------
        Client
            The `Client` object itself.

        Examples
        --------
        ```pycon
        >>> with Client() as client:
        ...     ...  # use client
        ...
        ```
        """
        return self

    def __exit__(self, *args):
        """
        Support for `with` statement.

        This method calls the `close` method when exiting the `with` block.
        """
        self.close()

    def __del__(self):
        """
        Support for object finalization.

        This method calls the `close` method when the `Client` object is being
        finalized (i.e., when it's being garbage collected).
        """
        self.close()

    def _raise_for_application_exists(self, app_id: str):
        """
        Raise an error if the application does not exist.

        Parameters
        ----------
        app_id : str
            The ID of the application to check.

        Raises
        ------
        DriverError
            If the application with the specified ID does not exist.

        """
        _all_states = [
            "NEW",
            "NEW_SAVING",
            "SUBMITTED",
            "ACCEPTED",
            "RUNNING",
            "FINISHED",
            "FAILED",
            "KILLED",
        ]
        if app_id not in [_app.id for _app in self.get_applications(states=_all_states)]:
            raise DriverError(f"Application with app id '{app_id}' not found!")

    def submit(self, spec, **kwargs):
        """
        Submit a new `Skein` application.

        Parameters
        ----------
        spec : skein_model.ApplicationSpec, str, or dict
            A description of the application to run. Can be an `skein_model.ApplicationSpec` object,
            a path to a yaml/json file, or a dictionary description of an application specification.
        **kwargs : dict, optional
            Additional keyword arguments to pass to the `submit_application` method.

        Returns
        -------
        app_id : str
            The ID of the submitted application.

        Examples
        --------
        ```pycon
        >>> app_spec = skein_model.ApplicationSpec(...)
        >>> with Client() as client:
        ...     app_id = client.submit(app_spec)
        ...
        ```
        """
        spec = skein_model.ApplicationSpec._from_any(spec)
        app_id = self._driver.submit_application(spec, **kwargs)
        return app_id

    def submit_and_connect(self, spec, **kwargs):
        """
        Submit a new `Skein` application, and wait to connect to it.

        This method submits a new application based on the provided specification, then attempts
        to connect to it. If an error occurs during the connection attempt, the application is
        killed.

        Parameters
        ----------
        spec : skein_model.ApplicationSpec, str, or dict
            A description of the application to run. Can be an `skein_model.ApplicationSpec` object, a path to
            a yaml/json file, or a dictionary description of an application specification.

        **kwargs : dict, optional
            Additional keyword arguments to pass to the `submit` method.

        Returns
        -------
        app_client : ApplicationClient
            An `ApplicationClient` object connected to the newly submitted application.

        Raises
        ------
        BaseException
            If an error occurs while connecting to the application, the application is killed and
            the exception is re-raised.

        Examples
        --------
        ```pycon
        >>> app_spec = skein_model.ApplicationSpec(...)
        >>> with Client() as client:
        ...     app_client = client.submit_and_connect(app_spec)
        ...
        ```
        """
        app_id = self.submit(spec, **kwargs)
        try:
            return self.connect(app_id, security=spec.master.security)
        except BaseException:
            self.kill(app_id)
            raise

    def connect(
        self,
        app_id: str,
        wait: bool = True,
        security: skein_model.Security = None,
        timeout: int | float | None = None,
    ):
        """
        Connect to a running application.

        Parameters
        ----------
        app_id : str
            The ID of the application to connect to.
        wait : bool, optional
            If `True` (default), this method blocks until the application starts. If `False`, this
            method immediately raises an `ApplicationNotRunningError` if the application isn't
            running.
        security : skein_model.Security, optional
            The security configuration to use when communicating with the application master. If
            not provided, the global configuration is used.
        timeout : int | float | None, optional
            The maximum time, in seconds, to wait for the application to start. If `None`, there
            is no timeout.

        Returns
        -------
        ApplicationClient
            An `ApplicationClient` object connected to the running application.

        Raises
        ------
        ApplicationNotRunningError
            If the application isn't running, regardless of the `wait` parameter value.

        Examples
        --------
        ```pycon
        >>> with Client() as client:
        ...     app_client = client.connect(app_id="application_1632738437881_0001")
        ...
        ```
        """
        if wait:
            report = self.wait_for_start(app_id, timeout)
        else:
            report = self.get_report(app_id)
        if report.state is not skein_model.ApplicationState.RUNNING:
            raise ApplicationNotRunningError(
                f"{app_id} is not running. Application state: {report.state}"
            )

        if security is None:
            security = skein_model.Security.from_default()

        return ApplicationClient("%s:%d" % (report.host, report.port), app_id, security=security)

    def get_applications(  # noqa: PLR0913
        self,
        states: str | list[skein_model.ApplicationState] | None = None,
        name: str | None = None,
        user: str | None = None,
        queue: str | None = None,
        started_begin: str | None = None,
        started_end: str | None = None,
        finished_begin: str | None = None,
        finished_end: str | None = None,
    ) -> list[skein_model.ApplicationReport]:
        """
        Get the status of current `Skein` applications.

        Parameters
        ----------
        states : str | list[skein_model.ApplicationState], optional
            If provided, applications will be filtered to these application
            states. Default is `['SUBMITTED', 'ACCEPTED', 'RUNNING']`.
        name : str, optional
            Only select applications with this name.
        user : str, optional
            Only select applications with this user.
        queue : str, optional
            Only select applications in this queue.
        started_begin : str, optional
            Only select applications that started after this time (inclusive).
            Can be a string representation of a datetime.
        started_end : str, optional
            Only select applications that started before this time (inclusive).
            Can be a string representation of a datetime.
        finished_begin : str, optional
            Only select applications that finished after this time (inclusive).
            Can be a string representation of a datetime.
        finished_end : str, optional
            Only select applications that finished before this time (inclusive).
            Can be a string representation of a datetime.

        Returns
        -------
        reports : List[skein_model.ApplicationReport]
            A list of `skein_model.ApplicationReport` objects for each application that matches the provided
            filters. If no applications match the filters, an empty list is returned.

        Examples
        --------
        ```pycon
        >>> reports = client.get_applications(states=["RUNNING"], user="testuser")
        ```
        """
        if states is not None:
            states = [skein_model.ApplicationState(s) for s in as_iterable(states)]
        else:
            states = [
                skein_model.ApplicationState.SUBMITTED,
                skein_model.ApplicationState.ACCEPTED,
                skein_model.ApplicationState.RUNNING,
            ]

        params = {
            "states": [str(s) for s in states] if states else None,
            "user": user,
            "queue": queue,
            "started_time_begin": datetime_to_millis(
                _parse_datetime(started_begin, "started_begin")
            ),
            "started_time_end": datetime_to_millis(_parse_datetime(started_end, "started_end")),
            "finished_time_begin": datetime_to_millis(
                _parse_datetime(finished_begin, "finished_begin")
            ),
            "finished_time_end": datetime_to_millis(_parse_datetime(finished_end, "finished_end")),
            "application_types": "skein",
            "name": name,
        }

        method = "applications"
        resp = self._driver._call(method, **params)["apps"]
        if resp:
            return sorted(
                (
                    msgspec.convert(report, type=ApplicationReport).to_skein()
                    for report in resp["app"]
                ),
                key=lambda x: x.id,
            )
        else:
            # return emmpty list if nothing response is empty
            return []

    def get_nodes(
        self, states: str | (list[skein_model.NodeState] | None) = None
    ) -> list[skein_model.NodeReport]:
        """Get the status of nodes in the cluster.

        Parameters
        ----------
        states : sequence of skein_model.NodeState, optional
            If provided, nodes will be filtered to these node states, by default `None`.
            `None` defaults to all states.

        Returns
        -------
        reports : List[skein_model.NodeReport]
        """

        params = {
            "states": ",".join([str(skein_model.NodeState(s)) for s in as_iterable(states)])
            if states
            else None
        }

        method = "nodes"
        resp = self._driver._call(method, **params)["nodes"]
        # return resp
        if resp:
            return sorted(
                (msgspec.convert(node, type=NodeReport).to_skein() for node in resp["node"]),
                key=lambda x: x.id,
            )
        else:
            # return emmpty list if nothing response is empty
            return []

    def get_queue(self, name: str) -> skein_model.Queue:
        """Get information about a queue.

        Parameters
        ----------
        name : str
            The queue name.

        Returns
        -------
        queue : skein_model.Queue
        """

        params = {"queue": name}
        method = "queue"
        resp = self._driver._call(method, **params)

        if resp:
            return msgspec.convert(resp, type=Queue).to_skein()
        else:
            raise context.ValueError(f"skein_model.Queue '{name}' does not exist")

    def get_child_queues(self, name: str) -> list[skein_model.Queue]:
        """Get information about all children of a parent queue.

        Parameters
        ----------
        name : str
            The parent queue name.

        Returns
        -------
        queues : List[skein_model.Queue]
        """
        params = {"queue": name}
        method = "child_queues"
        resp = self._driver._call(method, **params)
        if resp:
            return sorted(
                (msgspec.convert(queue, type=Queue).to_skein() for queue in resp),
                key=lambda x: x.name,
            )
        else:
            # return emmpty list if nothing response is empty
            return []

    def get_all_queues(self) -> list[skein_model.Queue]:
        """Get information about all queues in the cluster.

        Returns
        -------
        queues : List[skein_model.Queue]

        """
        method = "queues"
        resp = self._driver._call(method)
        if resp:
            return sorted(
                (msgspec.convert(queue, type=Queue).to_skein() for queue in resp.values()),
                key=lambda x: x.name,
            )
        else:
            # return emmpty list if nothing response is empty
            return []

    def get_report(self, app_id: str) -> skein_model.ApplicationReport:
        """Get a report on the status of a `Skein` application.

        Parameters
        ----------
        app_id : str
            The id of the application.

        Returns
        -------
        report : skein_model.ApplicationReport
        """
        # raise error if application does not exist
        self._raise_for_application_exists(app_id)

        params = {"application_id": app_id}
        method = "application"
        resp = self._driver._call(method, **params)["app"]
        if resp:
            return msgspec.convert(resp, type=ApplicationReport).to_skein()

    application_report = get_report

    def get_status(
        self,
        app_id: str,
    ) -> skein_model.ApplicationState:
        """Get the status of a `Skein` application.

        Parameters
        ----------
        app_id : str
            The id of the application.

        Returns
        -------
        state : skein_model.ApplicationState
        """
        return self.get_report(app_id).state

    def get_logs(self, app_id: str, user: str = "") -> skein_model.ApplicationLogs | None:
        """Get logs from a pending, running or completed running `Skein` application.

        Parameters
        ----------
        app_id : str
            The id of the application.
        user : str, optional
            (NOT supported!)
            The user to get the application logs as. Requires the current user
            to have permissions to proxy as ``user``. Default is the current
            user.

        Returns
        -------
        logs : skein_model.ApplicationLogs
            A mapping of ``yarn_container_id`` to ``logs`` for each container.
        """
        # raise error if application does not exist
        self._raise_for_application_exists(app_id)

        params = {"application_id": app_id}
        method = "application_logs"
        resp = self._driver._call(method, **params)
        if resp:
            return skein_model.ApplicationLogs(app_id, resp)
        else:
            return skein_model.ApplicationLogs(app_id, {"": "No logs available\n"})

    application_logs = get_logs

    def wait_for_start(
        self, app_id: str, timeout: int | float | None = None
    ) -> skein_model.ApplicationReport:
        """Watch and wait for application start up.

        Parameters
        ----------
        app_id : str
            The id of the application.
        timeout : int | float, optional
            The maximum time to wait for the application to start, in seconds.
            If `None`, there is no timeout.

        Returns
        -------
        report : skein_model.ApplicationReport
        """
        _start = time.time()
        while True:
            time.sleep(1)
            report = self.get_report(app_id)
            if has_started(report):
                return report
            if timeout is not None and time.time() - _start > timeout:
                raise DriverError(f"Application with app id '{app_id}' failed to start (timeout)!")

    def move_application(self, app_id: str, queue: str) -> bool:
        """Move an application to a different queue.

        Parameters
        ----------
        app_id : str
            The id of the application to move.
        queue : str
            The queue to move the application to.

        Returns
        -------
        success : bool
            `True` if application was moved succesfully
        """
        # raise error if application does not exist
        self._raise_for_application_exists(app_id)

        params = {"application_id": app_id, "queue": queue}
        method = "move_application"
        resp = self._driver._call(method, **params)["queue"]
        logger.info(f"Moved application {app_id} to {resp}")
        return resp == queue

    def kill_application(self, app_id: str, user: str = "") -> bool:
        """Kill a `Skein` application.

        Parameters
        ----------
        app_id : str
            The id of the application to kill.
        user : str, optional
            (Not supported)
            The user to kill the application as. Requires the current user to
            have permissions to proxy as ``user``. Default is the current user.

        Returns
        -------
        success : bool
            `True` if application was killed. `False` if not or if application state was
            not `'RUNNING'` - application was already killed before or is finished.
        """
        # raise error if application does not exist
        self._raise_for_application_exists(app_id)

        if has_completed(self.get_report(app_id)):
            logger.info(f"Application {app_id} already killed")
            return False

        params = {"application_id": app_id}
        method = "kill_application"
        self._driver._call(method, **params)["state"]

        if has_completed(self.get_report(app_id)):
            logger.info(f"Killed application {app_id}")
            return True
        else:
            raise DriverError(f"Killing application {app_id} failed")
