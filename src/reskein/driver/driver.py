#!/usr/bin/env python3
"""
diver/driver.py
===============

Python implementation of [Skein's JAVA based `Driver`][1] using [YARN
ResourceManager REST API][2] to gather cluster and application information as
well as to connect to and submit applications.

[1]: https://github.com/jcrist/skein/blob/master/java/src/main/java/com/anaconda/skein/Driver.java
[2]: https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
"""

from __future__ import annotations

__author__ = "jslorrma"
__maintainer__ = "jslorrma"
__email__ = "jslorrma@gmail.com"

# imports
import copy
import inspect
import os
import pathlib
from io import StringIO
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin, urlparse

import msgspec
import requests
from dotenv import dotenv_values
from fsspec.callbacks import TqdmCallback
from skein.core import _SKEIN_JAR
from skein.model import ApplicationSpec, File, Master, Security, Service

from .. import CONFIG_DIR
from ..common.auth import Authentication
from ..common.exceptions import (
    DriverConfigurationError,
    DriverConnectionError,
    DriverHTTPError,
    DriverProxyError,
    DriverSSLError,
    context,
)
from ..common.kerberos import kinit, principal_from_keytab
from ..common.logging import get_logger
from ..common.utils import cache
from ..hadoop.config import HadoopConfig, _parse_hadoop_config
from ..yarn.filesystem import HDFSFileSystem
from ..yarn.models.submit import (
    ApplicationSubmissionContext,
    LogAggregationContext,
)
from ..yarn.reosurcemanager import ResourceManager
from .services import get_hdfs_endpoint, get_rm_endpoint

if TYPE_CHECKING:
    from .. import PathType

# init logging
logger = get_logger(__package__)

# make sure the directory exists
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
"""The configuration directory for reskein."""

_DRIVER_ARGS_FILE = CONFIG_DIR / "driver"
"""The file containing the driver initialization arguments."""
_DRIVER_ENV_FILE = CONFIG_DIR / "driver.env"
"""The file containing the driver environment variables."""


class Driver:
    """Python skein `Driver` implementation using YARN's web services REST API to
    gather cluster and application informationas well as to connect to and schedule
    applications. In contrast to the `skein` implementation this `Driver` is
    written in Python and it's not necessary to be run in a seperate process.


    Parameters
    ----------
    rm_address : str, optional
        The `rm_address` parameter is the address of the ResourceManager HTTP(S) service.
        If not provided, the address is read from the `SKEIN_DRIVER_RM` environment
        variable or the `yarn-site.xml` configuration file. The directory path containing
        this configuration file can be set with the `YARN_CONF_DIR` or `HADOOP_CONF_DIR`
        environment variables, or with the `HadoopConfig().set_config_dir` method.
    hdfs_address : str, optional
        The `hdfs_address` parameter is the address of the (Web)HDFS HTTP(S) service. If
        not provided, the address is read from the `SKEIN_DRIVER_HDFS` environment
        variable or the `hdfs-site.xml` configuration file. The directory path
        containing this configuration file can be set with the `HDFS_CONF_DIR` or
        `HADOOP_CONF_DIR` environment variables, or with the
        `HadoopConfig().set_config_dir` method.
    auth : Authentication or str, optional
        The `auth` parameter is the authentication method used by the `Driver` to
        authenticate with the Hadoop cluster. It can be `Authentication.KERBEROS`
        `'kerberos'`, `Authentication.SIMPLE`, `'simple'`, or `None`. If not provided,
        the authentication method is read from the `SKEIN_DRIVER_AUTH` environment
        variable. If the environment variable is also not set, it will be read from the
        `core-site.xml` configuration file. The directory path containing this configuration
        file can be set with the `HADOOP_CONF_DIR` environment variable, or with the
        `HadoopConfig().set_config_dir` method.
    timeout : int, optional
        How many seconds to wait for the server to send data before giving up, by
        default `90`
    verify : bool
        Either a boolean, in which case it controls whether we verify the server's TLS
        certificate, or a string, in which case it must be a path to a CA bundle to use,
        by default to `True`
    proxies : Dict[str, str], optional
        Dictionary mapping protocol to the URL of the proxy, by default to `None`
    keytab : Union[str, pathlib.Path], optional
        Path to a keytab file to use for kerberos authentication of the `Driver`
        and the skein application. Can also be set with environment variable
        `SKEIN_KRB5_KEYTAB`. Can be `None`, for non-secure clusters. By default
        `None`.
    principal : str, optional
        The principal to use for kerberos authentication. If not given the
        principal will be taken from the environment variable
        `SKEIN_KRB5_PRINCIPAL` or the first entry in the `keytab`-file. By
        default `None`.
    user : str, optional
        The user name used for simple authentication, by default `None`. If not given
        the value of `SKEIN_USER_NAME` environment varibale or the current system
        user's username is used.
    log : PathType, bool, optional
        Sets the logging behavior. Values may be a path for logs to be written to, `True`
        to log to stdout/stderr, or `False` to turn off logging completely, by default
        `False`.
    log_level : str | int, optional
        The `Driver` log level. One of {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
        (from most to least verbose). If not provided, it will be fetched from the environment
        variable `"SKEIN_LOG_LEVEL"`. If the environment variable is also not set, it will default
        to `logging.INFO`.
    **kwargs
            Additional keyword arguments to pass to simple or kerberos `requests`-AuthHandler.
    """

    __slots__ = (
        "_timeout",
        "_verify",
        "_proxies",
        "_keytab",
        "_principal",
        "_user",
        "_auth",
        "_auth_handler",
        "_rm_address",
        "_hdfs_address",
        "_fs",
        "_rm",
        "_session",
        "_connected",
        "_kinit_cmd",
        "_defaultFS",
        "_from_global_driver"
    )

    _instance: Driver | None = None

    def __new__(cls, *args, **kwargs):
        """
        Create a new instance of the Driver class or return the global driver instance.

        This method creates a new instance of the Driver class. If the global driver has already
        been initialized, the global driver instance is returned. If the global driver has not
        been initialized, a new instance of the Driver class is created.
        """

        _global_driver_kwargs = read_driver()

        kwargs = {
            **_global_driver_kwargs,
            **kwargs,
        }

        if kwargs == _global_driver_kwargs and kwargs:
            logger.debug("loading global driver ...")
            if cls._instance is None:
                cls._instance  = super().__new__(cls)
            cls._instance._from_global_driver = True
            return cls._instance
        else:
            logger.debug("initializing new driver ...")
            _instance  = super().__new__(cls)
            _instance._from_global_driver = False
            return _instance


    def __init__(  # noqa: PLR0913
        self,
        rm_address: str | None = None,
        hdfs_address: str | None = None,
        auth: str | None = None,
        timeout: int | float | None = None,
        verify: bool | PathType | None = None,
        proxies: dict[str, str] | None = None,
        keytab: PathType | None = None,
        principal: str | None = None,
        user: str | None = None,
        log: PathType | bool | None = None,
        log_level: str | int | None = None,
        **kwargs,  # will be passed to request auth handler
    ):
        if not getattr(self, "_from_global_driver", False):
            self._setup_logging(log_level=log_level, log=log)
            self._setup_authentication(
                auth=auth, keytab=keytab, principal=principal, user=user, **kwargs
            )
            self._update_class_attributes(
                timeout=timeout,
                rm_address=rm_address,
                hdfs_address=hdfs_address,
                verify=verify,
                proxies=proxies,
            )

            # test connection only if a new driver is initialized
            self._test_connection()

    def _setup_logging(self, log_level: str | int | None, log: PathType | bool | None):
        """
        Set up logging for the Driver class.

        Parameters
        ----------
        log_level : str | int
            Logging level.
        log : PathType | bool
            Logging configuration.

        """
        # setup logging
        globals().update({"logger": get_logger(__package__, log_level=log_level, log=log)})

    def _setup_authentication(
        self,
        auth: str | None,
        keytab: PathType | None,
        principal: str | None,
        user: str | None,
        **kwargs,
    ):
        """
        Set up Kerberos for the Driver class.

        Parameters
        ----------
        auth : str
            Authentication method.
        keytab : PathType
            Path to the keytab file.
        principal : str
            Principal name.
        user : str
            User name.
        **kwargs : dict
            Additional keyword arguments for the request auth handler.

        Raises
        ------
        FileNotFoundError
            If the keytab file does not exist at the specified path.
        ValueError
            If the principal is specified but the keytab is not.
        """
        try:
            logger.debug("Setting up authentication...")

            # set user name
            self._user = (
                user or os.getenv("SKEIN_USER_NAME") or os.getenv("USER") or os.getenv("USERNAME")
            )
            logger.debug(f"User set to {self._user}")

            self._auth = Authentication(
                auth
                or os.getenv("SKEIN_DRIVER_AUTH")
                or ("kerberos" if HadoopConfig().is_kerberos_enabled else "simple")
            )
            logger.debug(f"Authentication set to {self._auth}")

            self._keytab = keytab or os.getenv("SKEIN_KRB5_KEYTAB")
            self._principal = principal or os.getenv("SKEIN_KRB5_PRINCIPAL")

            # kerberos init local ticket cache
            if self._keytab is not None:
                self._keytab = pathlib.Path(self._keytab).absolute()
                if not self._keytab.exists():
                    raise context.FileNotFoundError(f"keytab doesn't exist at '{self._keytab}'")

                self._principal = (
                    principal_from_keytab(self._keytab) if self._principal is None else self._principal
                )
                logger.debug(f"Principal set to {self._principal}")

            elif self._principal is not None:
                raise context.ValueError("Keytab must be specified for keytab login")

            if self._keytab and self._principal:
                # ensure valid tgt existence
                kinit(keytab=self._keytab)
                logger.debug("Kerberos ticket granted")

            # setup authentication
            if self._auth == Authentication.SIMPLE:
                # lazily import authentication handler
                from ..common.auth import HTTPSimpleAuth

                self._auth_handler = HTTPSimpleAuth(username=self._user, **kwargs)
                logger.debug("Simple authentication handler set up")
            else:  # Authentication.KERBEROS
                # lazily import authentication handler
                from ..common.auth import OPTIONAL, HTTPKerberosAuth

                self._auth_handler = HTTPKerberosAuth(
                    mutual_authentication=(kwargs.pop("mutual_authentication", None) or OPTIONAL),
                    sanitize_mutual_error_response=kwargs.pop("sanitize_mutual_error_response", None)
                    or False,
                    **kwargs,
                )
                logger.debug("Kerberos authentication handler set up")
        # handle exceptions caused by missing hdfs-site.xml or core-site.xml
        except FileNotFoundError as e:
            raise DriverConfigurationError(
                f"Failed to set up authentication. Configuration error: {e}"
            ) from e

    def _update_class_attributes(
        self,
        rm_address: str | None,
        hdfs_address: str | None,
        timeout: int | float,
        verify: bool | PathType,
        proxies: dict[str, str],
    ):
        """
        Update class attributes for the Driver class.

        Parameters
        ----------
        rm_address : str | None
            Address of the resource manager.
        hdfs_address : str | None
            Address of the HDFS.
        timeout : int | float
            Timeout duration.
        verify : bool | PathType
            Verification method or path.
        proxies : dict[str, str]
            Proxies to use.
        """

        # set session attributes
        self._verify = verify if verify is not None else True
        self._proxies = proxies
        logger.debug(f"Verify set to {self._verify}, proxies set to {self._proxies}")

        # update class attributes
        self._timeout = timeout or 90
        logger.debug(f"Timeout set to {self._timeout}")

        # create tmp session
        _session = requests.Session()
        _session.verify = self._verify
        _session.auth = self._auth_handler
        _session.proxies = self._proxies

        # set RessourceManager service address
        self._rm_address = (
            rm_address
            or os.getenv("SKEIN_DRIVER_RM")
            or get_rm_endpoint(
                session=_session, addresses=HadoopConfig().resource_manager_addresses
            )
            or HadoopConfig().resource_manager_addresses[-1]
        )
        logger.debug(f"Resource Manager address set to {self._rm_address}")

        # set hdfs service address
        self._hdfs_address = (
            hdfs_address
            or os.getenv("SKEIN_DRIVER_HDFS")
            or get_hdfs_endpoint(
                session=_session, addresses=HadoopConfig().name_node_webhdfs_addresses
            )
            or HadoopConfig().name_node_webhdfs_addresses[-1]
        )
        logger.debug(f"HDFS address set to {self._hdfs_address}")

    def _test_connection(self):
        """
        'Ping' service nodes to test connection.

        This method tests the connection to the ResourceManager and (Web)HDFS services.
        It raises a DriverConfigurationError if the service address for either service is None,
        or if there is an error in the connection to the service.

        Raises
        ------
        DriverConfigurationError
            If the ResourceManager service address or the (Web)HDFS service address is None,
            or if there is an error in the connection to the service.
        """
        logger.debug("Testing connection...")

        if self._rm_address is None:
            self._connected = False
            raise DriverConfigurationError(
                "RessourceManager service address is `None`. "
                "RessourceManager service address can be set with the `rm_address` "
                "parameter. If not set, the address is read from the "
                "`SKEIN_DRIVER_RM` environment variable or the `yarn-site.xml` "
                "configuration file. The directory path containing this "
                "configuration file can be set with the `YARN_CONF_DIR` or "
                "`HADOOP_CONF_DIR` environment variables, or with the "
                "`HadoopConfig().set_config_dir` method."
            )
        if self._hdfs_address is None:
            self._connected = False
            raise DriverConfigurationError(
                "(Web)HDFS service address is `None`. "
                "(Web)HDFS service address can be set with the `hdfs_address` "
                "parameter. If not set, the address is read from the "
                "`SKEIN_DRIVER_HDFS` environment variable or the `hdfs-site.xml` "
                "configuration file. The directory path containing this "
                "configuration file can be set with the `HDFS_CONF_DIR` or "
                "`HADOOP_CONF_DIR` environment variables, or with the "
                "`HadoopConfig().set_config_dir` method."
            )
        try:
            logger.debug(
                "Testing driver connection to RessourceManager at "
                f"{urlparse(self.rm._address).netloc} "
                f"and (Web)HDFS at {urlparse(self.fs.url).netloc}"
            )
            self.rm.cluster()
            self.fs.info("/tmp")
        except Exception as exc:
            self._connected = False
            _msg = exc.args[0]
            _rm_rul = urlparse(self._rm_address)

            if isinstance(exc, DriverHTTPError | DriverSSLError | DriverProxyError):
                _sn = "RessourceManager" if _rm_rul.geturl() in _msg else "(Web)HDFS"
                _msg = f"Testing driver connection to {_sn} service node returned: {_msg}"
            elif isinstance(exc, DriverConnectionError):
                _msg = f"Testing driver connection to {_sn} service node returned: {_msg}"
            else:
                _msg = f"Testing driver connection returned: {_msg}"

            raise DriverConfigurationError(_msg) from exc

        # if everything works as ecpected
        self._connected = True
        logger.debug("Connection test successful")

    @classmethod
    def start_global_driver( # noqa: PLR0913
        cls,
        rm_address: str | None = None,
        hdfs_address: str | None = None,
        auth: str | None = None,
        timeout: int | float | None = None,
        verify: bool | PathType | None = None,
        proxies: dict[str, str] | None = None,
        keytab: PathType | None = None,
        principal: str | None = None,
        user: str | None = None,
        log: PathType | bool | None = None,
        log_level: str | int | None = None,
        **kwargs,  # will be passed to request auth handler
        ) -> Driver:
        """
        Start the global driver or return the existing one.

        This method starts the global driver or returns the existing one if it has already been
        started. The global driver is a singleton instance of the Driver class that is shared
        across the entire Python process.

        Parameters
        ----------
        rm_address : str, optional
            The `rm_address` parameter is the address of the ResourceManager HTTP(S) service.
            If not provided, the address is read from the `SKEIN_DRIVER_RM` environment
            variable or the `yarn-site.xml` configuration file. The directory path containing
            this configuration file can be set with the `YARN_CONF_DIR` or `HADOOP_CONF_DIR`
            environment variables, or with the `HadoopConfig().set_config_dir` method.
        hdfs_address : str, optional
            The `hdfs_address` parameter is the address of the (Web)HDFS HTTP(S) service. If
            not provided, the address is read from the `SKEIN_DRIVER_HDFS` environment
            variable or the `hdfs-site.xml` configuration file. The directory path
            containing this configuration file can be set with the `HDFS_CONF_DIR` or
            `HADOOP_CONF_DIR` environment variables, or with the `HadoopConfig().set_config_dir`
            method.
        auth : Authentication or str, optional
            The `auth` parameter is the authentication method used by the `Driver` to authenticate
            with the Hadoop cluster. It can be `Authentication.KERBEROS` `'kerberos'`,
            `Authentication.SIMPLE`, `'simple'`, or `None`. If not provided, the authentication
            method is read from the `SKEIN_DRIVER_AUTH` environment variable. If the environment
            variable is also not set, it will be read from the `core-site.xml` configuration file.
            The directory path containing this configuration file can be set with the
            `HADOOP_CONF_DIR` environment variable, or with the `HadoopConfig().set_config_dir`
            method.
        timeout : int, optional
            How many seconds to wait for the server to send data before giving up, by default `90`
        verify : bool
            Either a boolean, in which case it controls whether we verify the server's TLS
            certificate, or a string, in which case it must be a path to a CA bundle to use,
            by default to `True`
        proxies : Dict[str, str], optional
            Dictionary mapping protocol to the URL of the proxy, by default to `None`
        keytab : Union[str, pathlib.Path], optional
            Path to a keytab file to use for kerberos authentication of the `Driver` and the skein
            application. Can also be set with environment variable `SKEIN_KRB5_KEYTAB`. Can be
            `None`, for non-secure clusters. By default `None`.
        principal : str, optional
            The principal to use for kerberos authentication. If not given the principal will be
            taken from the environment variable `SKEIN_KRB5_PRINCIPAL` or the first entry in the
            `keytab`-file. By default `None`.
        user : str, optional
            The user name used for simple authentication, by default `None`. If not given the value
            of `SKEIN_USER_NAME` environment varibale or the current system user's username is used.
        log : PathType, bool, optional
            Sets the logging behavior. Values may be a path for logs to be written to, `True` to log
            to stdout/stderr, or `False` to turn off logging completely, by default `False`.
        log_level : str | int, optional
            The `Driver` log level. One of {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'} (from
            most to least verbose). If not provided, it will be fetched from the environment variable
            `"SKEIN_LOG_LEVEL"`. If the environment variable is also not set, it will default to
            `logging.INFO`.
        **kwargs
            Additional keyword arguments to pass to simple or kerberos `requests`-AuthHandler.

        Returns
        -------
        Driver
            The global driver instance.
        """
        # Get the global driver start arguments
        frame = inspect.currentframe()
        # Get argument values of the current frame
        _start_args, _, _, _start_values = inspect.getargvalues(frame)


        # init arguments
        drv_kwargs = {
            # Add the current client's init arguments, excluding 'self' and None values
            **{
                arg: _start_values.get(arg)
                for arg in _start_args
                if _start_values.get(arg) is not None and arg != "cls"
            },
            # Add the kwargs, excluding None values
            **{
                arg: value
                for arg, value in _start_values.get("kwargs", {}).items()
                if value is not None
            },
        }

        # if "global driver" file exists and contains different init args
        try:
            # test connection
            gloabl_driver = cls(**drv_kwargs)
            # if Driver is successfully initialized and connection can be established
            # write / update "global driver" file
            write_driver(**drv_kwargs)
            write_driver_env_vars()
        except DriverConfigurationError as e:
            # remove "global driver" file
            cls.stop_global_driver()
            raise DriverConfigurationError(
                f"Failed to start driver with given configuration: {drv_kwargs}. {e}"
            ) from e

        return gloabl_driver

    @classmethod
    def stop_global_driver(cls):
        """
        Stop the global driver.

        This method stops the global driver and resets the global driver context.
        """
        Driver._instance = None
        clean_driver()


    @property
    @cache
    def rm(self) -> ResourceManager:
        """Get YARN RessourceManager REST API handler"""
        # initialize rm rest api
        return ResourceManager(
            address=self._rm_address,
            auth=self._auth_handler,
            timeout=self._timeout,
            verify=self._verify,
            proxies=self._proxies,
        )

    @property
    @cache
    def fs(self) -> HDFSFileSystem:
        """Get (Web)HDFS filesystem handler"""
        # initialize hdfs filesystem
        hdfs_url = urlparse(self._hdfs_address)
        self._fs = HDFSFileSystem(
            host=hdfs_url.hostname,
            port=hdfs_url.port,
            use_https=hdfs_url.scheme == "https",
            kerberos=(self._auth == "kerberos"),
            user=(self._user if self._auth == "simple" else None),
        )
        self._fs.session.verify = (
            str(self._verify) if isinstance(self._verify, pathlib.Path) else self._verify
        )
        if self._auth == "kerberos":
            self._fs.session.auth = self._auth_handler

        return self._fs

    # def __reduce__(self):
    #     return (type(self), (self._rm_address, self._hdfs_address, self._auth))

    def __repr__(self):
        return f"Driver<RM:{self._rm_address}, HDFS:{self._hdfs_address}>"

    # get defaultFS
    @property
    def defaultFS(self):
        """
        Get WebHDFS defaultFS.

        This property returns the default filesystem (defaultFS) for WebHDFS. If the
        defaultFS has not been set, it attempts to fetch it from the HadoopConfig(). If
        it's not available there, it tries to fetch it from the ResourceManager and then
        from WebHDFS.

        Returns
        -------
        str
            The default filesystem (defaultFS) for WebHDFS, or `None` if it could not be fetched.

        """
        if not hasattr(self, "_defaultFS"):
            logger.debug("Getting WebHDFS defaultFS...")
            if HadoopConfig().defaultfs:
                self._defaultFS = HadoopConfig().defaultfs
                logger.debug(f"DefaultFS set to {self._defaultFS} from HadoopConfig")
            else:
                # try getting from ResourceManager
                url = urljoin(self.rm._address, "/conf")
                try:
                    _conf = StringIO(
                        self.rm._session.request(
                            method="GET",
                            url=url,
                        ).text
                    )
                    self._defaultFS = _parse_hadoop_config(_conf).get("fs.defaultFS", None)
                    logger.debug(f"DefaultFS set to {self._defaultFS} from ResourceManager")
                except Exception:
                    logger.debug("Failed to get DefaultFS from ResourceManager")

                # try getting from WebHDFS
                if not self._defaultFS:
                    url = urljoin(self.fs.url, "/conf")
                    try:
                        _conf = StringIO(
                            self.fs.session.request(
                                method="GET",
                                url=url,
                            ).text
                        )
                        self._defaultFS = _parse_hadoop_config(_conf).get("fs.defaultFS", None)
                        logger.debug(f"DefaultFS set to {self._defaultFS} from WebHDFS")
                    except Exception:
                        logger.debug("Failed to get DefaultFS from WebHDFS")

        return self._defaultFS

    def _call(self, method: str, timeout: int | float | None = None, **kwargs):
        """
        Call a method on the ResourceManager.

        This method attempts to call a method on the ResourceManager. If the method call fails,
        it raises the exception.

        Parameters
        ----------
        method : str
            The name of the method to call on the ResourceManager.
        timeout : int | float, optional
            The timeout duration for the method call. If not provided, the default timeout
            duration will be used.
        **kwargs : dict
            Additional keyword arguments for the method call.

        Raises
        ------
        Exception
            If the method call fails.

        Returns
        -------
        Any
            The result of the method call.

        """
        logger.debug(f"Calling method {method} with timeout {timeout} and kwargs {kwargs}")
        try:
            return getattr(self.rm, method)(timeout=timeout, **kwargs)
        except Exception as _exc:
            logger.exception(f"Exception occurred while calling method {method}: {_exc}")

    @cache
    def _get_app_dir(self, appId: str, staging_dir: str | None = None) -> str:
        """
        Get the Skein application directory.

        This method returns the directory for the specified Skein application. If the staging
        directory is not provided, it defaults to the `.skein` directory in the user's home
        directory on the filesystem.

        Parameters
        ----------
        appId : str
            The ID of the Skein application.
        staging_dir : str, optional
            The staging directory for the application. If not provided, it
            defaults to the `.skein` directory in the user's home directory on
            the filesystem.

        Returns
        -------
        str
            The directory for the specified Skein application.

        """
        _staging_dir = staging_dir or f"{self.fs.home_directory()}/.skein"
        return f"{_staging_dir}/{appId}"

    def submit_application(
        self, spec: ApplicationSpec, staging_dir: str | None = None, **kwargs
    ) -> str:
        """
        Submit a Skein application.

        This method submits a Skein application with the specified application specification.
        It returns the ID of the submitted application.

        Parameters
        ----------
        spec : ApplicationSpec
            A description of the application as a Skein `ApplicationSpec`.
        staging_dir : str, optional
            The staging directory for the application. If not provided, it
            defaults to the `.skein` directory in the user's home directory on
            the filesystem.
        tokens : dict[str, str], optional
            Tokens that you wish to pass to your application, specified as key-value
            pairs. The key is an identifier for the token and the value is the
            token (which should be obtained using the respective web-services)
        secrets : dict[str, str], optional
            Secrets that you wish to use in your application, specified as key-value
            pairs. The key is an identifier and the value is the base-64 encoding
            of the secret
        log_include_pattern : Union[str, List[str]], optional
            The log files which match the defined regex include pattern will be uploaded
            when the applicaiton finishes
        priority : int, optional
            The priority of the application, by default `0`
        unmanaged_AM : bool, optional
            Is the application using an unmanaged application master, by default
            `False`.
        application_type : str, optional
            The application type, by default `'skein'`
        **kwargs : dict, optional
            Additional keyword arguments.

        Raises
        ------
        Exception
            If the application submission fails.

        Returns
        -------
        str
            The ID of the submitted application.
        """
        logger.debug("Submitting application...")

        # get new app id
        app_id = self.rm.new_application()["application-id"]
        logger.debug(f"New application ID: {app_id}")

        # get app dir path
        app_dir = self._get_app_dir(app_id, staging_dir=staging_dir)
        logger.debug(f"App directory: {app_dir}")

        logger.info(f"Submitting application '{app_id}' ...")
        app_context = self._make_app_sumbission_context(spec, app_id, app_dir, **kwargs)

        try:
            self.rm.submit_application(app_context.to_dict())
        except Exception as e:
            # delete app dir if submission fails
            self._delete_app_dir(app_dir)
            logger.exception(f"Application submission failed, app directory deleted. Error: {e}")

        logger.info(f"Application '{app_id}' submitted ...")
        return app_id

    def _make_app_sumbission_context(
        self,
        spec: ApplicationSpec,
        app_id: str,
        app_dir: str | None = None,
        prologue: str = "",
        epilogue: str = "",
        **kwargs,
    ) -> ApplicationSubmissionContext:
        """
        Create an application submission context from a Skein application specification.

        Parameters
        ----------
        spec : ApplicationSpec
            A description of an application as a Skein `ApplicationSpec`.
        app_id : str
            The application ID.
        app_dir : str | None, optional
            The Skein application directory. If `None`, the `setup_app_dir` step is skipped,
            by default `None`.
        prologue : str, optional
            Custom command(s) to execute before starting the application master, by default `""`.
        epilogue : str, optional
            Custom command(s) to execute after starting the application master, by default `""`.
        **kwargs : dict, optional
            Additional keyword arguments.

        Returns
        -------
        ApplicationSubmissionContext
            The application submission context.

        Raises
        ------
        ValueError
            If keytab is not specified when submitting a Skein application to a secured cluster.
        """
        from . import _SKEIN_LOG_DIR_EXPANSION_VAR

        logger.debug("Creating application submission context...")

        # use a copy of spec
        spec = copy.deepcopy(spec)

        # master env vars
        env = spec.master.env
        env["SKEIN_APPLICATION_ID"] = app_id
        env["LANG"] = os.getenv("LANG", "en_US.UTF-8")

        log4jConfig = (
            "-Dlog4j.configuration=file:./.skein.log4j.properties "
            if spec.master.log_config is not None
            else ""
        )

        _tokens = kwargs.pop("tokens", {})
        # handle authentication method and setup kerberos if used
        if self._auth == Authentication.KERBEROS:
            # collect delegation tokens
            _tokens = _tokens | self._collect_tokens()

            # update keytab and principal if provided via kwargs
            self._keytab = kwargs.get("keytab") or self._keytab or os.getenv("SKEIN_KRB5_KEYTAB")
            self._principal = (
                kwargs.get("principal") or self._principal or os.getenv("SKEIN_KRB5_PRINCIPAL")
            )

            if not self._keytab:
                raise context.ValueError(
                    "Keytab must be specified to submit skein application to secured cluster."
                )
            elif not self._principal:
                self._principal = principal_from_keytab(self._keytab)

            _kinit_cmd = f"kinit -kt {pathlib.Path(self._keytab).name} {self._principal}"
        else:
            env["HADOOP_USER_NAME"] = self._user
            _kinit_cmd = "echo 'simple authentication used. No kinit required.'"

        # Build master command to start application master
        master_cmd = "\n".join(
            (
                inspect.cleandoc(
                    f"""
                    # add working dir and skein.jar to CLASSPATH
                    export CLASSPATH={{PWD}}<CPS>.skein.jar<CPS>$(yarn classpath --glob)

                    # kinit if secure cluster
                    echo '-------------- KINIT --------------' > {_SKEIN_LOG_DIR_EXPANSION_VAR}/application.master.startup.log
                    {_kinit_cmd} >> {_SKEIN_LOG_DIR_EXPANSION_VAR}/application.master.startup.log
                    """
                ),
                "# custom command(s) to execute before starting the application master",
                inspect.cleandoc(
                    prologue.format(
                        log_file=f"{_SKEIN_LOG_DIR_EXPANSION_VAR}/application.master.prologue.log"
                    )
                    if "{log_file}" in prologue
                    else prologue
                ),
                inspect.cleandoc(
                    rf"""
                    # start application master
                    # -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5007
                    ${{JAVA_HOME:-/usr}}/bin/java -Xmx128M {log4jConfig} \
                        -Dskein.log.level={spec.master.log_level} \
                        -Dskein.log.directory={_SKEIN_LOG_DIR_EXPANSION_VAR} \
                        com.anaconda.skein.ApplicationMaster {app_dir} > {_SKEIN_LOG_DIR_EXPANSION_VAR}/application.master.log 2>&1
                """
                ),
                "# custom command(s) to execute after starting the application master",
                inspect.cleandoc(
                    epilogue.format(
                        log_file=f"{_SKEIN_LOG_DIR_EXPANSION_VAR}/application.master.epilogue.log"
                    )
                    if "{log_file}" in epilogue
                    else epilogue
                ),
            )
        )

        # setup remote app dir, upload and finalize local resources for master and services
        if app_dir:
            self._setup_app_dir(spec, app_dir)

        # return spec
        # am_context = AMContainer.create(
        #     commands=master_cmd,
        #     local_resources=spec.master.files,
        #     application_acls=spec.acls.to_dict(),
        #     environment=env,
        #     tokens=None,  # tokens if self._auth == Authentication.KERBEROS else None,
        # )
        return ApplicationSubmissionContext.from_skein(
            application_id=app_id,
            spec=spec,
            application_type=kwargs.pop("application_type", "skein"),
            application_tags=spec.tags,
            log_aggregation_context=LogAggregationContext.create(
                log_include_pattern=".*",
                log_aggregation_policy_class_name=(
                    "org.apache.hadoop.yarn.server.nodemanager.containermanager."
                    "logaggregation.AllContainerLogAggregationPolicy"
                ),
            ),
            # collect delegation tokens
            tokens=_tokens,
            **kwargs,
        ).with_commands(master_cmd)

    def _collect_tokens(self):
        """
        Collect ResourceManager and (Web)HDFS delegation tokens.

        This method collects delegation tokens from the ResourceManager and (Web)HDFS. Currently,
        only the HDFS delegation token is collected due to an issue with the ResourceManager
        delegation token.

        Returns
        -------
        dict
            A dictionary containing the delegation tokens. The keys are the service names and the
            values are the delegation tokens.

        Notes
        -----
        The ResourceManager delegation token is not currently collected due to an issue where the
        token on get request via YARN REST API has an empty service value and the token renewal
        fails with 'Does not contain a valid host:port authority'.
        """
        _tokens = {}
        # the token renewer should be yarn
        _renewer = "yarn"
        # get RM_DELEGATION_TOKEN
        # FIXIT: The delegation token on get request via YARN REST API has an
        #        empty service value and the token renewal fails with
        #        'Does not contain a valid host:port authority'
        # _rm_delegation_token = self.rm.delegation_token(_renewer)

        # get HDFS_DELEGATION_TOKEN
        _nn_service = HadoopConfig().name_node_service
        _tokens[_nn_service] = self.fs.get_delegation_token(
            _renewer, _nn_service, "HDFS_DELEGATION_TOKEN"
        )["urlString"]

        return _tokens or None

    def _setup_app_dir(self, spec: ApplicationSpec, app_dir: str, **kwargs):
        """
        Set up the remote Skein application directory and handle local resources.

        This method creates the Skein application directory and uploads the application resources
        to it. It also finalizes the master and services for the application.

        Parameters
        ----------
        spec : ApplicationSpec
            A description of the application as a Skein `ApplicationSpec`.
        app_dir : str
            The Skein application directory.
        **kwargs : dict, optional
            Additional keyword arguments.
        """
        from . import _SKEIN_DIR_PERM

        # Make the ~/.skein/app_id dir
        logger.debug(f"Uploading application resources to '{app_dir}'")
        self.fs.mkdirs(app_dir, exist_ok=True)
        self.fs.chmod(app_dir, _SKEIN_DIR_PERM)

        # Create LocalResources for the cert/pem files, and add them to the
        # security object.
        if not spec.master.security:
            spec.master.security = Security.from_default()

        #
        # MASTER
        #
        self._finalize_master(master=spec.master, app_dir=app_dir, **kwargs)

        #
        # SERVICES
        #
        for entry in spec.services:
            self._finalize_service(
                app_dir=app_dir,
                service_name=entry,
                service=spec.services[entry],
                cert_file=spec.master.security.cert_file,
                key_file=spec.master.security.key_file,
                keytab_file=(
                    spec.master.files[f"{pathlib.Path(self._keytab).name}"]
                    if self._auth == Authentication.KERBEROS
                    else None
                ),
                **kwargs,
            )

        # Write the application protobuf specification to file
        logger.debug(f"Writing application specification to '{app_dir}/.skein.proto'")
        # make .skein.proto
        proto_path = f"{self.defaultFS}{app_dir}/.skein.proto"
        with self.fs.open(proto_path, "wb") as _proto_file:
            _proto_file.write(spec.to_protobuf().SerializeToString())
        # upload .skein.proto file
        spec.master.files[".skein.proto"] = self._finalize_local_resource(
            app_dir, File(proto_path, "FILE"), False
        )

    def _finalize_master(self, master: Master, app_dir: str, **kwargs):
        """
        Handle master by uploading local resources and writing application specification to file.

        This method uploads local resources for the master to the Skein application directory and
        writes the application specification to a file. It also handles the master's security
        settings and additional files.

        Parameters
        ----------
        master : Master
            A description of the master as a Skein `Master`.
        app_dir : str
            The Skein application directory.
        **kwargs : dict, optional
            Additional keyword arguments.
        """

        # upload local resources from spec for master to app_dir
        for resource in master.files:
            self._finalize_local_resource(
                app_dir=app_dir,
                local_resource=master.files[resource],
                hash=True,
                **kwargs,
            )

        # define additional master files to be uploaded and added to spec
        master_files = [
            (".skein.jar", File(_SKEIN_JAR, "FILE")),
            (".skein.log4j.properties", master.log_config),
            (".skein.crt", master.security.cert_file),
            (".skein.pem", master.security.key_file),
        ]

        # add keytab to master files if kerberos is used
        if self._auth == Authentication.KERBEROS:
            master_files.append(
                (f"{pathlib.Path(self._keytab).name}", File(f"{self._keytab}", "FILE"))
            )

        # upload and finalize master files to app_dir
        for file_name, local_resource in master_files:
            if local_resource:
                master.files[file_name] = self._finalize_local_resource(
                    app_dir=app_dir, local_resource=local_resource, hash=False
                )

    def _finalize_service(  # noqa: PLR0913
        self,
        app_dir: str,
        service_name: str,
        service: Service,
        cert_file: File,
        key_file: File,
        keytab_file: File = None,
        **kwargs,
    ):
        """
        Handle service by creating and uploading service shell script and adding and uploading
        security files.

        This method creates a shell script for the service, uploads it to the Skein application
        directory, and adds it to the service's files. It also uploads the certificate and key
        files, and the keytab file if Kerberos authentication is used.

        Parameters
        ----------
        app_dir : str
            The Skein application directory.
        service_name : str
            The name of the service.
        service : Service
            The service.
        cert_file : File
            The certificate file.
        key_file : File
            The key file.
        keytab_file : File, optional
            The keytab file. Only required if Kerberos authentication is used.
        **kwargs : dict, optional
            Additional keyword arguments.
        """
        from . import _SKEIN_LOG_DIR_EXPANSION_VAR

        # Add LANG if present
        lang = os.getenv("LANG")
        if lang:
            service.env["LANG"] = lang

        # upload local resources from spec for service to app_dir
        for resource in service.files:
            self._finalize_local_resource(
                app_dir=app_dir,
                local_resource=service.files[resource],
                hash=True,
                **kwargs,
            )

        # Write the service script to file
        script_path = f"{app_dir}/{service_name}.sh"
        logger.debug(f"Writing service script for '{service_name}' to '{script_path}'")

        # make service script
        with self.fs.open(script_path, "wb") as scriptf:
            scriptf.write(service.script.encode())

        # Upload service script
        service.files[".skein.sh"] = self._finalize_local_resource(
            app_dir=app_dir, local_resource=File(script_path, "FILE"), hash=False, **kwargs
        )

        # add cert files
        service.files[".skein.crt"] = cert_file
        service.files[".skein.pem"] = key_file

        # add keytab to master files if kerberos is used
        if self._auth == Authentication.KERBEROS:
            service.files[f"{pathlib.Path(self._keytab).name}"] = keytab_file

            _kinit_cmd = f"kinit -kt {pathlib.Path(self._keytab).name} {self._principal}"
        else:
            _kinit_cmd = "echo 'simple authentication used. No kinit required.'"

        # Build command to execute script and set as new script
        service.script = inspect.cleandoc(
            f"""
            echo 'Starting service {service_name}...' > {_SKEIN_LOG_DIR_EXPANSION_VAR}/{service_name}.startup.log

            # kinit if secure cluster
            echo '-------------- KINIT --------------' >> {_SKEIN_LOG_DIR_EXPANSION_VAR}/{service_name}.startup.log
            {_kinit_cmd} >> {_SKEIN_LOG_DIR_EXPANSION_VAR}/{service_name}.startup.log

            # service script
            bash .skein.sh >> {_SKEIN_LOG_DIR_EXPANSION_VAR}/{service_name}.log 2>&1
            """
        )

    def _upload_file(self, source: str, dest: str, progress: bool):
        """
        Upload a file to the destination filesystem.

        This method uploads a file from the source path to the destination path on the filesystem.
        If the source file is on the HDFS filesystem, it is copied to the destination. Otherwise,
        it is uploaded to the destination. If the `progress` parameter is `True`, a progress bar
        is displayed during the upload.

        Parameters
        ----------
        source : str
            The path to the source file.
        dest : str
            The path to the destination file.
        progress : bool
            If `True`, a progress bar is displayed during the upload.

        Notes
        -----
        The file permissions of the uploaded file are set to `_SKEIN_FILE_PERM`.
        """
        from . import _SKEIN_FILE_PERM

        if "hdfs://" in source:
            logger.debug(f"Copying '{source}' to '{dest}'")
            self.fs.copy(source, dest)
        else:
            logger.debug(f"Uploading '{source}' to '{dest}'")
            self.fs.put_file(
                source.replace("file://", ""),
                dest,
                callback=TqdmCallback(
                    tqdm_kwargs={
                        "desc": f"Uploading '{pathlib.Path(source).name}'",
                        "ascii": True,
                        "disable": not progress,
                    }
                ),
            )
        self.fs.chmod(dest, _SKEIN_FILE_PERM)

    def _finalize_local_resource(
        self, app_dir: str, local_resource: File, hash: bool = False, **kwargs
    ) -> File:
        """
        Upload resources to `app_dir` if not already exists and adjust resource details accordingly.

        Parameters
        ----------
        app_dir : str
            The directory where the application resources will be uploaded.
        local_resource : File
            The local resource file to be uploaded.
        hash : bool, optional
            If True, a hash of the local resource source is used as a prefix for the destination
            directory, by default `False`.
        **kwargs
            Arbitrary keyword arguments. Currently, only "file_progress" is used.

        Returns
        -------
        File
            The updated local resource with the new source, size, and timestamp.

        Notes
        -----
        If the destination file already exists on the filesystem, it is not uploaded again.
        The source of the local resource is updated to the path of the uploaded file on the
        destination filesystem. The size and timestamp of the local resource are updated to
        match those of the uploaded file.
        """

        # get filename
        _src_filename = pathlib.Path(local_resource.source).name

        # get destination directory
        if hash:
            # use hash of source as prefix for destination directory
            import hashlib

            prefix = hashlib.md5(local_resource.source.encode()).hexdigest()
            dest_dir = f"{app_dir}/{prefix}"
        else:
            dest_dir = app_dir

        # construct final destination path
        dest_path = f"{dest_dir}/{_src_filename}"

        # check if file already exists and if not upload it
        if not self.fs.exists(dest_path):
            # File needs to be uploaded to the destination filesystem
            _progress = kwargs.get("file_progress") or False

            # create remote destination folder if it does not exist
            self.fs.makedirs(path=dest_dir, exist_ok=True)

            # upload file
            self._upload_file(source=local_resource.source, dest=dest_path, progress=_progress)

        # finally, update local resource details
        _info = self.fs.info(dest_path)
        logger.debug(
            f"Updating local resource source from: '{local_resource.source}' to '{self.defaultFS}{dest_path}'"
        )
        local_resource.source = f"{self.defaultFS}{dest_path}"
        local_resource.size = _info["size"]
        local_resource.timestamp = _info["modificationTime"]

        return local_resource

    def _delete_app_dir(self, app_dir: str):
        """
        Delete the Skein application directory.

        This method deletes the Skein application directory if it exists. If the deletion fails,
        it raises the exception.

        Parameters
        ----------
        app_dir : str
            The Skein application directory.

        Raises
        ------
        Exception
            If the deletion of the application directory fails.

        """
        try:
            if self.fs.exists(app_dir):
                self.fs.delete(app_dir, recursive=True)
                logger.debug(f"Deleted application directory '{app_dir}'")
        except Exception:
            logger.debug(f"Failed to delete application directory '{app_dir}'")
            raise


def read_driver() -> dict[str, Any]:
    """
    Reads the driver file and returns the data from the file as a dictionary.

    Returns
    -------
    dict
        A dictionary containing the data from the driver file or an empty dictionary.
    """
    logger.debug("Reading driver init file...")
    # Read the driver file
    try:
        with _DRIVER_ARGS_FILE.open("rb") as fil:
            drv_kwargs = msgspec.yaml.decode(fil.read()) or {}

        logger.debug(f"Successfully read driver init file: {drv_kwargs}")

        return drv_kwargs
    except FileNotFoundError:
        logger.debug("Failed to read driver init file, returning no init args...")
        return {}

def write_driver(**kwargs):
    """
    Writes the given keyword arguments to the driver file using YAML.

    Parameters
    ----------
    **kwargs
        The data to write to the driver file.
    """
    logger.debug(f"Writing to driver init file with kwargs: {kwargs}")

    # Ensure the config dir exists
    _DRIVER_ARGS_FILE.parent.mkdir(parents=True, exist_ok=True)

    # Write to the driver file
    with _DRIVER_ARGS_FILE.open("wb") as fil:
        fil.write(msgspec.yaml.encode(kwargs))

def clean_driver():
    """
    Clean the driver file.
    """
    logger.debug("Cleaning driver files ...")
    # delete driver file
    _DRIVER_ARGS_FILE.unlink(missing_ok=True)
    _DRIVER_ENV_FILE.unlink(missing_ok=True)

def load_driver_env_vars():
    """
    Load the environment variables from the `.env` file in the global
    configuration directory.

    Returns
    -------
    dict
        A dictionary containing the environment variables from the `.env` file.
    """
    logger.debug("Loading environment variables from .env file...")
    # parse environment variables from local and global .env file
    _env_vars = dotenv_values(_DRIVER_ENV_FILE) | dotenv_values()

    # set environment variables
    for k, v in _env_vars.items():
        if k in os.environ:
            continue
        if v is not None:
            os.environ[k] = v

def write_driver_env_vars(
    update: bool = True,
    **kwargs,
):
    """
    Write environment variables to the `.env` file in the global
    configuration directory.

    Parameters
    ----------
    **kwargs
        The environment variables to write to the `.env` file.
    """
    if kwargs:
        _env_vars = dotenv_values(_DRIVER_ENV_FILE) | kwargs
    else:
        # load and update environment variables
        _env_vars = dotenv_values(_DRIVER_ENV_FILE) | {
            env_key: os.getenv(env_key)
            for env_key in os.environ
            if any(key in env_key for key in ("SKEIN", "HADOOP", "KRB5", "YARN", "HDFS"))
        }
    logger.debug(f"Writing environment variables to .env file with kwargs: {_env_vars}")

    # Write out env vars to the .env file
    with _DRIVER_ENV_FILE.open("w") as f:
        for k, v in _env_vars.items():
            f.write(f"{k}={v}\n")
