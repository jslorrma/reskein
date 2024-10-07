#!/usr/bin/env python3
"""
hadoop/config.py
================

This module implements necessary parts of Hadoop core, HDFS, and YARN config
handling. It provides functionality for parsing Hadoop configuration files and
representing the configuration as a `HadoopConfig` object.

The module uses a caching mechanism to avoid redundant parsing of the same
configuration file. The cache is cleared if the modification time of the file
is changed. This is done by the `config_cache` decorator, which is applied to
the `HadoopConfig().get` method.

Example
-------

Here is an example of how to use the `HadoopConfig` class to retrieve a
configuration value:

```python
from reskein.hadoop.config import HadoopConfig

# Create a HadoopConfig object
config = HadoopConfig('/path/to/hadoop/config/files')

# Get a configuration value
value = config.get('yarn.resourcemanager.webapp.https.address')
```

You can also set the path to the Hadoop configuration files using an
environment variable. This can be useful if you want to avoid hardcoding the
path in your script. Here is an example:

```python
import os
from hadoop.config import HadoopConfig

# Set the path to the Hadoop configuration files
os.environ['HADOOP_CONF_DIR'] = '/path/to/hadoop/config/files'

# Create a HadoopConfig object
config = HadoopConfig()

# Get a configuration value
value = config.get('yarn.resourcemanager.webapp.https.address')
"""

from __future__ import annotations

__author__ = "jslorrma"
__maintainer__ = "jslorrma"
__email__ = "jslorrma@gmail.com"

# imports
import ipaddress
import os
import pathlib
import re
import xml.etree.ElementTree as ET
from functools import wraps
from typing import TYPE_CHECKING
from urllib.parse import urlparse

# module imports
from .. import PathType, logger
from ..common.utils import cache
from .get_config import from_cluster

if TYPE_CHECKING:
    from collections.abc import Callable

    from requests.auth import AuthBase


def config_cache(func: Callable):
    """
    A decorator that caches the results of a function and clears the cache if
    the relevant configuration file has been modified.

    Parameters
    ----------
    func : Callable
        The function to be decorated.

    Returns
    -------
    Callable
        The decorated function.
    """
    # Create a dictionary to store the modification times of the configuration files
    conf_files_ts = {}

    @wraps(func)
    def wrapper(conf_file, *args, **kwargs):
        # Check if the configuration file is already in the cache
        if conf_file not in conf_files_ts:
            # If the file is not in the cache, log its modification time
            conf_files_ts[conf_file] = conf_file.stat().st_mtime
        # If the file is in the cache, check if it has been modified since the last time it was cached
        elif conf_files_ts[conf_file] < conf_file.stat().st_mtime:
            # If the file has been modified, update its modification time in the cache
            conf_files_ts[conf_file] = conf_file.stat().st_mtime
            # And clear the cache of the function, because the configuration file has changed
            func.cache_clear()

        # Call the original function and return its result
        return func(conf_file, *args, **kwargs)

    return wrapper


@config_cache
@cache
def _parse_hadoop_config(config: PathType) -> dict[str, str]:
    """
    Parse a Hadoop configuration file and return a dictionary of configuration
    properties.

    This function reads an XML configuration file (such as `yarn-site.xml`),
    parses it into an XML tree, and then constructs a dictionary where the keys
    are the names of the properties and the values are the corresponding
    property values.

    Parameters
    ----------
    config : PathType
        The path to the Hadoop configuration file. This can be a string or a
        `pathlib.Path` object.

    Returns
    -------
    dict[str, str]
        A dictionary where the keys are the names of the properties and the
        values are the corresponding property values.
    """

    # read config and get tree root
    tree = ET.parse(str(config) if isinstance(config, pathlib.Path) else config)
    root = tree.getroot()

    # Construct list with profit values
    ph1 = [{el.tag: el.text for el in p} for p in root.findall("./property")]

    # Construct dict with property key values
    ph2 = {obj["name"]: obj["value"] for obj in ph1}

    return ph2


class HadoopConfig:
    """
    A singleton class to manage the Hadoop configuration.

    It provides methods to retrieve Hadoop configuration parameters from the
    Hadoop configuration files (`core-site.xml`, `yarn-site.xml`,
    `hdfs-site.xml`).

    This class is designed to be created just once and then employed
    consistently across your entire project or during the lifespan of a single
    Python runtime environment.

    Example
    -------
    Create a HadoopConfig instance using the configuration from a Hadoop
    cluster:

    ```python
    from reskein.hadoop.config import HadoopConfig

    # Instantiate HadoopConfig once using the config from a Hadoop cluster
    HadoopConfig.from_cluster(
        cluster_master_address='localhost',
        out_path='/path/to/save/config',
        name='my_cluster',
        auth=None
    )

    # Now, HadoopConfig() can be used everywhere in the project and the
    # Python runtime environment
    ```

    Create a HadoopConfig instance using the configuration files
    (`core-site.xml`, `yarn-site.xml`, `hdfs-site.xml`) stored in a local
    directory:

    ```python
    from reskein.hadoop.config import HadoopConfig

    # Instantiate HadoopConfig once using the configuration stored in a local
    # directory
    HadoopConfig(config_path='/path/to/hadoop/configfiles')
    ```

    Instead of providing the path to the configuration files when creating a
    HadoopConfig instance, you can also set the path using an environment
    variable.This is equivalent to the previous examples and after setting the
    environment variable, you can use HadoopConfig() everywhere in the project
    or the Python runtime environment:
    ```python
    # using an environment variable `HADOOP_CONF_DIR`
    import os
    os.environ['HADOOP_CONF_DIR'] = '/path/to/hadoop/configfiles'
    ```
    """

    _instance: HadoopConfig | None = None
    _config_path: PathType | None = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        elif args or kwargs:
            config_path = args[0] if args else kwargs.get("config_path")
            if cls._config_path != config_path:
                cls._config_path = config_path

        return cls._instance

    def __init__(self, config_path: PathType | None = None):
        """
        Initialize a HadoopConfig instance.

        Parameters
        ----------
        config_path : PathType | None, optional
            The path to the Hadoop configuration directory. If not provided,
            the path is determined by the `config_dir` property, which checks
            several environment variables (see *Note*) and defaults to
            `"/etc/hadoop/conf"` if none are set.

        Note
        ----
        The `config_dir` property is used within the class to find Hadoop
        configuration files. It checks the following environment variables in
        order: `"HADOOP_CONF_DIR"`, `"YARN_CONF_DIR"`, `"HDFS_CONF_DIR"`. If
        none of these environment variables are set, it defaults to
        `"/etc/hadoop/conf"`.
        """
        if config_path:
            self.set_config_path(config_path)

    @classmethod
    def from_cluster(
        cls,
        cluster_master_address: str,
        out_path: PathType | None = None,
        name: str | None = None,
        auth: AuthBase | None = None,
    ):
        """
        Create a new instance of this class using the Hadoop configuration from a Hadoop cluster.

        The configuration parameters are retrieved from the multiple web interfaces of the Hadoop
        cluster services.

        Parameters
        ----------
        cluster_master_address : str
            Hostname or IP of the Hadoop cluster's master node.
        out_path : str | pathlib.Path, optional
            Path to download the configuration files to. By default, it's `None`. If `None`,
            configuration files will be downloaded into `'~/.skein/hadoop_conf'`.
        name : str, optional
            The name of the cluster. By default, it's `None`.
        auth : requests.auth.AuthBase, optional
            An instance of `requests.auth.AuthBase` to be used for authentication. For an unsecured
            cluster that only requires a username, `reskein.auth.HTTPSimpleAuth` can be used. For
            secure kerberized clusters, `requests_kerberos.HTTPKerberosAuth` is required. Note that
            the user needs to have a valid Kerberos ticket beforehand. `reskein.utils.kerberos.kinit`
            can be used for this purpose.

        Returns
        -------
        cls
            An instance of this class with the Hadoop configuration parameters.

        Example
        -------
        ```python
        from reskein.hadoop.config import HadoopConfig

        # Create a new Config instance using the configuration from a Hadoop cluster
        HadoopConfig.from_cluster(
            cluster_master_address='localhost',
            out_path='/path/to/save/config',
            name='my_cluster',
            auth=None
        )
        ```
        """
        return cls(
            config_path=from_cluster(
                host=cluster_master_address, out_path=out_path, name=name, auth=auth
            )
        )

    @property
    def config_dir(self):
        """
        Get the Hadoop configuration directory.

        This property returns the path to the Hadoop configuration directory. It first checks if a
        path was provided when the HadoopConfig instance was created. If not, it checks the
        following environment variables in order: `"HADOOP_CONF_DIR"`, `"YARN_CONF_DIR"`,
        `"HDFS_CONF_DIR"`. If none of these environment variables are set, it defaults to
        `"/etc/hadoop/conf"`.

        Returns
        -------
        pathlib.Path
            The path to the Hadoop configuration directory.
        """
        return pathlib.Path(
            self._config_path
            or os.getenv("HADOOP_CONF_DIR")
            or os.getenv("YARN_CONF_DIR")
            or os.getenv("HDFS_CONF_DIR")
            or "/etc/hadoop/conf"
        )

    def set_config_path(self, config_path: PathType) -> bool:
        """
        Set the Hadoop configuration path.

        This method sets the path to the Hadoop configuration directory, where the
        `{core, yarn, hdfs}-site.xml` files can be found. It checks if these files exist in the
        provided path and raises a `FileNotFoundError` if any of them are not found. After setting
        the path, it clears the cache of the `_parse_hadoop_config` function.

        Parameters
        ----------
        config_path : PathType
            The path to the Hadoop configuration directory. This can be a string or a `pathlib.Path`
            object.

        Returns
        -------
        bool
            `True` if `{core, yarn, hdfs}-site.xml` files are found in the provided path.

        Raises
        ------
        FileNotFoundError
            Error raised if `core-site.xml`, `yarn-site.xml`, or `hdfs-site.xml` are not found in
            the provided path.
        """
        _path = (
            pathlib.Path(config_path).parent
            if "-site.xml" in str(config_path)
            else pathlib.Path(config_path)
        )

        if not (_path / "core-site.xml").exists():
            raise FileNotFoundError(f"'core-site.xml' not found in '{_path}'")
        if not (_path / "yarn-site.xml").exists():
            raise FileNotFoundError(f"'yarn-site.xml' not found in '{_path}'")
        if not (_path / "hdfs-site.xml").exists():
            raise FileNotFoundError(f"'hdfs-site.xml' not found in '{_path}'")

        logger.debug(f"Setting hadoop config path to '{_path}'")
        self._config_path = config_path

        return True

    def get(self, key: str) -> str:
        """
        Parse a Hadoop XML configuration file and return the value of a specified key.

        This method reads a Hadoop configuration file, parses it into an XML tree, and then
        retrieves the value of a specified key. If the value contains a reference to another key
        (in the format `${other_key}`), this method recursively retrieves the value of the other
        key.

        Parameters
        ----------
        key : str
            The name of the key to retrieve the value for.

        Returns
        -------
        str
            The value of the specified key in the Hadoop configuration file. If the key is not
            found, returns `None`. If the value of the key references another key, the value of the
            other key is returned.

        Raises
        ------
        FileNotFoundError
            If the specified configuration file does not exist in the Hadoop configuration directory.
        """
        if not list(self.config_dir.glob("*.xml")):
            raise FileNotFoundError(
                f"No config files found in the directory '{self.config_dir}'. "
                "You can specify the Hadoop configuration directory by using the "
                "`HadoopConfig().set_config_path` method or by setting one of the following "
                "environment variables: `HADOOP_CONF_DIR`, `YARN_CONF_DIR`, or `HDFS_CONF_DIR`."
            )

        def _get(key: str, default: str | None = None) -> str | None:
            """
            Searches for and get a given key in all Hadoop configuration files.
            """
            for _conf_file in self.config_dir.glob("*.xml"):
                # The `_parse_hadoop_config` function returns are chached, so if
                # it has been called before with the same arguments, it will return
                # the cached result
                if key in _parse_hadoop_config(_conf_file):
                    _value = _parse_hadoop_config(_conf_file)[key]
                    logger.debug(
                        f"Got key '{key}' with value '{_value}' from '{self.config_dir / _conf_file}'"
                    )
                    return _value

            return default

        _value = _get(key, None)

        # Note: hadoop config files can use value interpolation like,
        # <property>
        #     <name>yarn.resourcemanager.webapp.address</name>
        #     <value>${yarn.resourcemanager.hostname}:8088</value>
        # </property>
        if _value and "${" in _value:
            _sub = self.get(
                # use regex to get key
                re.findall(r"^\$\{(.*?)\}", _value)[0],
            )
            _value = re.sub(r"^\$\{(.*?)\}", _sub, _value)

        return _value

    @property
    def defaultfs(self) -> str | None:
        """
        Retrieves the default filesystem (fs) for Hadoop. This is typically specified in the Hadoop
        configuration or can be set via the `HADOOP_DEFAULT_FS` or `SKEIN_DEFAULT_FS` environment
        variables.
        """
        _defaultfs = (
            os.getenv("HADOOP_DEFAULT_FS")
            or os.getenv("SKEIN_DEFAULT_FS")
            or self.get("fs.defaultFS")
        )
        if _defaultfs:
            return _defaultfs

    @property
    def high_availability_enabled(self) -> bool:
        """
        Checks if high availability is enabled.
        This is determined by the `yarn.resourcemanager.ha.enabled` configuration key.
        """
        return self.get("yarn.resourcemanager.ha.enabled") in ("true", "1")

    @property
    def resource_manager_ids(self) -> list[str] | None:
        """
        Retrieves the ResourceManager IDs.
        This is determined by the `yarn.resourcemanager.ha.rm-ids` configuration key.
        """
        rm_ids = self.get("yarn.resourcemanager.ha.rm-ids")
        # try getting rm_ids from "yarn-site.xml"
        if not rm_ids:
            _yarn_site_cont = (self.config_dir / "yarn-site.xml").read_text()
            rm_ids = ",".join(
                set(
                    re.findall(
                        r"yarn.resourcemanager.webapp.https.address\.([a-zA-Z0-9]*)",
                        _yarn_site_cont,
                    )
                    + re.findall(
                        r"yarn.resourcemanager.webapp.address\.([a-zA-Z0-9]*)", _yarn_site_cont
                    )
                )
            )
        return rm_ids.split(",") if rm_ids else None

    @property
    def name_node_ids(self) -> list[str] | None:
        """
        Retrieves the NameNode IDs.
        This is determined by the `dfs.ha.namenodes.{_defaultfs}` configuration key.
        """
        _defaultfs = urlparse(self.defaultfs).hostname if self.defaultfs else None
        nn_ids = self.get(f"dfs.ha.namenodes.{_defaultfs}")
        return nn_ids.split(",") if nn_ids is not None else None

    @property
    def yarn_https_only(self) -> bool:
        """
        Determines if `HTTPS_ONLY` is the configured policy for `YARN`.
        This is determined by the `yarn.http.policy` configuration key.
        """
        http_policy = self.get("yarn.http.policy")
        if http_policy == "HTTPS_ONLY":
            return True
        return False

    @property
    def hdfs_https_only(self) -> bool:
        """
        Determines if `HTTPS_ONLY` is the configured policy for `WebHDFS`.
        This is determined by the `dfs.http.policy` configuration key.
        """
        http_policy = self.get("dfs.http.policy")
        if http_policy == "HTTPS_ONLY":
            return True
        return False

    @property
    def resource_manager_addresses(self) -> list[str] | None:
        """
        Retrieves the ResourceManager address(es).
        This is determined by the `yarn.resourcemanager.webapp.https.address` or
        `yarn.resourcemanager.webapp.address` configuration keys.
        """

        if self.yarn_https_only:
            _key = "yarn.resourcemanager.webapp.https.address"
            _scheme = "https"
        else:
            _key = "yarn.resourcemanager.webapp.address"
            _scheme = "http"

        # HA cluster return all RM nodes
        if self.resource_manager_ids:
            return [
                f"{_scheme}://{self.get(f'{_key}.{rm_id}')}" for rm_id in self.resource_manager_ids
            ]

        # Non-HA cluster
        elif self.get(_key):
            return [f"{_scheme}://{self.get(_key)}"]
        else:
            return None

    @property
    def name_node_webhdfs_addresses(self) -> list[str] | None:
        """
        Retrieves the NameNode WebHDFS address(es).
        This is determined by the `dfs.namenode.https-address.{_defaultfs}` or
        `dfs.namenode.http-address.{_defaultfs}` configuration keys.
        """

        _defaultfs = urlparse(self.defaultfs).hostname if self.defaultfs else None
        _scheme = "https" if self.yarn_https_only else "http"

        if self.hdfs_https_only:
            _key = (
                f"dfs.namenode.https-address.{_defaultfs}"
                if _defaultfs
                else "dfs.namenode.https-address"
            )
        else:
            _key = (
                f"dfs.namenode.http-address.{_defaultfs}"
                if _defaultfs
                else "dfs.namenode.http-address"
            )

        # HA cluster return all RM nodes
        if self.name_node_ids and self.get(f"{_key}.{self.name_node_ids[0]}"):
            return [f"{_scheme}://{self.get(_key + '.' + nn_id)}" for nn_id in self.name_node_ids]

        # Non-HA cluster
        elif self.get(_key):
            return [f"{_scheme}://{self.get(_key)}"]
        else:
            _defaultport = (
                (os.getenv("HDFS_HTTPS_PORT") or 9871)
                if self.yarn_https_only
                else (os.getenv("HDFS_HTTP_PORT") or 9870)
            )
            if isinstance(self.resource_manager_addresses, (list)):
                return [
                    f"{_address.rsplit(':',1)[0]}:{_defaultport}"
                    for _address in self.resource_manager_addresses
                ]
            else:
                return [f"{self.resource_manager_addresses.rsplit(':',1)[0]}:{_defaultport}"]

    @property
    def name_node_rpc_addresses(self) -> list[str] | None:
        """
        Retrieves the NameNode RPC address(es).
        This is determined by the `dfs.namenode.rpc-address.{_defaultfs}` configuration key.
        """

        _defaultfs = urlparse(self.defaultfs).hostname if self.defaultfs else None

        _key = (
            f"dfs.namenode.rpc-address.{_defaultfs}" if _defaultfs else "dfs.namenode.rpc-address"
        )

        # HA cluster return all RM nodes
        if self.name_node_ids and self.get(f"{_key}.{self.name_node_ids[0]}"):
            return [f"{self.get(_key + '.' + nn_id)}" for nn_id in self.name_node_ids]

        # Non-HA cluster
        elif self.get(_key):
            return [f"{self.get(_key)}"]
        else:
            _defaultport = os.getenv("HDFS_RPC_PORT") or 8020
            if isinstance(self.resource_manager_addresses, list):
                return [
                    f"{urlparse(_address).hostname}:{_defaultport}"
                    for _address in self.resource_manager_addresses
                ]
            else:
                return [f"{urlparse(self.resource_manager_addresses).hostname}:{_defaultport}"]

    @property
    def name_node_service(self) -> list[str] | None:
        """
        Retrieves the NameNode service key name.
        This is determined by the `dfs.namenode.rpc-address.{_defaultfs}` configuration key.
        """

        _defaultfs = urlparse(self.defaultfs).hostname if self.defaultfs else None

        if self.high_availability_enabled and _defaultfs:
            return f"ha-hdfs:{_defaultfs}"
        else:
            import socket

            def is_valid_ip(ip: str) -> bool:
                try:
                    ipaddress.ip_address(ip)
                    return True
                except ValueError:
                    return False

            return ",".join(
                [
                    f"{socket.gethostbyaddr(urlparse(f'dummy://{address}').hostname)[2][0]}:{urlparse(f'dummy://{address}').port}"
                    if is_valid_ip(urlparse(f"dummy://{address}").hostname)
                    else f"{urlparse(f'dummy://{address}').hostname}:{urlparse(f'dummy://{address}').port}"
                    for address in self.name_node_rpc_addresses
                ]
            )

    @property
    def resource_manager_service(self) -> str | None:
        """
        Retrieves the ResourceManager service key name.
        This is determined by the `yarn.resourcemanager.webapp.https.address` or
        `yarn.resourcemanager.webapp.address` configuration keys.
        """
        return ",".join(self.resource_manager_addresses)

    @property
    def is_kerberos_enabled(self) -> bool:
        """
        Checks if Kerberos authentication is enabled.
        This is determined by the `hadoop.security.authorization` and
        `hadoop.security.authentication` configuration keys.
        """

        _auth = (
            os.getenv("HADOOP_AUTH")
            if os.getenv("HADOOP_AUTH", "").lower() in ("kerberos", "simple")
            else None
        )

        if not _auth:
            if self.get("hadoop.security.authorization") == "true":
                _auth = self.get("hadoop.security.authentication")

        return _auth is None or _auth.lower() == "kerberos"

    @property
    def yarn_principal(self) -> str | None:
        """
        Retrieves the YARN Kerberos principal.
        This is determined by the `yarn.resourcemanager.principal` configuration key.
        """
        return self.get("yarn.resourcemanager.principal")

    @property
    def hdfs_principal(self) -> str | None:
        """
        Retrieves the HDFS Kerberos principal.
        This is determined by the `dfs.namenode.kerberos.principal` configuration key.
        """
        return self.get("dfs.namenode.kerberos.principal")

    @property
    def http_principal(self) -> str | None:
        """
        Retrieves the HTTP Kerberos principal.
        This is determined by the `dfs.namenode.kerberos.internal.spnego.principal` configuration
        key.
        """
        return self.get("dfs.namenode.kerberos.internal.spnego.principal")
