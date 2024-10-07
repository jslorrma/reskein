#!/usr/bin/env python3
"""
hadoop/get_config.py
====================

This module is responsible for retrieving the Hadoop configuration from a Hadoop
cluster. It provides a function `from_cluster` that fetches the configuration
parameters from the web interfaces of the Hadoop cluster services. The
configuration parameters include `core-site.xml`, `yarn-site.xml`, and
`hdfs-site.xml`. The function allows for specifying the host of the Hadoop
cluster's master node, an optional path to download the configuration files, an
optional name for the cluster, and an optional authentication method.

Example
-------

```python
from hadoop.get_config import from_cluster

# Retrieve configuration from a Hadoop cluster
from_cluster(
    host='localhost',
    out_path='/path/to/save/config',
    name='my_cluster',
    auth=None
)
```
"""

from __future__ import annotations

__author__ = "jslorrma"
__maintainer__ = "jslorrma"
__email__ = "jslorrma@gmail.com"

# imports
import pathlib
import warnings
import xml.etree.ElementTree as ET
from io import StringIO

import requests

# module imports
from .. import PathType, logger

# PORTS to scan
_PORTS = [8042, 9870, 9871, 50070, 50470, 8088, 9864, 9865, 50075, 50475]

# HADOOP config types
CONFIG_TYPES = ["core", "yarn", "hdfs"]


def from_cluster(
    host: str | list[str],
    out_path: PathType | None = None,
    name: str | None = None,
    auth: requests.auth.AuthBase | None = None,
    **kwargs,
):
    """
    Get Hadoop configuration (`core-site.xml`, `yarn-site.xml`, `hdfs-site.xml`) from a Hadoop
    cluster.

    The configuration parameters are retrieved from the multiple web interfaces of the Hadoop
    cluster services.

    Parameters
    ----------
    host : str | list[str]
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
    dict
        A dictionary containing the Hadoop configuration parameters.

    Example
    -------
    ```python
    from hadoop.get_config import from_cluster

    # Retrieve configuration from a Hadoop cluster
    from_cluster(
        host='localhost',
        out_path='/path/to/save/config',
        name='my_cluster',
        auth=None
    )
    ```
    """
    # output path
    _name = f"{name}/" if name else ""
    _out_path = pathlib.Path(out_path) or (pathlib.Path.home() / f".skein/hadoop_conf/{_name}")

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
        _out_path.mkdir(parents=True, exist_ok=True)
        root.write(str(_out_path / filename), encoding="utf-8")
        logger.info(f"'{filename}' successfully created in '{_out_path}'")

    # Build xyz-site.xml file
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

    def _request(port: int) -> str:
        """
        Sends a GET request to the specified host and port to retrieve the Hadoop configuration.
        """
        _host = host.rstrip("/") if "://" in host else f"http://{host.rstrip('/')}"
        url = f"{_host}:{port}/conf"

        logger.info(f"Trying to get Hadoop config from '{_host}' on port '{port}'")
        with requests.get(url, auth=auth, **kwargs) as resp:
            resp.raise_for_status()
            logger.info(f"Successfully retrieved Hadoop config from '{_host}' on port '{port}'")
            return resp.text

    for port in _PORTS:
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=requests.exceptions.RequestsWarning)
                config = StringIO(_request(port))
        except requests.RequestException:
            pass

        for config_type in CONFIG_TYPES:
            _create(config, config_type)

        return _out_path
