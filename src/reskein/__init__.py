#!/usr/bin/env python3
"""
Reskein
=======

`Reskein` is an application and library that that adjusts and uses jcrist's
[Skein][1] library to simplify the deployment of applications on Apache Hadoop
cluster using [YARN ResourceManager REST API][2].

Features:
---------
- **Driver Implementation**: The `Driver` is implemented in Python using Apache
  Hadoop's YARN' ResourceManager REST API, as opposed to the traditional Java
  implementation using Apache Hadoop's gRPC-based Java API.
- **No Java Requirement**: There's no need to install Java or set up a Java
  Virtual Machine (JVM). Additionally, the `Driver` doesn't need to run in a
  separate thread or process.
- **Driver Initialization**: The `Driver` is initialized inside the `Client`,
  enhancing efficiency and ease of use.

[1]: https://github.com/jcrist/skein
[2]: https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
"""

from __future__ import annotations

__author__ = "jslorrma"
__maintainer__ = "jslorrma"
__email__ = "jslorrma@gmail.com"


# version
try:
    from ._version import __version__, __version_tuple__, version
except ImportError:
    __version__ = version = "0.0.0"
    __version_tuple__ = (0, 0, 0)

import pathlib
import sys

LIBRARY_NAME = __name__

# set up logging
from .common.logging import get_logger

logger = get_logger(__name__)

# type definitions
PathType = str | pathlib.Path

# patching skein

# 'fnctl' module gets imported in 'skein\utils.py', but is unix / osx only ->
# patch global module import with local imported empty
if sys.platform.startswith("win"):
    from .common import fcntl

    sys.modules["fcntl"] = fcntl

    from skein.utils import _FileLock

    def _acquire_file(self):
        if self._file is None:
            self._file = open(self._path, "wb")
        fcntl.lock(self._file, fcntl.LockFlags.EXCLUSIVE)

    def _release_file(self):
        fcntl.unlock(self._file)
        self._file.close()
        self._file = None

    _FileLock._acquire_file = _acquire_file
    _FileLock._release_file = _release_file


# patching old skein versions missing some
# function and class definitions
import skein.model

if not hasattr(skein.model, "ApplicationLogs"):
    from ._patches import ApplicationLogs

    skein.model.ApplicationLogs = ApplicationLogs

# monkey patch skein.model.File's _normpath to properly
# handle windows paths
from ._patches import _normpath

skein.model.File._normpath = _normpath

# monkey patch skein.core.Client
import skein

from .client import Client

skein.Client = Client
skein.core.Client = Client

# specify the configuration directory
from skein.core import properties  # noqa: I001
CONFIG_DIR = (
    pathlib.Path.cwd() / ".skein"
    if (pathlib.Path.cwd() / ".skein").exists()
    else pathlib.Path(properties.config_dir).expanduser()
)
