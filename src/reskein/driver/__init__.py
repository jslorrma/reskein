#!/usr/bin/env python3
"""
driver
======

Submodule implementing [skein's JAVA based `Driver`][1] in Python using YARN
ResourceManager REST API][2] to gather cluster and application information as
well as to connect to and submit applications.

[1]: https://github.com/jcrist/skein/blob/master/java/src/main/java/com/anaconda/skein/Driver.java
[2]: https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
"""

from __future__ import annotations

__author__ = "jslorrma"
__maintainer__ = "jslorrma"
__email__ = "jslorrma@gmail.com"

from .driver import (  # noqa: F401
    Driver,
    clean_driver,
    load_driver_env_vars,
    read_driver,
    write_driver,
    write_driver_env_vars,
)

_SKEIN_DIR_PERM = "700"  # "777"
_SKEIN_FILE_PERM = "600"  # "666"
_SKEIN_LOG_DIR_EXPANSION_VAR = "<LOG_DIR>"
_KINIT_CMD = "kinit -kt {keytab} {principal}"
