#!/usr/bin/env python3
"""
yarn
====

Submodule implementing [YARN RessourceManager Rest API][1] and WebHDFS
filesystem API extending `fsspec`'s WebHDFS implementation to support  getting a
delegation token from the [WebHDFS endpoint][2].

[1]: https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
[2]: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Delegation_Token_Operations
"""

from __future__ import annotations

__author__ = "jslorrma"
__maintainer__ = "jslorrma"
__email__ = "jslorrma@gmail.com"
