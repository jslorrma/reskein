#!/usr/bin/env python3
"""
yarn/fsspec.py
==============

Implements [YARN RessourceManager Rest API][1] and WebHDFS filesystem API extending
`fsspec`'s WebHDFS implementation to support  getting a delegation token from the
[WebHDFS endpoint][1] and to use internal request exception handling.

[1]: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Delegation_Token_Operations
"""

from __future__ import annotations

__author__ = "jslorrma"
__maintainer__ = "jslorrma"
__email__ = "jslorrma@gmail.com"

# imports
from functools import wraps
from typing import IO, Any

from fsspec.implementations.webhdfs import WebHDFS

# module imports
from ..common.exceptions import handle_request_exception


class HDFSFileSystem(WebHDFS):
    """WebHDFS filesystem REST API wrapper based on ffspec's WebHDFS implementation."""

    # to use internal request exception handling
    @wraps(WebHDFS._call)
    def _call(  # noqa: PLR0913
        self,
        op: str,
        method: str = "get",
        path: str | None = None,
        data: dict | list[tuple[str, Any]] | bytes | IO | None = None,
        redirect: bool = True,
        **kwargs,
    ):
        """Patch fsspec WebHDFS `_call` method using exception handling"""
        try:
            return super()._call(
                op=op, method=method, path=path, data=data, redirect=redirect, **kwargs
            )
        except Exception as exc:
            handle_request_exception(exc, self.session.proxies)

    # to get delegation token with specific service and kind
    def get_delegation_token(
        self, renewer: str | None = None, service: str | None = None, kind: str | None = None
    ):
        """Retrieve token which can give the same authority to other users

        Parameters
        ----------
        renewer: str, optional
            User who may use this token; if None, will be current user
        service: str, optional
            Service to get delegation token for; if None, will be `'WEBHDFS'` or
            `'SWEBHDFS'`
        kind: str, optional
            Kind to get token for; if None, will be `'HDFS_DELEGATION_TOKEN'`
        """
        kwargs = {}
        if renewer:
            kwargs["renewer"] = renewer
        if service:
            kwargs["service"] = service
        if kind:
            kwargs["kind"] = kind
        out = self._call("GETDELEGATIONTOKEN", **kwargs)
        t = out.json()["Token"]
        if t is None:
            raise ValueError("No token available for this user/security context")
        return t
