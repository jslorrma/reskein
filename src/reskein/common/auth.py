#!/usr/bin/env python3
"""
common/auth.py
==============

This module contains the authentication handlers for HTTP requests,
specifically for interacting with Hadoop REST APIs.

It provides a class `HTTPSimpleAuth` that implements simple HTTP authentication
by attaching a username to the request. The username can be provided directly,
or it is read from the environment variables `'SKEIN_USER_NAME'`, `'USER'`, or
`'USERNAME'`. If none of these are set, the user is prompted to enter a
username.

The module also imports the `HTTPKerberosAuth` from the `requests_kerberos`
package for Kerberos authentication.
"""

from __future__ import annotations

__author__ = "jslorrma"
__maintainer__ = "jslorrma"
__email__ = "jslorrma@gmail.com"

import os

from requests import PreparedRequest, auth
from skein.objects import Enum

# import auth handlers
try:
    from requests_kerberos import OPTIONAL, HTTPKerberosAuth  # noqa: F401
except (ImportError, ModuleNotFoundError) as e:
    import inspect

    msg = inspect.cleandoc(
        """
        Package 'requests-kerberos' missing. Install with `conda`:
        $ conda install -c conda-forge requests-kerberos

        or `mamba`:
        $ mamba install -c conda-forge requests-kerberos

        or `pip`:
        $ pip install requests-kerberos
        """
    )
    import sys

    raise type(e)(str(e) + "\n\n" + msg).with_traceback(sys.exc_info()[2]) from e

from .. import logger


class HTTPSimpleAuth(auth.AuthBase):
    """Attaches HTTP simple username Authentication to the given Request
    object.

    Parameters
    ----------
    username : str, optional
        User name to authenticate with. If not given the value of `'SKEIN_USER_NAME'`
        environment varibale or the current system user's username is used.
    """

    def __init__(self, username: str | None = None):
        self.username = (
            username
            if username
            else (
                os.getenv("SKEIN_USER_NAME")
                or os.getenv("USER")
                or os.getenv("USERNAME")
                or input(
                    "Enter username for 'simple' HTTP authentication required! Enter username: "
                )
            )
        )

        logger.debug(f"Using simple HTTP authentication with username '{username}'")
        self.params = None
        self.auth_done = False

    def __call__(self, request: PreparedRequest):
        if not self.auth_done:
            self.params = {"user.name": self.username}
            self.auth_done = True

        request.prepare_url(request.url, self.params)
        return request


class Authentication(Enum):
    """Authentication method to use

    Attributes
    ----------
    SIMPLE : Authentication
        Simple authentication
    KERBEROS : Authentication
        Kerberos authentication
    """

    _values = ("SIMPLE", "KERBEROS")
