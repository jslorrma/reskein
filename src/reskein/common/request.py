#!/usr/bin/env python3
"""
common/request.py
=================

This module offers a high-level interface for HTTP requests, optimized for
Hadoop YARN and WebHDFS. It simplifies request dispatch, response and
exception handling for these systems.

The module includes a mechanism for modifying the warning message for
`InsecureRequestWarning` to inform the user about the insecure request,
mention that the warning is only shown once, and explain how to deactivate
the warning.

To deactivate insecure request warnings, set the environment variable
`IGNORE_INSECURE_REQUEST_WARNINGS` to 'True'.

Example
-------

Deactivating insecure request warnings:

```python
import os
os.environ["IGNORE_INSECURE_REQUEST_WARNINGS"] = "True"
````

Making a request:


```python
from requests import Session
from request import make_request
import msgspec

# Create a session
session = Session()

# Specify the base URL
url = "https://httpbin.org"

# Send a GET request to the "/get" endpoint
response = make_request(session, url, path="/get", method="GET")

# Print the response
print(msgspec.json.decode(response.content))
```
"""

from __future__ import annotations

__author__ = "jslorrma"
__maintainer__ = "jslorrma"
__email__ = "jslorrma@gmail.com"


# imports
import os
import warnings
from typing import Any
from urllib.parse import urljoin

from requests import Response, Session, exceptions
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning

from .. import logger
from .exceptions import handle_request_exception

_SESSION_AUTH_TOKENS = {}

# handle insecure request warnings


def _show_warning(  # noqa: PLR0913
    message: str,
    category: type[warnings.WarningMessage],
    filename: str,
    lineno: int,
    file: str | None = None,
    line: str | None = None,
):
    """
    Modifies the warning message for `InsecureRequestWarning` to inform the user about the insecure request,
    mention that the warning is only shown once, and explain how to deactivate the warning.
    """
    if category is InsecureRequestWarning:
        message = (
            "You are making an insecure request. This can expose sensitive "
            "information and potentially allow others to take over your account. "
            "This warning will only be shown once per session. "
            "If you understand the risks and wish to disable this warning, "
            "set the 'IGNORE_INSECURE_REQUEST_WARNINGS' environment variable to 'True'."
        )
    original_showwarning(message, category, filename, lineno, file=file, line=line)


# backup the original showwarning function
original_showwarning = warnings.showwarning
# monkey patch the showwarning function
warnings.showwarning = _show_warning
# show the warning only once
warnings.filterwarnings("once", category=InsecureRequestWarning)


def make_request(  # noqa: PLR0913
    session: Session,
    url: str,
    path: str | None = "/",
    method: str | None = "GET",
    params: dict[str, Any] | None = None,
    data: dict[str, Any] | bytes | None = None,
    timeout: int | float | None = None,
    raise_for_status: bool = True,
    **kwargs,
) -> Response | None:
    """
    Make a request to a REST API.

    Parameters
    ----------
    session : request.Session
        The requests session to use for the request.
    url : str
        The base URL of the REST API.
    path : str, optional
        The path of the API endpoint to request, by default `'/'`.
    method : str, optional
        The HTTP method to use for the request. One of {`'GET'`, `'POST'`, `'PUT'`,
        `'DELETE'`}, by default `'GET'`.
    params : dict[str, Any], optional
        Dictionary to send in the query string of the `Session.request`, by
        default `None`.
    data : dict[str, Any] | bytes, optional
        Dictionary or bytes object to send in the body of the `Session.request`,
        by default `None`.
    timeout : int | float, optional
        The number of seconds to wait for the server's response before giving up,
        defaults to `None` (no timeout).
    raise_for_status : bool
        Raises `HTTPError`, if one occurred.
    **kwargs
        Additional keyword arguments to pass to the `Session.request` method.

    Returns
    -------
    requests.Response or None
        The response from the server, either as a JSON dictionary or raw string.

    Raises
    ------
    DriverHTTPError
        If the request returns a status code of 400, 401, 403, 404, or 500.
    DriverProxyError
        If there is an issue with the specified proxies.
    DriverSSLError
        If there is an issue with the SSL certificates used for the request.
    DriverConnectionError
        If there is an issue establishing a connection for the request.

    Example
    -------
    ```pycon
    >>> from requests import Session
    >>> session = Session()
    >>> url = "https://httpbin.org"
    >>> make_request(session, url, path="/get", method="GET")
    <Response [200]>
    ```
    """
    # disable insecure request warnings if environment variable is set
    if os.getenv("IGNORE_INSECURE_REQUEST_WARNINGS", "False").lower() == "true":
        disable_warnings(InsecureRequestWarning)

    # make sure we recieve json response and not xml
    headers = {"Content-Type": "application/json"}

    # reset auth handler to skip user or kerberos negotiation if
    # we have a 'hadoop.auth' cookie from any previous request
    _auth_bak = session.auth
    if (session, url) in _SESSION_AUTH_TOKENS:
        session.auth = None
        headers["Cookie"] = f"hadoop.auth={_SESSION_AUTH_TOKENS[(session, url)]}"

    # construct url
    _url = urljoin(url, path)
    response = None
    try:
        logger.debug(f"Sending request to '{_url}' with: {method=}, {headers=}, {params=}, {data=}")
        response = session.request(
            method=method,
            url=_url,
            headers=headers,
            timeout=timeout,
            params=params,
            data=data,
            **kwargs,
        )
        logger.debug(f"Got response: {response.status_code=}, {response.reason=}")

        if raise_for_status:
            response.raise_for_status()

        # store "hadoop.auth" cookie for future requests
        session.auth = _auth_bak
        try:
            _SESSION_AUTH_TOKENS[(session, url)] = session.cookies.get_dict()["hadoop.auth"]
            logger.debug(
                f"Request session auth cookie set to: {_SESSION_AUTH_TOKENS[(session, url)]}"
            )
        except KeyError:
            pass

        return response
    # handle request exceptions
    except exceptions.RequestException as exc:
        handle_request_exception(exc, response, session.proxies)
