#!/usr/bin/env python3
"""
common/exceptions.py
====================

This module implements new exceptions and request error handling.
"""

from __future__ import annotations

__author__ = "jslorrma"
__maintainer__ = "jslorrma"
__email__ = "jslorrma@gmail.com"


# imports
import os
import re
import subprocess
import sys

from requests import Response
from requests import exceptions as request_exceptions

# import skein exceptions
from skein.exceptions import ConnectionError, DriverError, SkeinError, _Context

_SESSION_AUTH_TOKENS = {}


class ReSkeinError(SkeinError):
    """Basic reskein exception"""


class DriverRestServiceError(DriverError):
    """Internal HTTP exceptions from driver Rest service request"""


class DriverHTTPError(DriverRestServiceError):
    """Internal HTTP exceptions from driver Rest service request"""


class DriverSSLError(DriverRestServiceError):
    """Internal SSL exceptions from driver Rest service request"""


class DriverProxyError(DriverRestServiceError):
    """Internal proxy exceptions from driver Rest service request"""


class DriverConnectionError(ConnectionError):
    """Internal connection exceptions from driver Rest service request"""


class DriverConfigurationError(DriverError):
    """Driver configuration is invalid"""


for exc in [
    ValueError,
    KeyError,
    TypeError,
    FileExistsError,
    FileNotFoundError,
    AttributeError,
    UserWarning,
    subprocess.SubprocessError,
]:
    _Context.register_wrapper(exc)

context = _Context()


_400_ERROR_HINT = (
    "{exception_msg}\n\n"
    "400 Bad Request: The server couldn't process your request due to a client error. "
    "Check your request syntax, data formatting, required parameters, and request method."
)

_401_ERROR_HINT = (
    "{exception_msg}\n\n"
    "401 Unauthorized: Your request lacks valid authentication. "
    "Check the Hadoop cluster's security settings and ensure your `Driver` is "
    "properly initialized. Users can authenticate using either `'simple'` or "
    "`'kerberos'` (SPNEGO or Token) authentication. If using `auth='kerberos'`, "
    "ensure a valid Kerberos token is present. You might wnat to `kinit` again."
    "\nDebug info: List of Kerberos tokens:\n{krb5_tokens}'"
)

_403_ERROR_HINT = (
    "{exception_msg}\n\n"
    "403 Forbidden: The server understands your request but won't authorize it. "
    "Check your permissions and Kerberos configuration. The two possible causes "
    "are: the user may not have the necessary permissions, or an issue with the "
    "Kerberos configuration (especifically, the default realm may not match the "
    "Hadoop configuration principal realms)."
    "\nDebug info: Used default realm: '{krb5_default_realm}', Expected realm: '{hadoop_realm}'"
)

_404_ERROR_HINT = (
    "{exception_msg}\n\n"
    "404 Not Found: The server can't find the requested resource. "
    "Check the hostname and port of the `Driver` configuration and the "
    "path requested."
)

_500_ERROR_HINT = (
    "{exception_msg}\n\n"
    "500 Internal Server Error: The server encountered an unexpected issue. "
    "This could be a server-side bug, a configuration issue, or an internal exception."
)

_CONNECTION_ERROR_HINT = (
    "{exception_msg}\n\n"
    "Connection Error: There's a problem connecting to the service. "
    "Check the hostname and port of the `Driver` configuration, the network connection, "
    "and the service itself."
)

_PROXY_ERROR_HINT = (
    "{exception_msg}\n\n"
    "Proxy Error: There's an issue with the proxy server. Possible causes of "
    "this error include incorrect proxy settings, an invalid or expired proxy "
    "authentication credential, or a problem with the proxy server itself. "
    "Check your proxy settings. Proxies used: {proxies}"
)

_SSL_ERROR_HINT = (
    "{exception_msg}\n\n"
    "This error can occur when there is an issue with the SSL/TLS certificates "
    "used within your `Hadoop` cluster. This can be caused by a variety of "
    "issues, including an expired or invalid SSL/TLS certificate on the `Hadoop` "
    "cluster, an incomplete or untrusted certificate chain, or a problem with "
    "the your client's certificate store.\n"
    "To solve this you could provide the path to a certificate bundle file by "
    "passing the path to the `verify` parameter of the `Driver`. The `Driver` "
    "can also be configured to disable SSL verification by setting `verify=False`. "
    "However, this is not recommended as it can leave you vulnerable to security "
    "threats."
)

_SSL_ERROR_HINT = (
    "{exception_msg}\n\n"
    "SSL Error: There's an issue with the SSL/TLS certificates in your Hadoop cluster. "
    "Check the certificates and consider providing a certificate bundle file to the "
    "`verify` parameter of the `Driver` or disabling SSL verification (not recommended)."
)


def krb_cache():
    """Get the current kerberos cache."""
    return (
        subprocess.run("klist", text=True, capture_output=True, check=False, shell=True).stdout
        or "Cache is empty"
    )


def handle_request_exception(
    exception: Exception,
    failed_response: Response | None = None,
    proxies: dict[str, str] | None = None,
):
    """Handle request exceptions for failed HTTP requests.

    Parameters
    ----------
    exception : request_exceptions.RequestException
        Exception object for failed request.
    failed_response : Response, optional
        The response of the failed request to handle
    proxies : dict[str, str], optional
        Proxies to use for the request. Default is None.

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
    """
    exception_msg = str(exception)

    # logger.debug(f"handling the following expection: {exception_msg}, {exception_class}")

    if "400" in exception_msg:
        _error = DriverHTTPError
        _message = _400_ERROR_HINT.format(exception_msg=exception_msg)
    elif "401" in exception_msg:
        _error = DriverHTTPError
        _message = _401_ERROR_HINT.format(exception_msg=exception_msg, krb5_tokens=krb_cache())
    elif "403" in exception_msg:
        from ..hadoop.config import HadoopConfig

        # get hadoop realm
        hadoop_realm = HadoopConfig().http_principal.split("@")[1]
        # parse cache for principal realms
        cache_realms = set(re.findall(r"(?:HTTP|http)+\/[a-zA-Z0-9.-]+\@(.*?)\n", krb_cache()))
        cache_realms.discard(HadoopConfig().http_principal.split("@")[1])
        krb5_default_realm = list(cache_realms)[-1]

        _error = DriverHTTPError
        _message = _403_ERROR_HINT.format(
            exception_msg=exception_msg,
            krb5_default_realm=krb5_default_realm,
            hadoop_realm=hadoop_realm,
        )
    elif "404" in exception_msg:
        _error = DriverHTTPError
        _message = _404_ERROR_HINT.format(exception_msg=exception_msg)

    elif "500" in exception_msg:
        _error = DriverHTTPError
        _message = _500_ERROR_HINT.format(exception_msg=exception_msg)
    elif isinstance(exception, request_exceptions.ProxyError):
        _proxies = proxies or {"http": os.getenv("HTTP_PROXY"), "https": os.getenv("HTTPS_PROXY")}
        _error = DriverProxyError
        _message = _PROXY_ERROR_HINT.format(
            exception_msg=exception_msg,
            proxies=_proxies,
        )
    elif isinstance(exception, request_exceptions.SSLError):
        _error = DriverSSLError
        _message = _SSL_ERROR_HINT.format(exception_msg=exception_msg)
    elif isinstance(exception, request_exceptions.ConnectionError):
        _error = DriverConnectionError
        _message = _CONNECTION_ERROR_HINT.format(exception_msg=exception_msg)
    else:
        raise exception

    raise _error(
        f"{_message}\n\nOriginal server response:\n{failed_response.text}"
        if failed_response is not None
        else _message
    ).with_traceback(sys.exc_info()[2])
