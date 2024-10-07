#!/usr/bin/env python3
"""
driver/services.py
=========================

This module provides utility functions for checking the reachability and
activity status of a service node at a given addresses for Hadoop `YARN`
`ResourceManager` and `WebHDFS` service endpoints.
"""

from __future__ import annotations

__author__ = "jslorrma"
__maintainer__ = "jslorrma"
__email__ = "jslorrma@gmail.com"

from http import HTTPStatus
from typing import TYPE_CHECKING

import msgspec
import requests

from .. import logger
from ..common.exceptions import DriverError
from ..common.request import make_request

if TYPE_CHECKING:
    from collections.abc import Callable


# @cache
def _service_node_is_active(  # noqa: PLR0913
    session: requests.Session,
    address: str,
    path: str,
    test_fnc: Callable,
    service: str,
    params: dict[str, str] | None = None,
) -> tuple[bool, int]:
    """
    Check if a service node at `address` is reachable and active.

    Parameters
    ----------
    session : requests.Session
        The session to use for making the request.
    address : str
        The address of the service node.
    path : str
        The path to request.
    test_fnc : Callable
        A function to test if the response indicates the service node is active.
    service : str
        The name of the service.
    params : dict[str, str], optional
        The parameters to include in the request, by default `None`.

    Returns
    -------
    tuple[bool, int]
        A tuple containing a boolean indicating if the service node is active and the status code of
        the response.
    """

    try:
        response = make_request(
            session=session,
            url=address,
            path=path,
            params=params or {},
            allow_redirects=False,
            raise_for_status=False,
        )
    except (DriverError, requests.RequestException) as err:
        logger.debug(
            f"{service.capitalize()} service node not running at '{address}'. Error: {err}"
        )
        status_code = (
            err.args[1].status_code if len(err.args) > 1 else HTTPStatus.SERVICE_UNAVAILABLE
        )
        return False, status_code

    if response is None:
        logger.debug(f"{service.capitalize()} service node not running at '{address}'")
        return False, HTTPStatus.SERVICE_UNAVAILABLE

    if not test_fnc(response):
        logger.debug(
            f"{service.capitalize()} service node at '{address}' is inactive. Status code: {response.status_code}"
        )
        return False, response.status_code

    logger.debug(
        f"Using '{address}' as {service} service node address. Status code: {response.status_code}"
    )
    return True, response.status_code


def get_service_endpoint(  # noqa: PLR0913
    addresses: list[str],
    session: requests.Session,
    path: str,
    test_fnc: Callable,
    service: str,
    params: dict[str, str] | None = None,
) -> str | None:
    """
    Get the endpoint of an active service from a list of potential addresses.

    This function iterates over the list of addresses and uses the provided test function to check
    if the service at each address is active. If an active service is found, its address is
    returned. If no active service is found, but there are services that responded with a status
    code less than `500`, the address of the last such service is returned. If no services responded
    with a status code less than `500`, `None` is returned.

    Parameters
    ----------
    addresses : list[str]
        A list of potential addresses for the service.
    session : requests.Session
        The session to use for making the request.
    path : str
        The path to append to the address to form the request URL.
    test_fnc : Callable
        A function that takes the response from the service and returns a boolean indicating whether
        the service is active.
    service : str
        The name of the service. This is used only for logging purposes.
    params : dict[str, str], optional
        A dictionary of query parameters to include in the request, by default `None`.

    Returns
    -------
    str | None
        The address of an active service node, the address of the last reachable service node, or
        `None` if no reachable service node was found.
    """

    _reachable = []
    for address in addresses:
        _active, _status = _service_node_is_active(
            session=session,
            address=address,
            path=path,
            test_fnc=test_fnc,
            service=service,
            params=params,
        )
        if _active:
            return address
        elif _status < HTTPStatus.INTERNAL_SERVER_ERROR:
            _reachable.append(address)

    if _reachable:
        return _reachable[-1]


def get_rm_endpoint(addresses: list[str], session: requests.Session) -> str | None:
    """
    Get Yarn ResourceManager endpoint from a list of potential addresses.

    This function iterates over the list of addresses and sends a request to the `"/ws/v1/cluster"`
    path. If the response status code is less than `302` (`HTTPStatus.FOUND`), it considers the
    service at the address as active.

    Parameters
    ----------
    addresses : list[str]
        A list of potential addresses for the ResourceManager service.
    session : requests.Session
        The session to use for making the request.

    Returns
    -------
    str | None
        The address of the active ResourceManager service node, or `None` if no active service node
        was found.
    """
    return get_service_endpoint(
        addresses=addresses,
        session=session,
        path="/ws/v1/cluster",
        test_fnc=lambda r: r.status_code < HTTPStatus.FOUND,
        service="RessourceManager",
    )


def get_hdfs_endpoint(addresses: list[str], session: requests.Session) -> str | None:
    """
    Get Hadoop WebHDFS endpoint from a list of potential addresses.

    This function iterates over the list of addresses and sends a request to the `"/jmx"` path. It
    uses a helper function `_isactive` to check the response JSON and determine if the service at
    the address is active.

    Parameters
    ----------
    addresses : list[str]
        A list of potential addresses for the HDFS service.
    session : requests.Session
        The session to use for making the request.

    Returns
    -------
    str | None
        The address of the active HDFS service node, or `None` if no active service node was found.
    """

    def _isactive(r: requests.Response):
        try:
            return msgspec.json.decode(r.content)["beans"][0]["State"] == "active"
        except (msgspec.DecodeError, IndexError, KeyError):
            return False

    return get_service_endpoint(
        addresses=addresses,
        session=session,
        path="/jmx",
        test_fnc=_isactive,
        service="WebHDFS",
        params={"get": "Hadoop:service=NameNode,name=NameNodeStatus::State"},
    )
