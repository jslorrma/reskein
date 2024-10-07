#!/usr/bin/env python3
"""
common/utils.py
===============

Implements utiliy functions and decorators
"""

from __future__ import annotations

__author__ = "jslorrma"
__maintainer__ = "jslorrma"
__email__ = "jslorrma@gmail.com"

# imports
import functools
from collections.abc import Callable, Iterable
from typing import Any

try:  # cache is new in 3.9
    from functools import cache
except ImportError:  # python < 3.9
    # lru_cache wrapper with 'maxsize=None'
    # decorator for python < 3.9
    def cache(func: Callable):
        """Simple cache decorator for python < 3.9"""

        @functools.wraps(func)
        @functools.lru_cache(maxsize=None)  # noqa: UP033
        def f(*args, **kwargs):
            return func(*args, **kwargs)

        return f


from skein.core import Client

parse_datetime = cache(Client._parse_datetime)


def as_iterable(obj: Any | list[Any], type_: object | None = None) -> Iterable:
    """Helper function ensuring the given `obj` is returned as iterable.

    Parameters
    ----------
    obj : Union[Any, List[Any]]
        The object to ensure to be iterable.
    type_ : type, optional
        The type to convert the iterable to, by default `None`

    Returns
    -------
    Iterable
        Iterable object.
    """
    if isinstance(obj, str):
        iterable = [obj]
    else:
        try:
            iter(obj)
            iterable = obj
        except TypeError:
            iterable = [obj]

    return type_(iterable) if type_ is not None else iterable
