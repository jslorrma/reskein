#!/usr/bin/env python3
"""
fcntl.py
========

This module provides functionalities for controlling and managing file locks.
It serves as a Python interface to Unix's `fcntl()` and `ioctl()` routines, but
uses the `portalocker` library to implement cross-platform file locking.

The module includes:
- Custom exceptions for lock-related errors.
- An enumeration for lock flags.
- Functions to lock and unlock files with appropriate error handling using `portalocker`.

The `portalocker` library is used to handle the actual locking and unlocking of files,
providing a consistent interface across different platforms.
"""

from __future__ import annotations

__author__ = "jslorrma"
__maintainer__ = "jslorrma"
__email__ = "jslorrma@gmail.com"

import enum
import typing

import portalocker


class BaseLockException(Exception):
    """
    Base class for lock exceptions.

    This class is the base class for all exceptions related to locks. It has a single attribute,
    `fh`, which represents a file handle.

    Attributes
    ----------
    fh : typing.IO, optional
        The file handle associated with the exception, or `None` if no file handle is associated.
    """

    # Error codes:
    LOCK_FAILED = 1

    def __init__(
        self,
        *args: typing.Any,
        fh: typing.IO | None = None,
        **kwargs: typing.Any,
    ) -> None:
        self.fh = fh
        Exception.__init__(self, *args)


class LockException(BaseLockException):
    """
    Exception raised when a lock operation fails.
    """


class AlreadyLocked(LockException):
    """
    Exception raised when trying to acquire a lock that is already locked.
    """


class FileToLarge(LockException):
    """
    Exception raised when the file to be locked is too large.
    """


class LockFlags(enum.IntFlag):
    """
    A represention of lock flags.

    This enumeration has four members: `EXCLUSIVE`, `SHARED`, `NON_BLOCKING`, and `UNBLOCK`.

    Attributes
    ----------
    EXCLUSIVE : int
        Represents an exclusive lock.
    SHARED : int
        Represents a shared lock.
    NON_BLOCKING : int
        Represents a non-blocking lock.
    UNBLOCK : int
        Represents an unlock operation.
    """

    #: exclusive lock
    EXCLUSIVE = portalocker.LOCK_EX
    #: shared lock
    SHARED = portalocker.LOCK_SH
    #: non-blocking
    NON_BLOCKING = portalocker.LOCK_NB
    #: unlock
    UNBLOCK = portalocker.LOCK_UN


def _with_docstring(docstring):
    """
    A decorator to add a docstring to a function.

    Parameters
    ----------
    docstring : str
        The docstring to add.

    Returns
    -------
    callable
        The decorated function.
    """

    def decorator(func):
        func.__doc__ = docstring
        return func

    return decorator


_LOCK_DOC = """
Locks a file.

Parameters
----------
file_ : typing.IO
    The file to lock.
flags : LockFlags
    The flags to use when locking the file.

Raises
------
AlreadyLocked
    If the file is already locked.
RuntimeError
    If the flags are invalid.
LockException
    If an error occurs while locking the file.
"""

_UNLOCK_DOC = """
Unlocks a file.

Parameters
----------
file_ : typing.IO
    The file to unlock.

Raises
------
LockException
    If an error occurs while unlocking the file.
"""

@_with_docstring(_LOCK_DOC)
def lock(file_: typing.IO, flags: LockFlags):
    try:
        portalocker.lock(file_, flags)
    except portalocker.LockException as exc_value:
        raise LockException(exc_value, fh=file_) from exc_value

@_with_docstring(_UNLOCK_DOC)
def unlock(file_: typing.IO):
    try:
        portalocker.unlock(file_)
    except portalocker.LockException as exc_value:
        raise LockException(exc_value, fh=file_) from exc_value
