#!/usr/bin/env python3
"""
common/context.py
=================

This module provides the `Context` class for managing reskein contexts.

Contexts are a way to group together a set of variables that describes the
configuration for a particular Hadoop cluster, like e.g. the Hadoop
configuration directory or the driver initialization arguments.

The current context is determined from `./.skein/driver` file and `driver.env`
file in the root directory of the project. If these files don't exist, the
context is expected to be found in the global user configuration directory
`~/.skein`.

The context configurations themselves are stored in the subfolders of the
`contexts` folder. Each subfolder represents a different context, and the name
of the subfolder is the name of the context.

When activating a context, the context files are copied from the context
subfolder to the configuration directory `./.skein` or `~/.skein`. Contexts are
loaded and activated during the program initialization in the `__init__.py` file.

The context files are:
- `driver.env` file: contains environment variables
- `driver` file: contains the driver initialization arguments as a YAML file
"""  # noqa: D205

from __future__ import annotations

__author__ = "jslorrma"
__maintainer__ = "jslorrma"
__email__ = "jslorrma@gmail.com"

import os
import shutil
import psutil
import platform
from functools import cached_property
from typing import TYPE_CHECKING, Any

import msgspec
from dotenv import dotenv_values

from .. import CONFIG_DIR, get_logger

if TYPE_CHECKING:
    import pathlib

# constants
CHECK_ENV_VARS = {
    "SKEIN_",
    "HADOOP_",
    "KRB5_",
    "YARN_",
    "HDFS_",
}
"""Environment variables to check for when loading the context."""

# initialize logger
logger = get_logger()

# paths
_CONTEXTS_DIR = CONFIG_DIR / "contexts"
"""The directory containing the context configurations."""
_DRIVER_FILE = "driver"
"""The driver file containing the driver initialization arguments."""
_ENV_FILE = "driver.env"
"""The environment file containing the environment variables."""

# default context
_DEFAULT_CONTEXT = "default"


def _get_boot_time() -> float:
    """Get the system boot time."""
    if platform.system() != "Windows":
        return psutil.boot_time()
    
    # else use the uptime
    return psutil.uptime() - psutil.boot_time()

class Context:
    """
    Singleton class for managing reskein contexts.

    Class provides a context for the driver, that is used to manage connections
    to the Hadoop cluster. Contexts are a way to group together a set of
    variables that describes the configuration for a particular Hadoop cluster,
    like e.g. the Hadoop configuration directory or the driver initialization
    arguments.

    Note
    ----
    The current context is determined from a `./.skein/.context` file in the
    root directory of the project. If this file doesn't exist, the context is
    expected to be found in a global `.context` file in `~/.skein`.
    """

    _instance = None

    def __new__(cls, *args: tuple, **kwargs: dict[str, Any]) -> Context:  # noqa: ARG003
        """
        Create a new instance or return the existing instance.

        Note
        ----
        This method implements the Singleton pattern to ensure that only one
        instance of the `Context` class is created.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, name: str) -> None:
        """
        Initialize a new context.

        Parameters
        ----------
        name : str
            The name of the context.
        """
        self.name = name

        if self.name is None:
            # Check if the context needs to be reset
            self._check_reset_context()

    def _check_reset_context(self) -> None:
        """
        Check if the context needs to be reset due to a system restart.

        If the last activation timestamp is before the last system restart
        timestamp, the context is reset.
        """
        last_activation = self.dirver_file.stat().st_ctime
        last_system_restart = psutil.boot_time()

        if last_activation < last_system_restart:
            logger.info("Resetting context...")
            self.reset_context()


    @property
    def context_dir(self) -> pathlib.Path:
        """
        Get the directory of the current context.

        Returns
        -------
        pathlib.Path
            The directory of the current context.
        """
        _CONTEXTS_DIR.mkdir(parents=True, exist_ok=True)
        if self.name:
            return _CONTEXTS_DIR / self.name
        # else return the default context directory
        return _CONTEXTS_DIR

    @cached_property
    def dirver_file(self) -> pathlib.Path:
        """
        Get the driver file.

        Returns
        -------
        pathlib.Path
            The driver file of the current context.

        Note
        ----
        The driver file contains the driver initialization arguments as a YAML
        file. The driver file `driver` is located in config directory (see
        `CONFIG_DIR`).
        """
        return CONFIG_DIR / _DRIVER_FILE

    @cached_property
    def env_file(self) -> pathlib.Path:
        """
        Get the environment file.

        Returns
        -------
        pathlib.Path
            The environment file of the current context.

        Note
        ----
        The environment file contains the environment variables for the current
        context. The environment file `driver.env`is located in the config
        directory (see `CONFIG_DIR`).
        """
        return CONFIG_DIR / _ENV_FILE

    def _copy_context_files(self, name: str) -> None:
        """Copy the context files of the given `name` to the configuration directory."""
        logger.info("Copying context files to configuration directory...")

        self.name = name or self.name

        if (self.context_dir / _DRIVER_FILE).exists():
            with (
                (self.context_dir / _DRIVER_FILE).open("rb") as fil,
                self.dirver_file.open("wb") as drv_fil,
            ):
                drv_fil.write(fil.read())

        if (self.context_dir / _ENV_FILE).exists():
            with (
                (self.context_dir / _ENV_FILE).open("rb") as fil,
                self.env_file.open("wb") as env_fil,
            ):
                env_fil.write(fil.read())

    def _clean_context_files(self) -> None:
        """Clean the context directory."""
        logger.info("Cleaning context directory...")

        self.dirver_file.unlink(missing_ok=True)
        self.env_file.unlink(missing_ok=True)

    def activate(self, name: str | None = None) -> None:
        """
        Activate the context with the given `name`.

        Parameters
        ----------
        name : str, optional
            The name of the context to activate. If None, the current context is
            activated.

        Note
        ----
        The context is activated by loading the environment variables from the
        `driver.env` file in the context directory and updating the global
        environment variables.
        """
        self.name = name or self.name

        logger.info(f"Activating context '{self.name}'")

        self._copy_context_files(self.name)

        # load environment variables
        self.load_env_vars()

        # in case SKEIN_LOG_LEVEL is set with context, update logger
        globals().update({"logger": get_logger()})

    def __str__(self) -> str:
        """Get a string representation of the context."""
        return f"CONTEXT<{self.name}>"

    @staticmethod
    def list(with_paths: bool = False) -> list[str]:
        """
        List all available contexts.

        Parameters
        ----------
        with_paths : bool, optional
            If `True`, the paths to the contexts are returned as well, by
            default `False`.

        Returns
        -------
        list
            A list of all available contexts.
        """
        try:
            if with_paths:
                return [
                    (context.name, str(context))
                    for context in _CONTEXTS_DIR.iterdir()
                    if context.is_dir()
                ]
            # else return only the names
            return [context.name for context in _CONTEXTS_DIR.iterdir() if context.is_dir()]
        except FileNotFoundError:
            return []

    def save(self, name: str | None = None) -> None:
        """
        Save the current context with the given `name`.

        Parameters
        ----------
        name : str, optional
            The name of the context to save. If None, the current context is
            saved.

        Note
        ----
        The current context is saved by writing the driver keyword arguments
        and environment variables to the context directory.
        """
        # read current driver file
        _drv_kwargs = self.read_driver()

        self.name = name or self.name

        logger.info(f"Saving context '{self.name}'")

        # write driver file
        self.write_driver(**_drv_kwargs)
        # write environment variables
        self.write_env_vars()

    @staticmethod
    def delete(name: str) -> None:
        """
        Delete a context with the given `name`.

        Parameters
        ----------
        name : str
            The name of the context to remove.
        """
        logger.info(f"Removing context '{name}'")

        # reset context if it's the current context
        if name == CONTEXT.name:
            CONTEXT.reset_context()

        if (_context := _CONTEXTS_DIR / name).exists():
            shutil.rmtree(_context)

    def reset_context(self) -> None:
        """Reset the current context."""
        logger.info("Resetting context...")

        if self.env_file.exists():
            # delete environment variables
            for k in dotenv_values(self.env_file):
                if k in os.environ:
                    del os.environ[k]

        # delete context files
        self._clean_context_files()
        self.name = None

    @classmethod
    def __enter__(cls, name: str | None = None) -> Context:
        """
        Enter the runtime context related to this object.

        Parameters
        ----------
        name : str, optional
            The name of the context to activate. If `None`, the current context
            is activated.

        Returns
        -------
        Context
            The current instance of the Context.
        """
        # cache the old context name
        _old_ctx = cls._instance.name if cls._instance else None
        self = cls(name)
        # assign the old context name to the instance (used in __exit__ to reset the context)
        self._old_ctx = _old_ctx
        self.activate()

        return self

    def __exit__(self, *args: tuple, **kwargs: dict[str, Any]) -> None:
        """Exit the runtime context related to this object."""
        if self._old_ctx:
            self.activate(self._old_ctx)
            del self._old_ctx

    def read_driver(self) -> dict[str, Any]:
        """
        Reads the driver file and returns the data from the file as a dictionary.

        Returns
        -------
        dict
            A dictionary containing the data from the driver file or an empty dictionary.
        """
        logger.debug("Reading driver init file...")
        # Read the driver file
        try:
            with self.dirver_file.open("rb") as fil:
                drv_kwargs = msgspec.yaml.decode(fil.read()) or {}

            logger.debug(f"Successfully read driver init file: {drv_kwargs}")

            return drv_kwargs
        except FileNotFoundError:
            logger.debug("Failed to read driver init file, returning no init args...")
            return {}

    def write_driver(self, **kwargs: dict[str, Any]) -> None:
        """
        Writes the given keyword arguments to the driver file using YAML.

        Parameters
        ----------
        **kwargs
            The data to write to the driver file.
        """
        logger.debug(f"Writing to driver init file with kwargs: {kwargs}")

        # Ensure the config dir exists
        self.context_dir.mkdir(parents=True, exist_ok=True)

        # Write to the driver file
        with (self.context_dir / _DRIVER_FILE).open("wb") as drv_fil:
            drv_fil.write(msgspec.yaml.encode(kwargs))

    def load_env_vars(self) -> None:
        """
        Load the driver environment variables.

        Environment variables are loaded from the `driver.env` file in the
        context directory and updated in the global environment variables.
        """
        logger.debug("Loading environment variables from `driver.env` file...")
        try:
            # parse environment variables from local and global .env file
            _env_vars = dotenv_values(self.env_file)

            # set environment variables
            for k, v in _env_vars.items():
                if k in os.environ:
                    continue
                if v is not None:
                    os.environ[k] = v
        except FileNotFoundError:
            logger.debug("Failed to load environment variables from `driver.env` file...")

    def write_env_vars(self, **kwargs: dict[str, Any]) -> None:
        """
        Write environment variables to current context.

        Parameters
        ----------
        **kwargs
            The environment variables to write to the context.

        Note
        ----
        The environment variables are written to the `driver.env` file in the
        context directory.
        """
        if kwargs:
            _env_vars = dotenv_values(self.env_file) | kwargs
        else:
            # load and update environment variables
            _env_vars = dotenv_values(self.env_file) | {
                env_key: os.getenv(env_key)
                for env_key in os.environ
                if any(env_key.startswith(prefix) for prefix in CHECK_ENV_VARS)
            }
        logger.debug(f"Writing environment variables to `driver.env` file with kwargs: {_env_vars}")

        # Ensure the config dir exists
        self.context_dir.mkdir(parents=True, exist_ok=True)
        # Write out env vars to the .env file
        with (self.context_dir / _ENV_FILE).open("w") as env_fil:
            for k, v in _env_vars.items():
                env_fil.write(f"{k}={v}\n")


CONTEXT = Context()
"""
The current context.

The current context defaults to the last activated context.
"""
