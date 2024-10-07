#!/usr/bin/env python3
"""
common/context.py
=================

This module provides the `Context` class for managing reskein contexts.

Contexts are a way to group together a set of variables that describes the
configuration for a particular Hadoop cluster, like e.g. the Hadoop
configuration directory or the driver initialization arguments.

The current context is determined from a `./.skein/.context` file in the root
directory of the project. If this file doesn't exist, the context is expected to
be found in a global .context file in `~/.skein`.

The context configurations themselves are stored in the subfolders of the
`contexts` folder. Each subfolder represents a different context, and the name
of the subfolder is the name of the context.

When activating a context, the context files are copied from the context
subfolder to the configuration directory `./.skein` or `~/.skein`. Contexts are
loaded and activated during the program initialization in the `__init__.py` file.

The context files are:
- `.env` file: contains environment variables
- `driver` file: contains the driver initialization arguments as a YAML file
"""

from __future__ import annotations

__author__ = "jslorrma"
__maintainer__ = "jslorrma"
__email__ = "jslorrma@gmail.com"

import os
import pathlib
import shutil
from typing import Any

import msgspec
from dotenv import dotenv_values

from .. import get_logger, CONFIG_DIR

# initialize logger
logger = get_logger()


"""The configuration directory for reskein."""
_CONTEXTS_DIR = CONFIG_DIR / "contexts"
"""The directory containing the context configurations."""
_CURRENT_CONTEXT = CONFIG_DIR / ".context"
"""The file containing the current context."""
# path to the "global driver" file
_DRIVER_ARGS_FILE = CONFIG_DIR / "driver"
"""The file containing the driver initialization arguments."""
_DRIVER_ENV_FILE = CONFIG_DIR / ".env"
"""The file containing the driver environment variables."""

# the default context name
_DEFAULT_CONTEXT = "_default"


class Context:
    """
    The `Context` class is used to manage reskein contexts, mainly for the driver, that is
    used to manage connections to the Hadoop cluster.

    Contexts are a way to group together a set of variables that describes the configuration for a
    particular Hadoop cluster, like e.g. the Hadoop configuration directory or the driver
    initialization arguments.

    The current context is determined from a `./.skein/.context` file in the root directory of the
    project. If this file doesn't exist, the context is expected tobe found in a global .context
    file in `~/.skein`.
    """

    def __init__(self, name: str):
        """
        Initialize a new context.

        Parameters
        ----------
        name : str
            The name of the context.
        """
        self.name = name

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
        return _CONTEXTS_DIR / self.name

    def activate(self, name: str | None = None):
        """
        Activate the context by copying its files to the global configuration
        directory.

        Parameters
        ----------
        name : str, optional
            The name of the context to activate. If None, the current context is
            activated.
        """

        self._set_context(name)

        # copy files to configuration directory
        if name is not None:
            for file in (_DRIVER_ARGS_FILE.name, ".env"):
                if (self.context_dir / file).exists():
                    shutil.copy2(self.context_dir / file, CONFIG_DIR)
                else:
                    (CONFIG_DIR / file).unlink(missing_ok=True)

        logger.info(f"Activating context '{self.name}'")

        # load environment variables
        self.load_env_vars()

        # in case SKEIN_LOG_LEVEL is got set with context, update logger
        globals().update({"logger": get_logger()})

    def __str__(self) -> str:
        """
        Get a string representation of the context.
        """
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
                return [(context.name, str(context)) for context in _CONTEXTS_DIR.iterdir() if context.is_dir()]
            else:
                return [context.name for context in _CONTEXTS_DIR.iterdir() if context.is_dir()]
        except FileNotFoundError:
            return []

    @staticmethod
    def remove(name: str):
        """
        Remove a context.

        Parameters
        ----------
        name : str
            The name of the context to remove.
        """
        context_dir = _CONTEXTS_DIR / name

        if context_dir.exists():
            shutil.rmtree(context_dir)

    def _set_context(self, name: str | None = None) -> Context:
        """
        Set the current context.

        Parameters
        ----------
        name : str, optional
            The name of the context to set. If `None`, the current context is
            set.

        Returns
        -------
        Context
            The updated context object.

        """
        if name is not None:
            self.name = name

            with _CURRENT_CONTEXT.open("w") as f:
                f.write(self.name)

        return self

    def save(self, name: str | None = None):
        """
        Save the current context to the global configuration directory.

        Parameters
        ----------
        name : str, optional
            The name of the context to save. If None, the current context is
            saved.
        """
        # set context
        self._set_context(name)

        # create context directory if not exists
        self.context_dir.mkdir(parents=True, exist_ok=True)

        # copy files to context directory
        for file in (_DRIVER_ARGS_FILE.name, ".env"):
            try:
                if (CONFIG_DIR / file).exists():
                    shutil.copy2(CONFIG_DIR / file, self.context_dir)
            except (FileNotFoundError, PermissionError) as e:
                logger.warning(f"Error copying {file}: {e}")

        # and activate with new name
        if name:
            self.activate(name)

    def reset_context(self):
        """
        Reset the current context.

        This function deletes the environment variables and the current context
        directory, sets the context to 'default', and activates it.
        """
        # delete environment variables
        for k in dotenv_values(CONFIG_DIR / ".env"):
            if k in os.environ:
                del os.environ[k]

        # delete current context driver
        self.clean_driver()
        # set context to default
        self.save(_DEFAULT_CONTEXT)
        # activate context
        self.activate()

    def __enter__(self):
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
        if self.name is not None and self.name != CONTEXT.name:
            self._context_backup = CONTEXT.name
            CONTEXT.save()
            CONTEXT.activate(self.name)

        return self

    def __exit__(self, *args, **kwargs):
        """
        Exit the runtime context related to this object.
        """
        if hasattr(self, "_context_backup"):
            CONTEXT.activate(self._context_backup)
        # do not suppress exceptions
        return False

    @staticmethod
    def read_driver() -> dict[str, Any]:
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
            with _DRIVER_ARGS_FILE.open("rb") as fil:
                drv_kwargs = msgspec.yaml.decode(fil.read()) or {}

            logger.debug(f"Successfully read driver init file: {drv_kwargs}")

            return drv_kwargs
        except FileNotFoundError:
            logger.debug("Failed to read driver init file, returning no init args...")
            return {}

    @staticmethod
    def write_driver(**kwargs):
        """
        Writes the given keyword arguments to the driver file using YAML.

        Parameters
        ----------
        **kwargs
            The data to write to the driver file.
        """
        logger.debug(f"Writing to driver init file with kwargs: {kwargs}")

        # Ensure the config dir exists
        _DRIVER_ARGS_FILE.parent.mkdir(parents=True, exist_ok=True)

        # Write to the driver file
        with _DRIVER_ARGS_FILE.open("wb") as fil:
            fil.write(msgspec.yaml.encode(kwargs))

        # Save the current context
        CONTEXT.save()

    @classmethod
    def clean_driver(cls):
        """
        Clean the driver file.
        """
        logger.debug("Cleaning driver files ...")
        # delete driver file
        _DRIVER_ARGS_FILE.unlink(missing_ok=True)
        (CONFIG_DIR / ".env").unlink(missing_ok=True)

    @staticmethod
    def load_env_vars():
        """
        Load the environment variables from the `.env` file in the global
        configuration directory.

        Returns
        -------
        dict
            A dictionary containing the environment variables from the `.env` file.
        """
        logger.debug("Loading environment variables from .env file...")
        # parse environment variables from local and global .env file
        _env_vars = dotenv_values(CONFIG_DIR / ".env") | dotenv_values()

        # set environment variables
        for k, v in _env_vars.items():
            if k in os.environ:
                continue
            if v is not None:
                os.environ[k] = v

    @staticmethod
    def write_env_vars(
        update: bool = True,
        **kwargs,
    ):
        """
        Write environment variables to the `.env` file in the global
        configuration directory.

        Parameters
        ----------
        **kwargs
            The environment variables to write to the `.env` file.
        """
        if kwargs:
            _env_vars = dotenv_values(CONFIG_DIR / ".env") | kwargs
        else:
            # load and update environment variables
            _env_vars = dotenv_values(CONFIG_DIR / ".env") | {
                env_key: os.getenv(env_key)
                for env_key in os.environ
                if any(key in env_key for key in ("SKEIN", "HADOOP", "KRB5", "YARN", "HDFS"))
            }
        logger.debug(f"Writing environment variables to .env file with kwargs: {_env_vars}")

        # Ensure the config dir exists
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        # Write out env vars to the .env file
        with (CONFIG_DIR / ".env").open("w") as f:
            for k, v in _env_vars.items():
                f.write(f"{k}={v}\n")


try:
    CONTEXT = Context(_CURRENT_CONTEXT.read_text().strip())
except FileNotFoundError:
    CONTEXT = Context(_DEFAULT_CONTEXT)
