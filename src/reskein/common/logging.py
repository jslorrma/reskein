#!/usr/bin/env python3
"""
common/logging.py
=================


"""

from __future__ import annotations

__author__ = "jslorrma"
__maintainer__ = "jslorrma"
__email__ = "jslorrma@gmail.com"

import logging
import os
import pathlib
from typing import TYPE_CHECKING

from rich.console import Console, ConsoleRenderable, RenderableType, _is_jupyter
from rich.logging import LogRender, RichHandler
from rich.text import Text, TextType
from rich.theme import Theme

from .. import LIBRARY_NAME

if TYPE_CHECKING:
    from collections.abc import Iterable
    from datetime import datetime

    from rich._log_render import FormatTimeCallable
    from rich.table import Table

# setup theme
_logging_theme = Theme(
    {
        # repr
        "repr.str": "not bold not italic grey39",
        "repr.bool_true": "italic #4585C9",
        "repr.bool_false": "italic #B87961",
        "repr.number": "#598A44",
        "repr.ipv6": "bold #598A44 blink",
        "repr.url": "not bold not italic underline #4585C9",
        # logging
        "logging.level.debug": "not dim bold #598A44",
        "logging.level.info": "not dim #FED00B",
        "logging.level.warning": "not dim red3",
        "logging.level.error": "not dim bold red3",
        "logging.level.critical": "not dim bright_white on red3",
        # traceback
        "traceback.error": "bold red3",
        "traceback.border.syntax_error": "red3",
        "traceback.border": "#4585C9",
        "traceback.text": "#4585C9",
        "traceback.title": "#4585C9",
        "traceback.exc_type": "bold red3",
        "traceback.exc_value": "#4585C9",
        "traceback.offset": "bold red3",
    }
)


class _LogRender(LogRender):
    # custom log render
    def __call__(  # noqa: PLR0913
        self,
        console: Console,
        renderables: Iterable[ConsoleRenderable],
        log_time: datetime | None = None,
        time_format: str | FormatTimeCallable | None = None,
        level: TextType = "",
        path: str | None = None,
        line_no: int | None = None,
        link_path: str | None = None,
    ) -> Table:
        """
        Render a log call to a table.

        Parameters
        ----------
        console : Console
            The console to use for rendering.
        renderables : Iterable[ConsoleRenderable]
            The renderables to log.
        log_time : datetime | None, optional
            The time of the log, by default None
        time_format : str | FormatTimeCallable | None, optional
            The format of the time, by default None
        level : TextType, optional
            The level of the log, by default ""
        path : str | None, optional
            The path of the log, by default None
        line_no : int | None, optional
            The line number of the log, by default None
        link_path : str | None, optional
            The link path of the log, by default None

        Returns
        -------
        Table
            The rendered log as a table.
        """
        from rich.containers import Renderables
        from rich.table import Table

        # Helper function to get the style for a given log level
        def _get_style(level: TextType):
            return _logging_theme.styles.get(
                f"logging.level.{level._text[0].strip().lower()}", None
            )

        # Create a new table for the log output
        output = Table.grid(padding=(0, 1), expand=True)
        output.add_column(style="log.level", width=self.level_width)
        output.add_column(style="log.time")
        output.add_column(ratio=1, style="log.message", overflow="fold")
        output.add_column(style="log.path")

        # Create a new row for the log output
        row: list[RenderableType] = []

        # add the log level to the row
        row.append(Text("[", style=_get_style(level)) + level)

        # Get the log time and format it
        log_time = log_time or console.get_datetime()
        time_format = time_format or self.time_format
        log_time_display = (
            time_format(log_time)
            if callable(time_format)
            else Text(f"{log_time.strftime(time_format)}]", style=_get_style(level))
        )
        # add the log time to the row
        row.append(log_time_display)

        # Add the renderables to the row
        row.append(Renderables(renderables))

        # Create the path text and add it to the row
        path_text = Text()
        path_text.append(path, style=f"link file://{link_path}" if link_path else "")
        if line_no:
            path_text.append(":")
            path_text.append(
                f"{line_no}", style=f"link file://{link_path}#{line_no}" if link_path else ""
            )
        # add the path text to the row
        row.append(path_text)

        # Add the row to the table and return it
        output.add_row(*row)

        return output


class _RichHandler(RichHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._log_render = _LogRender(time_format=kwargs.get("log_time_format"))
        self.setFormatter(logging.Formatter("[bold]%(name)s[/] - %(message)s"))


def get_logger(
    name: str | None = None,
    log_level: str | int | None = None,
    log: pathlib.Path | str | bool | None = None,
) -> logging.Logger:
    """
    Get a logger with the given name.

    Parameters
    ----------
    name : str
        The name of the logger.
    log_level : str | int, optional
        The log level of the logger.
    log : pathlib.Path | str | bool, optional
        Sets the logging behavior. Values may be a path for logs to be written
        to, `True` to log to stdout/stderr, or `False` to turn off logging
        completely, by default `False`.

    Returns
    -------
    logging.Logger
        The logger with the given name.
    """
    log_level = log_level or os.getenv("SKEIN_LOG_LEVEL")
    log = log if log is not None else (True if log_level is not None else False)

    # get logger with given name
    _logger = logging.getLogger(name or LIBRARY_NAME)
    _logger.propagate = False

    # check if we have to setup a new handler
    if (
        # check if logger has no handlers
        not _logger.hasHandlers()
        # check if logger has a file handler but we want to log to stdout
        or (
            isinstance(log, str | pathlib.Path)
            and not isinstance(_logger.handlers[0], logging.FileHandler)
        )
        # check if logger has a stdout handler but we want to log to a file
        or (isinstance(log, bool) and not isinstance(_logger.handlers[0], _RichHandler))
    ):
        if isinstance(log, str | pathlib.Path):
            # clear log handlers
            _file_handler = logging.FileHandler(log)
            _file_handler.setFormatter(
                logging.Formatter(
                    "[%(levelname)s %(asctime)s %(name)s] : %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
            )
            _logger.addHandler(_file_handler)
        else:
            # initialize log handler
            _stdout_handler = _RichHandler(
                rich_tracebacks=True,
                console=Console(
                    color_system="truecolor",
                    theme=_logging_theme,
                    width=150 if _is_jupyter() else None,
                ),
                log_time_format="%Y-%m-%d %H:%M:%S",
                markup=True,
            )

            _logger.addHandler(_stdout_handler)

    if not log:
        _logger.setLevel(logging.WARNING)
        return _logger

    else:
        _logger.setLevel(log_level or logging.INFO)

    return _logger
