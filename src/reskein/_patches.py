#!/usr/bin/env python3
"""
model.py
========


"""

from __future__ import annotations

__author__ = "jslorrma"
__maintainer__ = "jslorrma"
__email__ = "jslorrma@gmail.com"


import os
import pathlib
import re
from collections.abc import Mapping
from io import StringIO
from urllib.parse import urlparse


# patching old skein versions missing Model for ApplicationLogs
class ApplicationLogs(Mapping):
    """A mapping of `yarn_container_id` to their logs for an application"""

    def __init__(self, app_id, logs):
        self.app_id = app_id
        self.logs = logs

    def __getitem__(self, k):
        return self.logs[k]

    def __iter__(self):
        return iter(self.logs)

    def __len__(self):
        return len(self.logs)

    def __repr__(self):
        return "ApplicationLogs<%s>" % self.app_id

    def _ipython_key_completions_(self):
        return list(self.logs)

    def _repr_html_(self):
        elements = ["<h3 style='margin-bottom: 10px'>%s</h3>\n" % self.app_id]
        elements.extend(
            f"<details>\n<summary style='display:list-item'>{title}</summary>\n"
            f"<pre><code>\n{log}\n\n</code></pre>\n"
            "</details>"
            for title, log in sorted(self.logs.items())
        )
        return "\n".join(elements)

    def dump(self, file=None):
        """Write the logs to a file or stdout.

        Parameters
        ----------
        file : file-like, optional
            A file-like object to write the logs to. Defaults to ``sys.stdout``.
        """
        if file is not None:
            write = lambda s: print(s, file=file)
        else:
            write = print
        N = len(self.logs) - 1
        write("** Logs for %s **" % self.app_id)
        write("")
        for n, (k, v) in enumerate(sorted(self.logs.items())):
            write(k)
            write("=" * len(k))
            write(v)
            if n < N:
                write("")

    def dumps(self):
        """Write the logs to a string."""
        s = StringIO()
        self.dump(s)
        s.seek(0)
        return s.read()


# monkey patch skein.model.File's _normpath to properly
# handle windows paths


@staticmethod
def _normpath(path: str, origin: str | None = None):
    """
    Normalize a path, handling Windows paths and adding a file scheme if not
    present.

    Parameters
    ----------
    path : str
        The path to be normalized.
    origin : str, optional
        The origin path to be joined with `path` if `path` is not absolute.

    Returns
    -------
    str
        The normalized path.

    Notes
    -----
    This function is a monkey patch for skein.model.File's _normpath to properly
    handle windows paths.
    """
    _has_scheme = lambda path: len(re.findall("([a-zA-Z0-9\-\_\.]*)://", path)) > 0
    if not _has_scheme(path):
        path = "file://%s" % (pathlib.Path(path).absolute().as_posix())

    url = urlparse(path)
    if not url.scheme:
        if not os.path.isabs(url.path):
            if origin is not None:
                path = os.path.normpath(os.path.join(origin, url.path))
            else:
                path = os.path.abspath(url.path)
        else:
            path = url.path
        return f"file://{url.netloc}{path}"
    return path
