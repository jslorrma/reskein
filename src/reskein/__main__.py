#!/usr/bin/env python3
"""
Reskein
=======

`Reskein` is an application and library that that adjusts and uses jcrist's
[Skein][1] library to simplify the deployment of applications on Apache Hadoop
cluster using [YARN ResourceManager REST API][2].

Features:
---------
- **Driver Implementation**: The `Driver` is implemented in Python using Apache
  Hadoop's YARN' ResourceManager REST API, as opposed to the traditional Java
  implementation using Apache Hadoop's gRPC-based Java API.
- **No Java Requirement**: There's no need to install Java or set up a Java
  Virtual Machine (JVM). Additionally, the `Driver` doesn't need to run in a
  separate thread or process.
- **Driver Initialization**: The `Driver` is initialized inside the `Client`,
  enhancing efficiency and ease of use.

[1]: https://github.com/jcrist/skein
[2]: https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
"""

from __future__ import annotations

__author__ = "jslorrma"
__maintainer__ = "jslorrma"
__email__ = "jslorrma@gmail.com"

import os
import pathlib
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import rich_click as click
from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich.traceback import install
from skein.exceptions import ApplicationNotRunningError, ConnectionError
from skein.model import ContainerState
from skein.utils import humanize_timedelta

from . import CONFIG_DIR, get_logger
from .client import Client
from .common.exceptions import DriverError
from .driver import Driver, load_driver_env_vars, read_driver, write_driver_env_vars

if TYPE_CHECKING:
    from skein.model import ApplicationReport, Container

# install rich traceback
install(show_locals=True, max_frames=5)

# click rich configuration
# click.rich_click.USE_MARKDOWN = True
click.rich_click.USE_RICH_MARKUP = True

logger = get_logger(__name__)

# set terminal width
_TERMINAL_WIDTH = os.get_terminal_size().columns

#
# validation functions
#
def _validate_config_dir(ctx, param, value):
    if value is None:
        return value
    value = pathlib.Path(value).resolve().absolute()
    if not all(
        (value / file).exists() for file in ("core-site.xml", "yarn-site.xml", "hdfs-site.xml")
    ):
        raise click.BadParameter(
            "Invalid configuration directory. Please provide a directory containing the "
            "`core-site.xml`, `yarn-site.xml`, and `hdfs-site.xml` files."
        )
    return value


def _validate_url(ctx, param, value):
    if value is None:
        return value
    parsed = urlparse(value)
    if not all([parsed.scheme, parsed.netloc]):
        raise click.BadParameter("Invalid url")
    return value


def _validate_log(ctx, param, value):
    if isinstance(value, str):
        if value.lower() in ("true", "1", "on"):
            return True
        elif value.lower() in ("false", "0", "off"):
            return False
        else:
            return pathlib.Path(value).resolve().absolute()
    return value


# create a output table
def _make_output_table(title: str, header: list[tuple[str, dict]], rows: list[list[str]], **kwargs):
    """
    Create a table for output.

    Parameters
    ----------
    title : str
        The title of the table.
    header : list[tuple[str, dict]]
        The header of the table. Each tuple contains the column name and a
        dictionary of keyword arguments to pass to `rich.table.Table.add_column`.
    rows : list[list[str]]
        The rows of the table. Each list contains the values for a row.

    Returns
    -------
    table : rich.table.Table
        The table.
    """
    table = Table(
        show_header=any(h[0] for h in header),
        header_style="bold deep_sky_blue1",
        show_lines=False,
        box=None,
        title=title,
        min_width=min(120, _TERMINAL_WIDTH),
        title_justify="left",
        expand=True,
        **kwargs,
    )
    for i, (col, header_kwargs) in enumerate(header):
        if "style" not in header_kwargs:
            header_kwargs["style"] = "dim" if i == 0 else None
        table.add_column(col, **header_kwargs)
    for row in rows:
        table.add_row(*row)
    return table


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "-v",
    "--verbose",
    count=True,
    help="Increase verbosity (can be passed multiple times)",
)
@click.option(
    "-c",
    "--hadoop-conf",
    type=click.Path(exists=True, file_okay=False, resolve_path=True),
    callback=_validate_config_dir,
    help=(
        "The path to the Hadoop configuration directory, where the "
        "`{core, yarn, hdfs}-site.xml` files can be found. If not provided, "
        "the Hadoop configuration directory can be be set via environment "
        "variables `HADOOP_CONF_DIR`, `YARN_CONF_DIR`, or `HDFS_CONF_DIR`."
    ),
)
@click.version_option(prog_name="reskein")
def cli(verbose: int, hadoop_conf: pathlib.Path | None):
    """
    Reskein is a simple CLI tool for deploying applications on Apache Hadoop
    cluster using via YARN ResourceManager REST API and jcrist's Skein library.

    You can try using --help at the top level and also for
    specific subcommands.
    """
    if verbose > 0:
        os.environ["SKEIN_LOG_LEVEL"] = "INFO" if verbose == 1 else "DEBUG"

    if hadoop_conf:
        os.environ["HADOOP_CONF_DIR"] = f"{hadoop_conf.resolve().absolute()}"

    # in case SKEIN_LOG_LEVEL is got set with context, update logger
    globals().update({"logger": get_logger()})


##
## ENTRYPINTS GROUPS
##
@cli.group(context_settings={"help_option_names": ["-h", "--help"]})
def application():
    """
    Manage applications.
    """


@cli.group(context_settings={"help_option_names": ["-h", "--help"]})
def container():
    """
    Manage application containers.
    """


@cli.group(context_settings={"help_option_names": ["-h", "--help"]})
def kv():
    """
    Manage the key-value store.
    """


@cli.group(context_settings={"help_option_names": ["-h", "--help"]})
def driver():
    """
    Manage the reskein driver.

    NOTE: Driver (re)start will cache also relevant environment variables,
    verbosity level and hadoop configuration directory.
    """


@cli.group(context_settings={"help_option_names": ["-h", "--help"]})
def config():
    """
    Manage configuration.
    """


################
## DRIVER
################
def _driver_info(context: str | None = None):
    from .hadoop.config import HadoopConfig

    # read the driver configuration and load the environment variables
    load_driver_env_vars()
    info = read_driver()
    env_vars = {
        env_key: os.getenv(env_key)
        for env_key in os.environ
        if any(key in env_key for key in ("SKEIN", "HADOOP", "KRB5", "YARN", "HDFS"))
    }

    # Create a console for rich output
    console = Console()

    # Create a table
    _rows = []
    _footnote = Text("", style="dim")
    if "rm_address" not in info:
        try:
            _rows.append(
                (
                    "rm_address*",
                    os.getenv("SKEIN_DRIVER_RM", HadoopConfig().resource_manager_service),
                )
            )
            _footnote = Text("*) from hadoop config", style="dim")
        except FileNotFoundError:
            ...

    if "hdfs_address" not in info:
        try:
            _rows.append(
                (
                    "hdfs_address*",
                    os.getenv(
                        "SKEIN_DRIVER_HDFS", ",".join(HadoopConfig().name_node_webhdfs_addresses)
                    ),
                )
            )
            _footnote = Text("*) from hadoop config", style="italic dim")
        except FileNotFoundError:
            ...

    # Add rows to the table
    for key, value in info.items():
        _rows.append((key, str(value)))

    if _footnote._text:
        _rows.append((_footnote, ""))

    if env_vars:
        _env_rows = [(key, str(value)) for key, value in env_vars.items()]

    # determine the width of the first column
    _first_col_width = max(len(row[0]) for row in _rows + (_env_rows if env_vars else [])) + 2

    drv_tbl = _make_output_table(
        title="Driver Info",
        header=[
            ("Argument", {"overflow": "fold", "width": _first_col_width}),
            ("Value", {"overflow": "fold", "width": _TERMINAL_WIDTH-_first_col_width}),
        ],
        rows=_rows,
    )
    console.print(drv_tbl, "")

    if env_vars:
        env_tbl = _make_output_table(
            title="Environment Variables",
            header=[
                ("Variable", {"overflow": "fold", "width": _first_col_width}),
                ("Value", {"overflow": "fold", "width": _TERMINAL_WIDTH-_first_col_width}),
            ],
            rows=_env_rows,
        )
        console.print(env_tbl)


@driver.command(name="stop", help="Stop the reskein driver")
def driver_stop():
    Driver.stop_global_driver()
    click.echo("Driver stopped")


@driver.command(name="info", help="The paremeters used to initialize the driver")
@click.option(
    "-c",
    "--context",
    type=str,
    default=None,
    help=(
        "The name of the context to read the driver configuration from. If not "
        "provided, current context is used."
    ),
)
def driver_info(context: str | None = None):
    _driver_info(context=context)


@driver.command(
    name="start",
    help="Start the reskein driver",
)
@click.option(
    "--rm-address",
    metavar="URL",
    callback=_validate_url,
    help=(
        "The ResourceManager HTTP(S) service address. If not provided, the "
        "address is read from the `SKEIN_DRIVER_RM` environment variable or "
        "the Hadoop configuration."
    ),
)
@click.option(
    "--hdfs-address",
    metavar="URL",
    callback=_validate_url,
    help=(
        "The (Web)HDFS HTTP(S) service address. If not provided, the address "
        "is read from the `SKEIN_DRIVER_HDFS` environment variable or the "
        "Hadoop configuration."
    ),
)
@click.option(
    "--auth",
    type=click.Choice(["simple", "kerberos"]),
    default=None,
    help=(
        "The authentication method used by the `Driver` to authenticate with "
        "the Hadoop cluster. It can be `'kerberos'`, `'simple'`. "
        "If not provided, the authentication method is read from the "
        "`SKEIN_DRIVER_AUTH` environment variable or derived from the "
        "Hadoop configuration."
    ),
)
@click.option(
    "--timeout",
    type=int,
    default=None,
    help=(
        "The number of seconds to wait for the server to send data before "
        "giving up, by default `90` (seconds)."
    ),
)
@click.option(
    "--verify",
    type=click.BOOL,
    default=None,
    help=(
        "Controls whether we verify the server's TLS certificate, or a path to "
        "a CA bundle to use, by default `True`."
    ),
)
@click.option(
    "--proxy",
    metavar="URL",
    default=None,
    help=("The URL of the proxy to use."),
)
@click.option(
    "--user",
    default=None,
    help=(
        "The username for simple authentication. If not given, the value of "
        "`SKEIN_USER_NAME` environment variable or the current system user's "
        "username is used."
    ),
)
@click.option(
    "--keytab",
    metavar="PATH",
    default=None,
    help=(
        "The path to the keytab file for Kerberos authentication. If not "
        "provided, the path is read from the `SKEIN_DRIVER_KEYTAB` "
        "environment variable."
    ),
)
@click.option(
    "--principal",
    default=None,
    help=(
        "The principal for Kerberos authentication. If not provided, the "
        "principal is read from the `SKEIN_DRIVER_PRINCIPAL` environment "
        "variable."
    ),
)
@click.option(
    "--log",
    metavar="{PATH, True}",
    callback=_validate_log,
    default=None,
    help=(
        "Whether to log the driver output to the console or a file. Option "
        "takes a path to a file or True to log to the console). "
    ),
)
def driver_start(  # noqa: PLR0913
    rm_address: str | None = None,
    hdfs_address: str | None = None,
    auth: str | None = None,
    timeout: int | None = None,
    verify: bool | str | None = None,
    proxy: str | None = None,
    keytab: str | None = None,
    principal: str | None = None,
    user: str | None = None,
    log: pathlib.Path | bool | None = None,
):
    try:
        Driver.start_global_driver(
            rm_address=rm_address,
            hdfs_address=hdfs_address,
            auth=auth,
            timeout=timeout,
            verify=verify,
            proxy={"http": proxy, "https": proxy} if proxy else None,
            user=user,
            keytab=keytab,
            principal=principal,
            log=log,
            log_level=os.getenv("SKEIN_LOG_LEVEL", None),
        )
        Console().print("Driver started with context with configuration:\n")
        _driver_info()
    except DriverError as e:
        logger.error(f"Starting driver failed: {e}")


@driver.command(name="restart", help="Restart (stop and start) the reskein driver")
def driver_restart(  # noqa: PLR0913
    rm_address: str | None = None,
    hdfs_address: str | None = None,
    auth: str | None = None,
    timeout: int | None = None,
    verify: bool | str | None = None,
    proxy: str | None = None,
    keytab: str | None = None,
    principal: str | None = None,
    user: str | None = None,
    log: pathlib.Path | bool | None = None,
):
    Driver.stop_global_driver(force=True)
    try:
        Driver.start_global_driver(
            rm_address=rm_address,
            hdfs_address=hdfs_address,
            auth=auth,
            timeout=timeout,
            verify=verify,
            proxy={"http": proxy, "https": proxy} if proxy else None,
            user=user,
            keytab=keytab,
            principal=principal,
            log=log,
            log_level=os.getenv("SKEIN_LOG_LEVEL", None),
        )
        Console().print("Driver restarted with context configuration:\n")
        _driver_info()
    except DriverError as e:
        logger.error(f"Restarting driver failed: {e}")


driver_restart.params = driver_start.params


################
## APPLICATION
################
def _print_application_status(
    apps: list[ApplicationReport], app_id: str | None = None, full: bool = False
):
    header = [
        "application_id",
        "name",
        "state",
        "status",
        "containers",
        "vcores",
        "memory",
        "runtime",
        "address",
    ]
    if full:
        header.extend(["user", "queue", "progress", "tracking_url"])

    _rows = []
    for app in apps:
        _rows.append(
            [
                app.id,
                app.name,
                str(app.state),
                str(app.final_status),
                str(app.usage.num_used_containers),
                str(app.usage.used_resources.vcores),
                str(app.usage.used_resources.memory),
                humanize_timedelta(app.runtime),
                f"{app.host}:{app.port}",
            ]
        )
        if full:
            _rows[-1].extend([app.user, app.queue, str(app.progress), app.tracking_url])

    table = _make_output_table(
        title=f"Application Status for [dim]{app_id}[/]" if app_id else "Application Status",
        header=[(h, {}) for h in header],
        rows=_rows,
    )

    # Create a console for rich output
    console = Console()
    console.print(table)


@application.command(name="submit", help="Submit a Skein Application")
@click.argument(
    "spec", type=click.Path(exists=True, file_okay=True, dir_okay=False, resolve_path=True)
)
def application_submit(spec: pathlib.Path):
    with Client(log_level=os.getenv("SKEIN_LOG_LEVEL", "INFO")) as c:
        c.submit(str(spec))


@application.command(name="ls", help="List applications")
@click.option(
    "--all",
    "-a",
    is_flag=True,
    help=("Show applications in all states (default is only active applications)."),
)
@click.option(
    "--state",
    "-s",
    multiple=True,
    type=click.Choice(["accepted", "running", "finished", "failed", "killed"]),
    help=("Select applications with this state. May be repeated to filter on multiple states."),
)
@click.option(
    "--name",
    type=str,
    help="Select applications with this name.",
)
@click.option(
    "--queue",
    type=str,
    help="Select applications with this queue.",
)
@click.option(
    "--user",
    type=str,
    help="Select applications with this user.",
)
@click.option(
    "--started-begin",
    type=str,
    help=(
        "Select applications that started after this datetime (inclusive). "
        "Accepts several date and time formats (e.g. `YYYY-M-D H:M:S` or "
        "`H:M`). See the documentation for more information."
    ),
)
@click.option(
    "--started-end",
    type=str,
    help=("Select applications that started before this datetime (inclusive)"),
)
@click.option(
    "--finished-begin",
    type=str,
    help=("Select applications that finished after this datetime (inclusive)"),
)
@click.option(
    "--finished-end",
    type=str,
    help=("Select applications that finished before this datetime (inclusive)"),
)
@click.option(
    "--full",
    "-f",
    is_flag=True,
    help=("Show all application information."),
)
def application_ls(  # noqa: PLR0913
    all: bool = False,
    state: tuple[str] | None = None,
    name: str | None = None,
    queue: str | None = None,
    user: str | None = None,
    started_begin: str | None = None,
    started_end: str | None = None,
    finished_begin: str | None = None,
    finished_end: str | None = None,
    full: bool = False,
):
    with Client(log_level=os.getenv("SKEIN_LOG_LEVEL", "INFO")) as c:
        apps = c.get_applications(
            states=state,
            name=name,
            queue=queue,
            user=user,
            started_begin=started_begin,
            started_end=started_end,
            finished_begin=finished_begin,
            finished_end=finished_end,
        )
        _print_application_status(apps, full=full)


@application.command(name="status", help="Status of a Skein application")
@click.argument("app_id", type=str)
@click.option(
    "--full",
    "-f",
    is_flag=True,
    help=("Show all application information."),
)
def application_status(app_id: str, full: bool = False):
    with Client(log_level=os.getenv("SKEIN_LOG_LEVEL", "INFO")) as c:
        apps = c.application_report(app_id)
        _print_application_status([apps], app_id, full=full)


@application.command(name="logs", help="Get the logs from a completed Skein application")
@click.argument("app_id", type=str)
@click.option(
    "--user",
    default="",
    type=str,
    help=(
        "The user to get the application logs as. Requires the "
        "current user to have permissions to proxy as ``user``. "
        "Default is the current user."
    ),
)
def application_logs(app_id: str, user: str = ""):
    def dump(logs):
        """Write the logs to a file or stdout.

        Parameters
        ----------
        file : file-like, optional
            A file-like object to write the logs to. Defaults to ``sys.stdout``.
        """
        from rich.highlighter import NullHighlighter
        from rich.rule import Rule

        console = Console(highlighter=NullHighlighter())

        console.print(f"Application logs for [dim]{app_id}[/]:", justify="left")
        console.print("")
        for k, v in sorted(logs.items()):
            console.print(
                Rule(style="dim"),
            )
            console.print(Text(k, style="bold deep_sky_blue1"), justify="left")
            console.print("")
            console.print(v.strip())
            console.print("")

    with Client(log_level=os.getenv("SKEIN_LOG_LEVEL", "INFO")) as c:
        _logs = c.application_logs(app_id, user=user)
        dump(_logs)


@application.command(name="kill", help="Kill a Skein application")
@click.argument("app_id", type=str)
@click.option(
    "--user",
    default="",
    type=str,
    help=(
        "The user to kill the application as. Requires the "
        "current user to have permissions to proxy as `user`. "
        "Default is the current user."
    ),
)
def application_kill(app_id: str, user: str = ""):
    with Client(log_level=os.getenv("SKEIN_LOG_LEVEL", "INFO")) as c:
        try:
            c.kill_application(app_id, user=user)
        except DriverError as e:
            logger.error(f"Killing application {app_id} failed: {e}")


@application.command(name="specification", help="Get specification for a running skein application")
@click.argument("app_id", type=str)
def application_specification(app_id: str):
    with Client(log_level=os.getenv("SKEIN_LOG_LEVEL", "INFO")) as c:
        try:
            app = c.connect(app_id)
            spec = app.get_specification()
        except (ApplicationNotRunningError, ConnectionError) as e:
            logger.error(f"Getting application specification for {app_id} failed: {e}")
            click.Abort()

    console = Console()
    console.print(spec.to_yaml(skip_nulls=True))


@application.command(name="mv", help="Move a Skein application to a different queue")
@click.argument("app_id", type=str)
@click.argument("queue", type=str)
def application_move(app_id: str, queue: str):
    with Client(log_level=os.getenv("SKEIN_LOG_LEVEL", "INFO")) as c:
        try:
            c.move_application(app_id, queue)
        except Exception as e:
            logger.error(f"Moving application {app_id} failed: {e}")


@application.command(name="shutdown", help="Shutdown a Skein application")
@click.argument("app_id", type=str)
@click.option(
    "--status",
    default="SUCCEEDED",
    type=str,
    help=("Final Application Status. Default is SUCCEEDED"),
)
@click.option(
    "--diagnostics",
    default=None,
    type=str,
    help=("The application diagnostic exit message. If not provided, a default will be used."),
)
def application_shutdown(app_id: str, status: str = "SUCCEEDED", diagnostics: str | None = None):
    with Client(log_level=os.getenv("SKEIN_LOG_LEVEL", "INFO")) as c:
        try:
            app = c.connect(app_id)
            app.shutdown(status, diagnostics)
        except (ApplicationNotRunningError, ConnectionError) as e:
            logger.error(f"Shutting down application {app_id} failed: {e}")


################
## CONTAINER
################
def _print_container_status(containers: list[Container], app_id: str, full: bool = False):
    header = ["service", "id", "state", "runtime"]
    if full:
        header.extend(["instance", "yarn_container_id", "exit_message", "yarn_node_http_address"])

    _rows = []
    for c in containers:
        _rows.append([c.service_name, c.id, str(c.state), humanize_timedelta(c.runtime)])
        if full:
            _rows[-1].extend(
                [str(c.instance), c.yarn_container_id, c.exit_message, c.yarn_node_http_address]
            )

    table = _make_output_table(
        title=f"Container Status for [dim]{app_id}[/]",
        header=[(h, {}) for h in header],
        rows=_rows,
    )
    # Create a console for rich output
    console = Console()
    console.print(table)


@container.command(name="ls", help="List containers")
@click.argument("app_id", type=str)
@click.option(
    "--all",
    "-a",
    is_flag=True,
    help=("Show containers in all states (default is only active containers)."),
)
@click.option(
    "--service",
    "-s",
    multiple=True,
    type=str,
    help=("Select containers with this service. May be repeated to filter on multiple services."),
)
@click.option(
    "--state",
    "-s",
    multiple=True,
    type=click.Choice(["accepted", "running", "finished", "failed", "killed"]),
    help=("Select containers with this state. May be repeated to filter on multiple states."),
)
@click.option(
    "--full",
    "-f",
    is_flag=True,
    help=("Show all container information."),
)
def container_ls(
    app_id: str,
    all: bool = False,
    service: tuple[str] | None = None,
    state: tuple[str] | None = None,
    full: bool = False,
):
    if all:
        state = tuple(ContainerState)
    with Client(log_level=os.getenv("SKEIN_LOG_LEVEL", "INFO")) as c:
        try:
            app = c.connect(app_id)
            containers = app.get_containers(states=state, services=service)
            _print_container_status(containers, app_id, full=full)
        except (ApplicationNotRunningError, ConnectionError) as e:
            logger.error(f"Listing containers for application {app_id} failed: {e}")


@container.command(name="kill", help="Kill a container")
@click.argument("app_id", type=str)
@click.argument("container_id", type=str)
def container_kill(app_id: str, container_id: str):
    with Client(log_level=os.getenv("SKEIN_LOG_LEVEL", "INFO")) as c:
        try:
            app = c.connect(app_id)
            app.kill_container(container_id)
        except (ApplicationNotRunningError, ConnectionError) as e:
            logger.error(f"Killing container {container_id} for application {app_id} failed: {e}")


@container.command(name="scale", help="Scale a service to a requested number of containers")
@click.argument("app_id", type=str)
@click.option(
    "--service",
    "-s",
    required=True,
    help="Service name",
)
@click.option(
    "--number",
    "-n",
    type=int,
    required=True,
    help="The requested number of instances",
)
def container_scale(app_id: str, service: str, number: int):
    with Client(log_level=os.getenv("SKEIN_LOG_LEVEL", "INFO")) as c:
        try:
            app = c.connect(app_id)
            app.scale(service, number)
        except (ApplicationNotRunningError, ConnectionError) as e:
            logger.error(
                f"Scaling service {service} to {number} for application {app_id} failed: {e}"
            )


################
## KEYSTORE
################


@kv.command(name="get", help="Get a value from the key-value store")
@click.argument("app_id", type=str)
@click.option(
    "--key",
    required=True,
    help="The key to get",
)
@click.option(
    "--wait",
    is_flag=True,
    help="If true, will block until the key is set",
)
def kv_get(app_id: str, key: str, wait: bool = False):
    with Client(log_level=os.getenv("SKEIN_LOG_LEVEL", "INFO")) as c:
        try:
            app = c.connect(app_id)
            result = app.kv.wait(key) if wait else app.kv[key]
            click.echo(result)
        except (ApplicationNotRunningError, ConnectionError) as e:
            logger.error(f"Getting key {key} for application {app_id} failed: {e}")
            click.Abort()


@kv.command(name="put", help="Put a value in the key-value store")
@click.argument("app_id", type=str)
@click.option(
    "--key",
    required=True,
    help="The key to put",
)
@click.option(
    "--value",
    type=str,
    required=False,
    default=None,
    help="The value to put. If not provided, will be read from stdin.",
)
def kv_put(app_id: str, key: str, value: str | None = None):
    with Client(log_level=os.getenv("SKEIN_LOG_LEVEL", "INFO")) as c:
        try:
            app = c.connect(app_id)
            if value is None:
                import sys

                app.kv[key] = sys.stdin.buffer.readline().strip()
            else:
                app.kv[key] = value.encode()
        except (ApplicationNotRunningError, ConnectionError) as e:
            logger.error(f"Putting key {key} for application {app_id} failed: {e}")
            click.Abort()


@kv.command(name="del", help="Delete a value from the key-value store")
@click.argument("app_id", type=str)
@click.option(
    "--key",
    required=True,
    help="The key to delete",
)
def kv_del(app_id: str, key: str):
    with Client(log_level=os.getenv("SKEIN_LOG_LEVEL", "INFO")) as c:
        try:
            app = c.connect(app_id)
            del app.kv[key]
        except (ApplicationNotRunningError, ConnectionError) as e:
            logger.error(f"Deleting key {key} for application {app_id} failed: {e}")
            click.Abort()


@kv.command(name="ls", help="List all keys in the key-value store")
@click.argument("app_id", type=str)
@click.option(
    "--full",
    "-f",
    is_flag=True,
    help=("Show all key-value pairs."),
)
def kv_ls(app_id: str, full: bool = False):
    with Client(log_level=os.getenv("SKEIN_LOG_LEVEL", "INFO")) as c:
        try:
            app = c.connect(app_id)
            keys = sorted(app.kv)
            if keys:
                if full:
                    kv_tbl = _make_output_table(
                        title=f"Key-Value Store for [dim]{app_id}[/]",
                        header=[
                            ("Key", {"overflow": "fold", "width": 30}),
                            ("Value", {"overflow": "fold", "min_width": 100}),
                        ],
                        rows=[(key, app.kv[key].decode()) for key in keys],
                    )
                    Console().print(kv_tbl)
                else:
                    click.echo("\n".join(keys))
        except (ApplicationNotRunningError, ConnectionError) as e:
            logger.error(f"Listing keys for application {app_id} failed: {e}")
            click.Abort()


###############
## CONFIG
###############
# @config.command(name="ls", help="List all contexts")
# def config_ls():
#     from .common.context import CONTEXT, Context

#     contexts = Context.list(with_paths=True)
#     if contexts:
#         ctx_tbl = _make_output_table(
#             title="Contexts",
#             header=[
#                 ("Name", {"overflow": "fold", "width": 30}),
#                 ("Path", {"overflow": "fold", "min_width": 100}),
#             ],
#             rows=[
#                 *[(name if name != CONTEXT.name else f"{name}*", path) for name, path in contexts],
#                 *[("*) active context", "")],
#             ],
#         )
#         Console().print(ctx_tbl)


# @config.command(name="show", help="Show context configuration")
# @click.option(
#     "--context",
#     type=str,
#     default=None,
#     help=(
#         "The name of the context to show the configuration for. If not "
#         "provided, current context is used."
#     ),
# )
# def config_show(context: str | None = None):
#     from .common.context import CONTEXT

#     Console().print(f"Configuration for context [dim]{context or CONTEXT.name}[/]:\n")
#     _driver_info(context=context)


# @config.command(name="activate", help="Activate a context")
# @click.argument("name", type=str)
# def config_activate(name: str):
#     from .common.context import CONTEXT

#     CONTEXT.activate(name)


# @config.command(name="deactivate", help="Deactivate the current context")
# def config_deactivate():
#     from .common.context import CONTEXT

#     CONTEXT.reset_context()


# @config.command(name="rm", help="Remove a context")
# @click.argument("name", type=str)
# def config_rm(name: str):
#     from .common.context import CONTEXT

#     CONTEXT.remove(name)


# @config.command(name="save", help="Save current context")
# @click.argument("name", type=str)
# def config_save(name: str):
#     from .common.context import CONTEXT

#     CONTEXT.save(name)


# @config.command(name="get", help="Get a configuration value")
# @click.option(
#     "--key",
#     "-k",
#     type=str,
#     help=(
#         "The name of the configuration value to get. If the key is a driver "
#         "argument, it will be read from the driver configuration. Otherwise, "
#         "it will be read from the environment variables."
#     ),
# )
# @click.option(
#     "--context",
#     "-c",
#     type=str,
#     default=None,
#     help=(
#         "The name of the context to get the configuration value for. If not "
#         "provided, current context is used."
#     ),
# )
# def config_get(key: str, context: str | None = None):
#     import inspect

#     from .common.context import CONFIG_DIR, CONTEXT

#     with Context(context):
#         if key in inspect.signature(Client.__init__).parameters:
#             click.echo(CONTEXT.read_driver()[key])
#         else:
#             click.echo((dotenv_values(CONFIG_DIR / ".env") | dotenv_values()).get(key))


# @config.command(name="set", help="Set a configuration value")
# @click.option(
#     "--key",
#     "-k",
#     type=str,
#     help=(
#         "The name of the configuration value to set. If the key is a driver "
#         "argument, it will be set in the driver configuration. Otherwise, it "
#         "will be set in the environment variables."
#     ),
# )
# @click.option(
#     "--value",
#     "-v",
#     type=str,
#     help=("The value to set. If not provided, the value is read from stdin."),
# )
# @click.option(
#     "--context",
#     "-c",
#     type=str,
#     default=None,
#     help=(
#         "The name of the context to set the configuration value for. If not "
#         "provided, current context is used."
#     ),
# )
# def config_set(key: str, value: str, context: str | None = None):
#     import inspect

#     from .common.context import CONTEXT, Context

#     with Context(context):
#         # determine if key is a driver argument or a environment variable
#         if key in inspect.signature(Client.__init__).parameters:
#             CONTEXT.write_driver(None, **(CONTEXT.read_driver() | {key: value}))
#         else:
#             CONTEXT.write_env_vars(**{key: value})

#         # CONTEXT.save(context)


@config.command(
    name="gencerts",
    help=(
        "Generate security credentials. Creates a self-signed TLS "
        "key/certificate pair for securing Skein communication, and writes "
        "it to the skein configuration directory ('~.skein/' by default)."
    ),
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help=("Overwrite existing configuration."),
)
def config_gencerts(force: bool = False):
    from skein import Security

    from . import CONFIG_DIR

    try:
        sec = Security.new_credentials()
        sec.to_directory(directory=CONFIG_DIR, force=force)
    except FileExistsError:
        logger.error(
            f"Security credentials already exist in {CONFIG_DIR}. Use --force to overwrite."
        )
        click.Abort()


@config.command(name="get-hadoop-config", help=(
    "Get a hadoop configuration from a hadoop cluster. This command connects to a hadoop cluster "
    "anad retrieves necessary configuration files to interact with the cluster. The configuration "
    f"is saved in {CONFIG_DIR / 'hadoopconf'} directory."
    )
)
@click.argument("address", metavar="URL", callback=_validate_url)
@click.option(
    "--auth",
    type=click.Choice(["simple", "kerberos"]),
    default="simple",
    help=(
        "The authentication method used by the `Driver` to authenticate with "
        "the Hadoop cluster. It can be `'kerberos'`, `'simple'`. "
        "If not provided, the authentication method is read from the "
        "`SKEIN_DRIVER_AUTH` environment variable or derived from the "
        "Hadoop configuration."
    ),
)
@click.option(
    "--verify",
    type=click.BOOL,
    default=None,
    help=(
        "Controls whether we verify the server's TLS certificate, or a path to "
        "a CA bundle to use, by default `True`."
    ),
)
@click.option(
    "--proxy",
    metavar="URL",
    default=None,
    help=("The URL of the proxy to use."),
)
@click.option(
    "--user",
    default=None,
    help=(
        "The username for simple authentication. If not given, the value of "
        "`SKEIN_USER_NAME` environment variable or the current system user's "
        "username is used."
    ),
)
@click.option(
    "--keytab",
    metavar="PATH",
    default=None,
    help=(
        "The path to the keytab file for Kerberos authentication. If not "
        "provided, the path is read from the `SKEIN_DRIVER_KEYTAB` "
        "environment variable."
    ),
)
@click.option(
    "--principal",
    default=None,
    help=(
        "The principal for Kerberos authentication. If not provided, the "
        "principal is read from the `SKEIN_DRIVER_PRINCIPAL` environment "
        "variable."
    ),
)
@click.option(
    "--context",
    type=str,
    default=None,
    help=(
        "The name of the context to save the configuration to. If not "
        "provided, current context is used."
    ),
)
@click.option(
    "--save",
    "-s",
    is_flag=True,
    help=("Save the configuration to the current context."),
)
def config_get_hadoop_config(  # noqa: PLR0913
    address,
    auth: str = "simple",
    verify: bool | str | None = None,
    proxy: str | None = None,
    keytab: str | None = None,
    principal: str | None = None,
    user: str | None = None,
    context: str | None = None,
    save: bool = False,
):
    from .common.kerberos import kinit
    from .hadoop.config import HadoopConfig

    # setup authentication
    if auth.lower() == "simple":
        # lazily import authentication handler
        from .common.auth import HTTPSimpleAuth

        _user = user or os.getenv("SKEIN_USER_NAME") or os.getenv("USER") or os.getenv("USERNAME")
        _auth_handler = HTTPSimpleAuth(username=_user)

        logger.debug("Simple authentication handler set up")
    else:  # Authentication.KERBEROS
        # lazily import authentication handler
        from .common.auth import OPTIONAL, HTTPKerberosAuth

        _auth_handler = HTTPKerberosAuth(
            mutual_authentication=OPTIONAL, sanitize_mutual_error_response=False
        )
        # do kinit
        if principal or keytab:
            kinit(principal, keytab)

        logger.debug("Kerberos authentication handler set up")

    # kwargs
    kwargs = {}
    if verify is not None:
        kwargs["verify"] = verify
    if proxy is not None:
        kwargs["proxy"] = {"http": proxy, "https": proxy}

    try:
        HadoopConfig.from_cluster(
            cluster_master_address=address,
            auth=_auth_handler,
            out_path=CONFIG_DIR / "hadoopconf",
            **kwargs,
        )
        if save:
            write_driver_env_vars(HADOOP_CONF_DIR=CONFIG_DIR / "hadoopconf")
    except Exception as e:
        logger.error(f"Getting hadoop configuration failed: {e}")
        click.Abort()


# main function
main = cli

if __name__ == "__main__":
    main()
