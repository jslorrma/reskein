#!/usr/bin/env python3
"""
common/kerberos.py
==================
"""

from __future__ import annotations

__author__ = "jslorrma"
__maintainer__ = "jslorrma"
__email__ = "jslorrma@gmail.com"

# imports
import getpass
import inspect
import os
import pathlib
import re
import subprocess

from .. import PathType, logger
from .exceptions import context


def principal_from_keytab(keytab: PathType) -> str:
    """Read principal from `keytab` file

    Parameters
    ----------
    keytab : Union[str, pathlib.Path]
        Path to a keytab file to read the principal from.
    """
    if not pathlib.Path(keytab).exists():
        raise context.FileNotFoundError(f"keytab doesn't exist at '{keytab}'")

    _ret = subprocess.run(f"klist -k {keytab}", shell=True, stdout=subprocess.PIPE, check=False)
    if _ret.returncode == 0 and "@" in _ret.stdout.decode():
        try:
            _principal = re.search("[\S]*@[A-Z.]*", _ret.stdout.decode()).group()
            logger.debug(f"Principal found in keytab file '{keytab}': '{_principal}'")
            return _principal
        except AttributeError:  # no principal found in keytab file
            pass


def _get_pass(principal: str | None = None) -> str:
    """Ask the user for kerberos password

    Parameters
    ----------
    principal : str, optional
        The user principal.

    Returns
    -------
    str
        The password for the principal

    Raises
    ------
    UserWarning
        An exception is raised in case the password confirmation fails three
        times in a row.
    """
    _MAX_RETRIES = 3
    _retries = 0
    while _retries < _MAX_RETRIES:
        _pass = getpass.getpass(f"Enter password for '{principal}': ")
        _confirm = getpass.getpass(f"Confirm password for '{principal}': ")
        if _pass == _confirm:
            return _pass
        else:
            print("Passwords do not match! Retrying...")
            _retries += 1

    logger.debug(f"Could not get password for user '{principal}'.")

    raise context.UserWarning(f"Passwords did not match for '{principal}'.")


if os.name == "nt":
    import tempfile

    def _kinit_with_principal(principal: str, password: str | None = None) -> bool:
        """Kinit with principal (Windows)

        The communication of the password via the `input` parameter of
        `subprocess.run` does not work under windows, therefore a temporary
        keytab file is created, which is used when kinit in the subprocess.

        Parameters
        ----------
        principal : str
            The principal to get a tgt for.
        password : str, optional
            The password to use, by default `None`. If `None` prompt user to
            enter password.

        Returns
        -------
        bool
            True if new tgt was acquired

        Raises
        ------
        SubprocessError
            An exception is raised in case kinit fails.
        UserWarning
            An exception is raised in case the password confirmation fails three
            times in a row.
        """
        try:
            # get password
            _pass = password or _get_pass(principal)
            # write temporary keytab file to make password-less kinit
            with tempfile.TemporaryDirectory() as tmp:
                _keytab = pathlib.Path(tmp, ".keytab")
                generate_keytab(principal, _keytab, _pass)

                # get new tgt
                return _kinit_with_keytab(principal, _keytab)

        except subprocess.SubprocessError:
            raise
        except UserWarning:
            raise

    def generate_keytab(
        principal: str,
        path: PathType,
        password: str | None = None,
        encryptions: list[str] | None = None,
    ) -> bool:
        """Generate a keytab file (Windows)

        Parameters
        ----------
        principal : str
            The principal to add to keytab-file
        path : pathlib.Path | str
           The file path where the keytab file will be stored
        password : Optional[str], optional
            The password to encrypt and add to keytab-file, by default `None`. If
            `None` the user is propmpted for the password.
        encryptions : Optional[List[str]], optional
            **Note:** NOT USED ON WINDOWS SYSTEMS
            A list of encryptions to encrypt the password, by default `None`. If
            `None` this encryptions are used: `{"aes128-cts-hmac-sha1-96",
            "aes256-cts-hmac-sha1-96", "aes256-cts", "rc4-hmac"}`

        Returns
        -------
        bool
            True if keytab file got generated

        Raises
        ------
        SubprocessError
            An exception is raised in case keytab generation subprocess fails.
        UserWarning
            An exception is raised in case the password confirmation fails three
            times in a row.
        """

        logger.debug(f"Generating keytab file using principal: {principal} at {path}")
        try:
            _pass = password or _get_pass(principal)

            _r = subprocess.run(
                f'ktab -a "{principal}" "{_pass}" -n 0 -k {path}',
                shell=True,
                capture_output=True,
                check=False,
            )
            if _r.returncode != 0:
                raise context.SubprocessError(_r.stderr.decode())

            return _r.returncode == 0

        except subprocess.SubprocessError:
            raise
        except UserWarning:
            raise

elif os.name == "posix":  # pragma: no cover

    def _kinit_with_principal(principal: str, password: str | None = None) -> bool:
        """Kinit with principal (Unix-like)

        Parameters
        ----------
        principal : str
            The principal to get a tgt for.
        password : str, optional
            The password to use, by default `None`. If `None` prompt user to
            enter password.

        Returns
        -------
        bool
            True if new tgt was acquired

        Raises
        ------
        SubprocessError
            An exception is raised in case kinit fails.
        UserWarning
            An exception is raised in case the password confirmation fails three
            times in a row.
        """
        try:
            # get password
            _pass = password or _get_pass(principal)
            _r = subprocess.run(
                f"kinit {principal}",
                input=_pass.encode(),
                shell=True,
                capture_output=True,
                check=False,
            )
            if _r.returncode != 0:
                raise context.SubprocessError(_r.stderr.decode())

            return _r.returncode == 0

        except subprocess.SubprocessError:
            raise
        except UserWarning:
            raise

    def generate_keytab(
        principal: str,
        path: PathType,
        password: str | None = None,
        encryptions: list[str] | None = None,
    ) -> bool:
        """Generate a keytab file (Unix)

        Parameters
        ----------
        principal : str
            The principal to add to keytab-file
        path : pathlib.Path | str
           The file path where the keytab file will be stored
        password : Optional[str], optional
            The password to encrypt and add to keytab-file, by default `None`. If
            `None` the user is propmpted for the password.
        encryptions : Optional[List[str]], optional
            A list of encryptions to encrypt the password, by default `None`. If
            `None` this encryptions are used: `{"aes128-cts-hmac-sha1-96",
            "aes256-cts-hmac-sha1-96", "aes256-cts", "rc4-hmac"}`

        Returns
        -------
        bool
            True if keytab file got generated

        Raises
        ------
        SubprocessError
            An exception is raised in case keytab generation subprocess fails.
        UserWarning
            An exception is raised in case the password confirmation fails three
            times in a row.
        """

        logger.debug(f"Generating keytab file using principal: {principal} at {path}")
        try:
            _pass = password or _get_pass(principal)
            _encryptions = encryptions or [
                "aes128-cts-hmac-sha1-96",
                "aes256-cts-hmac-sha1-96",
                "aes256-cts",
                "rc4-hmac",
            ]
            # create keytab using ktutil
            _cmd = "ktutil << EOF >> /dev/null"
            for _enc in _encryptions:
                _cmd += f'\naddent -password -p "{principal}" -k 1 -e {_enc}\n{_pass}'

            _cmd += f"\nwkt {path!s}\nEOF"

            _r = subprocess.run(
                inspect.cleandoc(_cmd),
                shell=True,
                capture_output=True,
                check=False,
            )
            if _r.returncode != 0:
                raise context.SubprocessError(_r.stderr.decode())

            return _r.returncode == 0

        except subprocess.SubprocessError:
            raise
        except UserWarning:
            raise
else:
    raise RuntimeError("Only defined for nt and posix platforms")


def _kinit_with_keytab(principal: str, keytab: PathType) -> bool:
    """Kinit with keytab file"""

    # test for valid tgt existence
    logger.debug(
        f"No valid tgt. Running 'kinit' with keytab '{keytab}' and principal '{principal}'"
    )
    try:
        # get new tgt
        _r = subprocess.run(
            f"kinit -kt {keytab} {principal}",
            shell=True,
            capture_output=True,
            check=False,
        )
        if _r.returncode != 0:
            logger.info(_r.stderr.decode())

        return _r.returncode == 0

    except subprocess.SubprocessError:
        return False


def kinit(
    principal: str | None = None, password: str | None = None, keytab: PathType | None = None
) -> bool:
    """Kinit with principal or keytab file.

    Renew kerberos tgt if not valid anymore or does not exist.

    Parameters
    ----------
    principal : str
        The principal to get a tgt for.
    password : str, optional
        The password to use, by default `None`. If `None` prompt user to
        enter password.
    keytab : PathType
        Path to a keytab file.
    """
    if principal and "@" not in principal:
        raise context.AttributeError(f"Missing realm in principal '{principal}'")
    if not principal and not keytab:
        raise context.AttributeError("Either 'principal' or 'keytab' is required.")

    _principal = principal or principal_from_keytab(keytab)
    # test for valid tgt existence
    if (
        subprocess.run("klist -s", shell=True, check=False).returncode != 0
        or _principal
        not in subprocess.run(
            "klist -l", shell=True, stdout=subprocess.PIPE, check=False
        ).stdout.decode()
    ):
        # clean up kerberos cache
        subprocess.run("kdestroy -A", shell=True, check=False)
        if keytab:
            return _kinit_with_keytab(_principal, keytab)
        else:
            return _kinit_with_principal(_principal, password)
