#!/usr/bin/env python3
"""
yarn/models
===========

This module contains the implementation of the yarn models used for handling
payload and response messages for the Hadoop `YARN` REST API.

The module provides a base class `ResourceManagerMessage` that serves as the
foundation for handling different types of messages. It includes methods for
decoding messages, converting them to a dictionary, converting them to
protobuf, and converting them to a `Skein` model.

Derived classes are expected to implement the specifics of different types
of messages. They should define the specific fields and structures of the
messages they handle, and define the `_skein_cls` attribute to indicate the
`Skein` model class to convert to. If the `_skein_cls` attribute is not defined,
the message does not have a corresponding `Skein` model - the `to_skein` and
`to_protobuf` methods will return `None` in this case.
"""

from __future__ import annotations

__author__ = "jslorrma"
__maintainer__ = "jslorrma"
__email__ = "jslorrma@gmail.com"

import inspect
from typing import TYPE_CHECKING, ClassVar

import msgspec

if TYPE_CHECKING:
    from collections.abc import Callable


class ResourceManagerMessage(msgspec.Struct, omit_defaults=True, kw_only=True):
    """
    Base class for handling payload and response messages for the Hadoop `YARN`
    REST API.

    This class provides base methods to decode messages from the Hadoop `YARN`
    REST API, convert to a dictionary, convert to protobuf, and convert to a
    `Skein` model.

    Derived classes are expected to implement the specifics of different types
    of messages. They should define the specific fields and structures of the
    messages they handle, and define the `_skein_cls` attribute to indicate the
    `Skein` model class to convert to.

    Methods
    -------
    decode(message: bytes | str) -> Message
        Decodes a message.
    to_skein_model()
        Converts the message to a skein model.
    to_protobuf()
        Converts the message to protobuf.
    to_dict()
        Converts the message to a dictionary.
    """

    _skein_cls: ClassVar[None] = None

    def __post_init__(self):
        """
        Post-initialization method for the Message class.

        """
        # compute fields
        for name, value in inspect.getmembers(self):
            if inspect.ismethod(value) and hasattr(value, "_compute_field"):
                _to = getattr(self, name)()
                if _to is not None:
                    setattr(self, value._compute_field, getattr(self, name)())

        # reset internal fields
        for field, default in zip(self.__struct_fields__, self.__struct_defaults__, strict=False):
            if field.startswith("_"):
                setattr(
                    self,
                    field,
                    default if default is not msgspec._core.Factory else default.factory(),
                )

    @classmethod
    def compute_field(cls, func: Callable[..., None]):
        """
        Decorator to register a function to compute fields.

        The decorated function name must be the field name to compute prefixed
        or postfixed with "_" or prefixed with "_compute_". The decorator will
        strip these prefixes and postfixes from the function name to determine
        name of the field the function converts.

        Example
        -------

        This example demonstrates the usage of the `ResourceManagerMessage` class
        and its `compute_field` decorator in the context of an
        `ApplicationReport` class.

        The `ApplicationReport` class has fields (`id`, `user`, `name`,
        `host`, `port`) and an internal field `_address` used for message
        decoding.

        Two compute methods, `host_` and `_compute_port`, are defined using the
        `ResourceManagerMessage.compute_field` decorator, to split the `_address`
        field into `host` and `port` fields.

        ```python
        from typing import ClassVar

        import msgspec
        from skein import model as skein_model

        from reskein.yarn.models.messages import ResourceManagerMessage

        class ApplicationReport(Message, kw_only=True):
            _skein_cls: ClassVar[type[skein_model.ApplicationReport]] = skein_model.ApplicationReport

            # message fields
            id: str
            user: str
            name: str
            host: str = ""
            port: str = ""

            # internal fields for message decoding, will be used by converters
            _address: str = msgspec.field(name="amRPCAddress")

            @Message.compute_field
            def host_(self):
                # splits the _address field into `host` field
                return self._address.split(":")[0]

            @Message.compute_field
            def _convert_port(self):
                # splits the _address field into `port` field and cast to int
                return int(self._address.split(":")[1])
        ```

        Using an example JSON message `app_report` to be decoded into an
        `ApplicationReport` instance using msgspec.json.decode. The `host` and
        `port` fields are automatically populated from `amRPCAddress` by the
        converter methods.

        ```python
        # sample JSON message to be decoded
        app_report = b'''
            {
                "id": "application_1234567890123_001",
                "user": "testuser",
                "name": "hello_world",
                "amRPCAddress": "localhost:41234"
            }
        '''

        msgspec.json.decode(app_report, type=ApplicationReport)
        ```
        """
        func._compute_field = func.__name__.strip("_").removeprefix("compute_")
        return func

    @classmethod
    def decode(cls, message: bytes | str) -> ResourceManagerMessage:
        """
        Decode a message.

        Parameters
        ----------
        message : bytes | str
            The message to decode.

        Returns
        -------
        Message
            The decoded message.
        """
        return msgspec.json.decode(message, type=cls, strict=False)

    def to_skein(self):
        """
        Convert the message to a skein model.

        Returns
        -------
        The skein model, or `None` if `_skein_cls` is not defined.
        """
        if self._skein_cls:
            return self._skein_cls.from_protobuf(self.to_protobuf())

    def to_protobuf(self):
        """
        Convert the message to protobuf.

        Returns
        -------
        The protobuf message, or `None` if `_skein_cls` is not defined.
        """
        if self._skein_cls:
            return self._skein_cls._protobuf_cls(**self.to_dict())

    def to_dict(self):
        """
        Convert the message to a dictionary.

        Returns
        -------
        dict
            The message as a dictionary.
        """

        def _to_dict(obj):
            """Converts an object to a dictionary."""
            if isinstance(obj, dict):
                return {
                    _field: _to_dict(_value)
                    for _field, _value in obj.items()
                    if _is_valid_field(_value)
                }
            elif isinstance(obj, list | tuple | set):
                return type(obj)(_to_dict(_item) for _item in obj if _is_valid_field(_item))
            elif _is_valid_field(obj):
                return obj

        def _is_valid_field(value):
            """Checks if a field should be included in the dictionary."""
            if (
                isinstance(value, msgspec.UnsetType)
                or value is None
                or (isinstance(value, list | tuple | set | dict) and not value)
            ):
                return False
            return True

        return _to_dict(msgspec.to_builtins(self))
