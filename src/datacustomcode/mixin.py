# Copyright (c) 2025, Salesforce, Inc.
# SPDX-License-Identifier: Apache-2
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

from typing import ClassVar, TypeVar


def _get_all_subclass_descendants(cls: type) -> list[type]:
    all_subclasses = [cls]

    for subclass in cls.__subclasses__():
        all_subclasses.append(subclass)
        all_subclasses.extend(_get_all_subclass_descendants(subclass))

    return all_subclasses


_V = TypeVar("_V", bound="UserExtendableNamedConfigMixin")


class UserExtendableNamedConfigMixin:
    """Allows users to access extended classes by name in config.

    Our client code is text config driven. This means that if a user extends readers
    and writers, they wouldn't be able to add their own classes if they want to use a
    config. This allows them to name their subclasses and let them be found at runtime
    by the config driven execution.
    """

    CONFIG_NAME: str

    _registered_config_names: ClassVar[dict[str, type]] = {}

    def __init_subclass__(cls, **kwargs):
        """Validate CONFIG_NAME uniqueness across all subclasses."""
        super().__init_subclass__(**kwargs)
        if "CONFIG_NAME" not in cls.__dict__:
            return
        if cls.CONFIG_NAME is None or cls.CONFIG_NAME == "":
            return

        if cls.CONFIG_NAME in UserExtendableNamedConfigMixin._registered_config_names:
            existing_class = UserExtendableNamedConfigMixin._registered_config_names[
                cls.CONFIG_NAME
            ]
            raise TypeError(
                f"Class {cls.__name__} has the same CONFIG_NAME ('{cls.CONFIG_NAME}') "
                f"as existing class {existing_class.__name__}. "
                f"Each concrete class must have a unique CONFIG_NAME."
            )
        UserExtendableNamedConfigMixin._registered_config_names[cls.CONFIG_NAME] = cls

    @classmethod
    def subclass_from_config_name(cls: type[_V], config_name: str) -> type[_V]:
        """Create an instance of subclass by calling its string name (``CONFIG_NAME``).

        This is and should stay dynamic because a user may interactively add subclasses
        through REPL systems like Jupyter.

        Args:
            config_name: should match a subclass's ``CONFIG_NAME``.
        """
        subclass_config_name_map = {}
        for type_ in _get_all_subclass_descendants(cls):
            if name := getattr(type_, "CONFIG_NAME", ""):
                subclass_config_name_map[name] = type_
        try:
            return subclass_config_name_map[config_name]
        except KeyError as exc:
            raise KeyError(
                "Passed config_name does not match any subclass CONFIG_NAME"
            ) from exc

    @classmethod
    def available_config_names(cls: type[_V]) -> list[str]:
        """Get all available config names from the subclasses."""
        config_names = [
            type_.CONFIG_NAME
            for type_ in _get_all_subclass_descendants(cls)
            if hasattr(type_, "CONFIG_NAME") and type_.CONFIG_NAME
        ]
        return config_names
