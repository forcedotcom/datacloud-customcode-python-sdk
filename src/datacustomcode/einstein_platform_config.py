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

from typing import (
    ClassVar,
    Optional,
    Type,
    cast,
)

from datacustomcode.common_config import BaseObjectConfig


class CredentialsObjectConfig(BaseObjectConfig):
    type_to_create: ClassVar[Type]
    credentials_profile: Optional[str] = None
    sf_cli_org: Optional[str] = None

    def to_object(self):
        """Create an object instance, automatically including credentials in options"""

        options = self.options.copy()
        if self.credentials_profile is not None:
            options["credentials_profile"] = self.credentials_profile
        if self.sf_cli_org is not None:
            options["sf_cli_org"] = self.sf_cli_org

        type_ = self.type_to_create.subclass_from_config_name(self.type_config_name)
        return cast(type_, type_(**options))
