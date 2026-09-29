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

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TYPE_CHECKING, Optional

from datacustomcode.io import cdf
from datacustomcode.io.reader.query_api import QueryAPIDataCloudReader

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

SEED_LIMIT = 10


def _clean_for_json(value: Any) -> Any:
    if isinstance(value, float):
        if math.isnan(value):
            return None
        if value.is_integer():
            return int(value)
    return value


class StreamingSourceSeeder:
    def __init__(
        self,
        spark: SparkSession,
        credentials_profile: str = "default",
        dataspace: Optional[str] = None,
        sf_cli_org: Optional[str] = None,
    ) -> None:
        self.spark = spark
        self.reader = QueryAPIDataCloudReader(
            spark=spark,
            credentials_profile=credentials_profile,
            dataspace=dataspace,
            sf_cli_org=sf_cli_org,
        )

    def seed_source(
        self, name: str, layer: str, fixtures_root: str
    ) -> bool:
        """Seed streaming fixtures and source schema.

        Returns:
            ``True`` when schema + fixtures are written
            ``False`` when the source is empty.
            Query API errors (missing creds, missing
            source) propagate as ``RuntimeError``.
        """
        read_fn = self.reader.read_dlo if layer == "dlo" else self.reader.read_dmo

        try:
            head_df = read_fn(name).limit(1)
        except Exception as exc:
            raise RuntimeError(
                f"Failed to read {layer}='{name}': {exc}."
            ) from exc

        head_pandas = head_df.toPandas()
        if len(head_pandas) == 0:
            return False

        schema = head_df.schema
        out_dir = Path(fixtures_root) / name
        out_dir.mkdir(parents=True, exist_ok=True)
        schema_file_path = out_dir / "_schema.json"
        schema_file_path.write_text(json.dumps(schema.jsonValue()))
        print(
            f"\nStreaming source schema written to {schema_file_path} "
            f"({len(schema.fields)} fields)"
        )

        # Mix UPSERT and DELETE in the seed so the customer sees both
        # operation types in the starter fixture
        snapshot = read_fn(name).limit(SEED_LIMIT).toPandas()
        seed_rows = []
        for i, record in enumerate(snapshot.to_dict("records")):
            op = (
                cdf.MergeRecordType.DELETE
                if i % 2
                else cdf.MergeRecordType.UPSERT
            )

            cleaned = {k: _clean_for_json(v) for k, v in record.items()}
            seed_rows.append({
                **cleaned,
                cdf.COMMIT_VERSION: i + 1,
                cdf.COMMIT_TIMESTAMP: datetime.now(timezone.utc).isoformat(),
                cdf.MERGE_RECORD_TYPE: op.value,
            })
        (out_dir / "000_seed.json").write_text(
            "\n".join(json.dumps(r, default=str) for r in seed_rows)
        )
        return True
