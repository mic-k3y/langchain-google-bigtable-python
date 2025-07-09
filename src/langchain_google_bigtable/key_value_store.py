# Copyright 2025 Google LLC
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

from typing import Any, Callable, Dict, Iterator, List, Optional

from google.cloud import bigtable  # type: ignore

from langchain.storage import BaseStore
from engine import BigtableEngine

from .common import use_client_or_default

DEFAULT_TABLE_COLUMN_FAMILIES = ["kv"]


def init_byte_store_table(
        instance_id: str,
        table_id: str,
        project_id: Optional[str] = None,
        client: Optional[bigtable.Client] = None,
        column_families: Optional[List[str]] = DEFAULT_TABLE_COLUMN_FAMILIES
) -> None:
    """
    Create a table for saving of LangChain Key-value pairs.
    If table already exists, a google.api_core.exceptions.AlreadyExists error is thrown.
    """

    table_client = (
        use_client_or_default(client, "key_value_store", project_id)
        .instance(instance_id)
        .table(table_id)
    )

    families: Dict[str, bigtable.column_family.MaxVersionsGCRule] = dict()
    for cf in column_families:
        families[cf] = bigtable.column_family.MaxVersionsGCRule(1)
    table_client.create(column_families=families)
