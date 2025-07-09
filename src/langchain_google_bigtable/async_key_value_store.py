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

import asyncio
from threading import Thread
from typing import Any, AsyncIterator, Dict, Iterator, List, Optional, Sequence, Tuple

from engine import BigtableEngine

from google.cloud.bigtable.data import BigtableDataClientAsync
from google.cloud.bigtable.data.mutations import DirectRow
from google.cloud.bigtable.data.models import Row, Cell


class AsyncBigtableByteStore:
    """The core, async-only implementation of the Bigtable ByteStore. Internal use only."""

    def __init__(
            self,
            engine: BigtableEngine,
            instance_id: str,
            table_id: str,
            value_column_family: str,
            value_column_qualifier: str,
            app_profile_id: Optional[str] = None,
    ):
        """
        Initializes the async-native store.

        Args:
            engine: The BigtableEngine that will run the operations.
            instance_id: The Bigtable instance ID.
            table_id: The Bigtable table ID.
            value_column_family: The column family to store values in.
            value_column_qualifier: The column qualifier to store values in.
            app_profile_id: Optional Bigtable app profile ID for routing requests.
        """
        self.engine = engine
        self.instance_id = instance_id
        self.table_id = table_id
        self.value_column_family = value_column_family
        self.value_column_qualifier = value_column_qualifier
        self.table = engine.client.get_table(
            instance_id=instance_id,
            table_id=table_id,
            app_profile_id=app_profile_id
        )

    @classmethod
    async def create(cls, **kwargs) -> "AsyncBigtableByteStore":
        """Async factory for the store. Allows for future async setup logic."""
        return cls(**kwargs)

    async def amget(self, keys: List[str]) -> List[Optional[bytes]]:
        """
        Asynchronously gets multiple values from Bigtable by their keys.

        Note: Returns None for keys that don't exist and for keys that exist but no cell in the value column
        """
        if not keys:
            return []
        row_keys = [key.encode('utf-8') for key in keys]

        rows = await self.table.read_rows(rows=row_keys)

        row_maps = {row.row_key: row for row in rows}

        results = []
        for key in row_keys:
            row = row_maps.get(key)
            if row:
                cell = row.cells.get(self.value_column_family, {}).get(self.value_column_qualifier.encode('utf-8'), [])
                if cell:
                    results.append(cell[0].value)
                else:
                    results.append(None) # Key exists but column doesn't
            else:
                results.append(None) # Key does not exist
        return results

    async def amset(self, kv_pairs: List[Tuple[str, bytes]]) -> None:
        """Asynchronously sets multiple key-value pairs in Bigtable."""
        mutations = []

        for key, val in kv_pairs:
            mutation = DirectRow(row_key=key.encode('utf-8'))
            mutation.set_cell(self.value_column_family, self.value_column_qualifier.encode('utf-8'), val)
            mutations.append(mutation)

        if mutations:
            await self.table.mutate_rows(mutations)

    async def amdelete(self, keys: List[str]) -> None:
        """Asynchronously deletes multiple rows by their keys."""
        mutations = []
        for key in keys:
            mutation = DirectRow(row_key=key.encode('utf-8'))
            mutation.delete()
            mutations.append(mutation)

        if mutations:
            await self.table.mutate_rows(mutations)

    async def ayield_keys(self, prefix: Optional[str] = None) -> AsyncIterator[str]:
        """
        Asynchronously yields keys from the table, optionally filtering by prefix.

        Args:
            prefix: An optional prefix to match the row keys.

        Returns:
            An AsyncIterator of row key strings.
        """
        if not prefix or prefix == "":
            # return all keys
            row_filter = StripValueFilter()
            async for row in self.table.read_rows(filter_=row_filter):
                yield row.row_key.decode('utf-8')

        else:
            # return keys matching the prefix
            end_key = prefix[:-1] + chr(ord(prefix[-1]) + 1)

            row_set = RowSet()
            row_set.add_row_range_from_keys(prefix.encode("utf-8"), end_key.encode("utf-8"))

            async for row in self.table.read_rows(row_set=row_set):
                yield row.row_key.decode('utf-8')
