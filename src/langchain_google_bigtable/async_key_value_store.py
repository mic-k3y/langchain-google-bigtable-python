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

