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
from bigtable.data import BigtableDataClientAsync

from langchain.storage import BaseStore

from engine import BigtableEngine
from async_key_value_store import AsyncBigtableByteStore
from .common import use_client_or_default, get_async_data_client

DEFAULT_TABLE_COLUMN_FAMILIES = ["kv"]
DEFAULT_VALUE_COLUMN_FAMILY = "kv"
DEFAULT_VALUE_COLUMN_QUALIFIER = "val"

class BigtableByteStore(BaseStore[str, bytes]):
    """
    Public-facing Bigtable ByteStore for LangChain. Provides a unified API
    for both synchronous and asynchronous operations.
    """

    __create_key = object()

    def __init__(self, key: object, engine: BigtableEngine, async_store: AsyncBigtableByteStore):
        """
        Private constructor. Use the .create() or .create_sync()
        classmethods to instantiate this class.
        """
        if key != BigtableByteStore.__create_key:
            raise Exception("Do not call the constructor directly. Use a .create() or .create_sync() factory method.")
        self._engine = engine
        self.__async_store = async_store

    @classmethod
    def create_sync(
            cls,
            instance_id: str,
            table_id: str,
            *,
            engine: Optional[BigtableEngine] = None,
            async_data_client: Optional[BigtableDataClientAsync] = None,
            project_id: Optional[str] = None,
            value_column_family: Optional[str] = DEFAULT_VALUE_COLUMN_FAMILY,
            value_column_qualifier: Optional[str] = DEFAULT_VALUE_COLUMN_QUALIFIER,
            app_profile_id: Optional[str] = None
    ):
        """
        Creates a sync-initialized instance of the BigtableByteStore.

        This is the standard entry point for synchronous applications. It will create
        a new BigtableEngine if one is not provided.

        Args:
            instance_id: The Bigtable instance ID.
            table_id: The Bigtable table ID.
            engine: An optional, existing engine to share resources.
            async_data_client: An optional, pre-configured async client.
            project_id: An optional project ID to use if setting up a new data client.
            value_column_family: The column family for storing values. Defaults to "kv"
            value_column_qualifier: The column family for storing values. Defaults to "val".
            app_profile_id: An Optional Bigtable app profile ID for routing requests.

        Returns:
            A new, fully initialized BigtableByteStore instance.

        Note:
            'engine' argument overrides 'async_data_client' argument if both arguments are provided.
        """

        if engine and async_data_client:
            raise ValueError("Cannot provide both 'engine' and 'async_data_client'.")
        if not engine:
            if not async_data_client:
                async_data_client = get_async_data_client(project_id)
            engine = BigtableEngine.sync_initialize(client=async_data_client)

        coro = AsyncBigtableByteStore.create(
            engine=engine, instance_id=instance_id, table_id=table_id,
            value_column_family=value_column_family, value_column_qualifier=value_column_qualifier,
            app_profile_id=app_profile_id
        )

        async_store = engine.__run_as_sync(coro)
        return cls(cls.__create_key, engine, async_store)

    @classmethod
    async def create(
            cls,
            instance_id: str,
            table_id: str,
            *,
            engine: Optional[BigtableEngine] = None,
            async_data_client: Optional[BigtableDataClientAsync] = None,
            project_id: Optional[str] = None,
            value_column_family: Optional[str] = DEFAULT_VALUE_COLUMN_FAMILY,
            value_column_qualifier: Optional[str] = DEFAULT_VALUE_COLUMN_QUALIFIER,
            app_profile_id: Optional[str] = None
    ):
        """
        Creates an async-initialized instance of the BigtableByteStore.

        This is the standard entry point for asynchronous applications.

        Args:
            instance_id: The Bigtable instance ID.
            table_id: The Bigtable table ID.
            engine: An optional, pre-configured async client.
            async_data_client: An optional, pre-configured async client.
            project_id: An optional project_id to use if setting up a new data client.
            value_column_family: The column family for storing values. Defaults to "kv".
            value_column_qualifier: The column qualifier for storing values. Defaults to "val".
            app_profile_id: An optional Bigtable app profile ID for routing requests.

        Returns:
            A new, fully initialized BigtableByteStore Instance.

        Note:
            'engine' argument overrides 'async_data_client' argument if both arguments are provided.
        """
        if not engine:
            if not async_data_client:
                async_data_client = get_async_data_client(project_id)
            engine = BigtableEngine.sync_initialize(client=async_data_client)

        async_store = await AsyncBigtableByteStore.create(
            engine=engine, instance_id=instance_id, table_id=table_id,
            value_column_family=value_column_family, value_column_qualifier=value_column_qualifier,
            app_profile_id=app_profile_id
        )
        return cls(cls.__create_key, engine, async_store)

    def mget(self, keys: List[str]) -> List[Optional[bytes]]:
        """Synchronously get values for a sequence of keys."""
        return self._engine._run_as_sync(self.__async_store.amget(keys))

    async def amget(self, keys: List[str]) -> List[Optional[bytes]]:
        """Asynchronously get values for a sequence of keys."""
        return await self._engine._run_as_async(self.__async_store.amget(keys))

    def mset(self, kv_pairs: List[Tuple[str, bytes]]) -> None:
        """Synchronously set values for a sequence of key-value pairs."""
        return self._engine._run_as_sync(self.__async_store.amset(kv_pairs))



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
