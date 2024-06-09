"""SQL client handling.

This includes DuckDBStream and DuckDBConnector.
"""

import sqlalchemy
import typing as t
from singer_sdk import SQLConnector, SQLStream
from typing import Optional, Iterable, Dict, Any
from singer_sdk._singerlib import CatalogEntry
from sqlalchemy.engine import Engine
from sqlalchemy.engine.reflection import Inspector

class DuckDBConnector(SQLConnector):
    """Connects to the DuckDB SQL source."""

    def get_sqlalchemy_url(cls, config: dict) -> str:
        """Concatenate a SQLAlchemy URL for use in connecting to the source."""
        return (
            f"duckdb:///{config['path']}"
        )

    @staticmethod
    def to_jsonschema_type(sql_type: sqlalchemy.types.TypeEngine) -> dict:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.
        """
        # Optionally, add custom logic before calling the parent SQLConnector method.
        # You may delete this method if overrides are not needed.
        return SQLConnector.to_jsonschema_type(sql_type)

    @staticmethod
    def to_sql_type(jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.
        """
        # Optionally, add custom logic before calling the parent SQLConnector method.
        # You may delete this method if overrides are not needed.
        return SQLConnector.to_sql_type(jsonschema_type)
    
    def create_sqlalchemy_connection(self) -> sqlalchemy.engine.Connection:
        """Return a new SQLAlchemy connection using the provided config.

        By default this will create using the sqlalchemy `stream_results=True` option
        described here:

        https://docs.sqlalchemy.org/en/14/core/connections.html#using-server-side-cursors-a-k-a-stream-results

        Developers may override this method if their provider does not support
        server side cursors (`stream_results`) or in order to use different
        configurations options when creating the connection object.

        Returns:
            A newly created SQLAlchemy engine object.
        """
        return (
            self.create_sqlalchemy_engine()
            .connect()
        )

    def discover_catalog_entry(
        self,
        engine: Engine,  # noqa: ARG002
        inspected: Inspector,
        schema_name: str,
        table_name: str,
        is_view: bool,  # noqa: FBT001
    ) -> CatalogEntry:
        catalog_entry = super().discover_catalog_entry(
            engine,
            inspected,
            schema_name,
            table_name,
            is_view
        )
        catalog_entry.database = self.config["database"]
        return catalog_entry

    def discover_catalog_entries(self) -> list[dict]:
        """Return a list of catalog entries from discovery.

        Returns:
            The discovered catalog entries as a list.
        """
        result: list[dict] = []
        engine = self._engine
        inspected = sqlalchemy.inspect(engine)
        for schema_name in self.get_schema_names(engine, inspected):
            # Iterate through each table and view

            # Override logic to remove database if in schema name
            parts = schema_name.split(".")
            if len(parts) > 1:
                schema_name = parts[1]

            for table_name, is_view in self.get_object_names(
                engine,
                inspected,
                schema_name,
            ):
                catalog_entry = self.discover_catalog_entry(
                    engine,
                    inspected,
                    schema_name,
                    table_name,
                    is_view,
                )
                result.append(catalog_entry.to_dict())

        return result

    def parse_full_table_name(  # noqa: PLR6301
        self,
        full_table_name: str,
    ) -> tuple[str | None, str | None, str]:
        """Parse a fully qualified table name into its parts.

        Developers may override this method if their platform does not support the
        traditional 3-part convention: `db_name.schema_name.table_name`

        Args:
            full_table_name: A table name or a fully qualified table name. Depending on
                SQL the platform, this could take the following forms:
                - `<db>.<schema>.<table>` (three part names)
                - `<db>.<table>` (platforms which do not use schema groupings)
                - `<schema>.<name>` (if DB name is already in context)
                - `<table>` (if DB name and schema name are already in context)

        Returns:
            A three part tuple (db_name, schema_name, table_name) with any unspecified
            or unused parts returned as None.
        """
        # raise Exception(full_table_name)
        db_name: str | None = None
        schema_name: str | None = None

        parts = full_table_name.split(".")
        if len(parts) == 1:
            table_name = full_table_name
        if len(parts) == 2:  # noqa: PLR2004
            schema_name, table_name = parts
        if len(parts) == 3:  # noqa: PLR2004
            db_name, schema_name, table_name = parts

        # I need to append database to schema name since the connection isnt at the database level like expected
        return db_name, f"{self.config['database']}.{schema_name}", table_name


class DuckDBStream(SQLStream):
    """Stream class for DuckDB streams."""

    connector_class = DuckDBConnector

    def get_records(self, partition: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            partition: If provided, will read specifically from this data slice.

        Yields:
            One dict per record.
        """
        # Optionally, add custom logic instead of calling the super().
        # This is helpful if the source database provides batch-optimized record
        # retrieval.
        # If no overrides or optimizations are needed, you may delete this method.
        yield from super().get_records(partition)
