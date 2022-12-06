"""SQL client handling.

This includes DuckDBStream and DuckDBConnector.
"""

import sqlalchemy

from singer_sdk import SQLConnector, SQLStream
from typing import Optional, Iterable, Dict, Any


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
