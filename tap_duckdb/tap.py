"""DuckDB tap class."""

from typing import List

from singer_sdk import SQLTap, SQLStream
from singer_sdk import typing as th  # JSON schema typing helpers
from tap_duckdb.client import DuckDBStream


class TapDuckDB(SQLTap):
    """DuckDB tap class."""
    name = "tap-duckdb"
    default_stream_class = DuckDBStream

    config_jsonschema = th.PropertiesList(
        th.Property(
            "path",
            th.StringType,
            required=True,
            description="Path to .duckdb file"
        ),
        th.Property(
            "database",
            th.StringType,
            required=True,
            description="Database name"
        ),
    ).to_dict()


if __name__ == "__main__":
    TapDuckDB.cli()
