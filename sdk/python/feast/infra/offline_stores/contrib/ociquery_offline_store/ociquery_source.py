from typing import Callable, Dict, Iterable, Optional, Tuple

from feast import type_map
from feast.data_source import DataSource
from feast.errors import DataSourceNoNameException, DataSourceNotFoundException
from feast.feature_logging import LoggingDestination
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.FeatureService_pb2 import (
    LoggingConfig as LoggingConfigProto,
)
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.value_type import ValueType


class OciQuerySource(DataSource):
    def __init__(
            self,
            *,
            timestamp_field: Optional[str] = "",
            table: Optional[str] = None,
            database: Optional[str] = None,
            data_source: Optional[str] = None,
            created_timestamp_column: Optional[str] = None,
            field_mapping: Optional[Dict[str, str]] = None,
            date_partition_column: Optional[str] = None,
            query: Optional[str] = None,
            name: Optional[str] = None,
            description: Optional[str] = "",
            tags: Optional[Dict[str, str]] = None,
            owner: Optional[str] = "",
    ):
        """
        Creates a OciQuerySource object.

        Args:
            timestamp_field : event timestamp column.
            table (optional): Athena table where the features are stored. Exactly one of 'table'
                and 'query' must be specified.
            database: Athena Database Name
            data_source (optional): Athena data source
            created_timestamp_column (optional): Timestamp column indicating when the
                row was created, used for deduplicating rows.
            field_mapping (optional): A dictionary mapping of column names in this data
                source to column names in a feature table or view.
            date_partition_column : Timestamp column used for partitioning.
            query (optional): The query to be executed to obtain the features. Exactly one of 'table'
                and 'query' must be specified.
            name (optional): Name for the source. Defaults to the table if not specified, in which
                case the table must be specified.
            description (optional): A human-readable description.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            owner (optional): The owner of the oci_query source, typically the email of the primary
                maintainer.
        """
        _database = "default" if table and not database else database
        self.oci_query_options = OciQueryOptions(
            table=table, query=query, database=_database, data_source=data_source
        )

        if table is None and query is None:
            raise ValueError('No "table" argument provided.')

        # If no name, use the table as the default name.
        if name is None and table is None:
            raise DataSourceNoNameException()
        _name = name or table
        assert _name

        super().__init__(
            name=_name if _name else "",
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            date_partition_column=date_partition_column,
            description=description,
            tags=tags,
            owner=owner,
        )

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        """
        Creates a OciQuerySource from a protobuf representation of a OciQuerySource.

        Args:
            data_source: A protobuf representation of a OciQuerySource

        Returns:
            A OciQuerySource object based on the data_source protobuf.
        """
        return OciQuerySource(
            name=data_source.name,
            timestamp_field=data_source.timestamp_field,
            table=data_source.oci_query_options.table,
            database=data_source.oci_query_options.database,
            data_source=data_source.oci_query_options.data_source,
            created_timestamp_column=data_source.created_timestamp_column,
            field_mapping=dict(data_source.field_mapping),
            date_partition_column=data_source.date_partition_column,
            query=data_source.oci_query_options.query,
            description=data_source.description,
            tags=dict(data_source.tags),
        )

    # Note: Python requires redefining hash in child classes that override __eq__
    def __hash__(self):
        return super().__hash__()

    def __eq__(self, other):
        if not isinstance(other, OciQuerySource):
            raise TypeError(
                "Comparisons should only involve OciQuerySource class objects."
            )

        return (
                super().__eq__(other)
                and self.oci_query_options.table == other.oci_query_options.table
                and self.oci_query_options.query == other.oci_query_options.query
                and self.oci_query_options.database == other.oci_query_options.database
                and self.oci_query_options.data_source == other.oci_query_options.data_source
        )

    @property
    def table(self):
        """Returns the table of this OciQuery source."""
        return self.oci_query_options.table

    @property
    def database(self):
        """Returns the database of this OciQuery source."""
        return self.oci_query_options.database

    @property
    def query(self):
        """Returns the OciQuery query of this OciQuery source."""
        return self.oci_query_options.query

    @property
    def data_source(self):
        """Returns the OciQuery data_source of this OciQuery source."""
        return self.oci_query_options.data_source

    def to_proto(self) -> DataSourceProto:
        """
        Converts a RedshiftSource object to its protobuf representation.

        Returns:
            A DataSourceProto object.
        """
        data_source_proto = DataSourceProto(
            type=DataSourceProto.BATCH_ATHENA,
            name=self.name,
            timestamp_field=self.timestamp_field,
            created_timestamp_column=self.created_timestamp_column,
            field_mapping=self.field_mapping,
            date_partition_column=self.date_partition_column,
            description=self.description,
            tags=self.tags,
            oci_query_options=self.oci_query_options.to_proto(),
        )

        return data_source_proto

    def validate(self, config: RepoConfig):
        # As long as the query gets successfully executed, or the table exists,
        # the data source is validated. We don't need the results though.
        self.get_table_column_names_and_types(config)

    def get_table_query_string(self, config: Optional[RepoConfig] = None) -> str:
        """Returns a string that can directly be used to reference this table in SQL."""
        if self.table:
            data_source = self.data_source
            database = self.database
            if config:
                data_source = config.offline_store.data_source
                database = config.offline_store.database
            return f'"{data_source}"."{database}"."{self.table}"'
        else:
            return f"({self.query})"

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return type_map.oci_query_to_feast_value_type

    def get_table_column_names_and_types(
            self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        """
        Returns a mapping of column names to types for this OciQuery source.

        Args:
            config: A RepoConfig describing the feature repo
        """
        from botocore.exceptions import ClientError

        from sdk.python.feast.infra.offline_stores.contrib.ociquery_offline_store.ociquery import (
            OciQueryOfflineStoreConfig,
        )
        from sdk.python.feast.infra.utils import oci_utils

        assert isinstance(config.offline_store, OciQueryOfflineStoreConfig)

        client = oci_utils.get_oci_query_data_client(config.offline_store.region)
        if self.table:
            try:
                table = client.get_table_metadata(
                    CatalogName=self.data_source,
                    DatabaseName=self.database,
                    TableName=self.table,
                )
            except ClientError as e:
                raise oci_utils.OciQueryError(e)

            # The API returns valid JSON with empty column list when the table doesn't exist
            if len(table["TableMetadata"]["Columns"]) == 0:
                raise DataSourceNotFoundException(self.table)

            columns = table["TableMetadata"]["Columns"]
        else:
            statement_id = oci_utils.execute_oci_query_query(
                client,
                config.offline_store.data_source,
                config.offline_store.database,
                config.offline_store.workgroup,
                f"SELECT * FROM ({self.query}) LIMIT 1",
            )
            columns = oci_utils.get_oci_query_query_result(client, statement_id)[
                "ResultSetMetadata"
            ]["ColumnInfo"]

        return [(column["Name"], column["Type"].upper()) for column in columns]


class OciQueryOptions:
    """
    Configuration options for a OciQuery data source.
    """

    def __init__(
            self,
            table: Optional[str],
            query: Optional[str],
            database: Optional[str],
            data_source: Optional[str],
    ):
        self.table = table or ""
        self.query = query or ""
        self.database = database or ""
        self.data_source = data_source or ""

    @classmethod
    def from_proto(cls, oci_query_options_proto: DataSourceProto.OciQueryOptions):
        """
        Creates a OciQueryOptions from a protobuf representation of a OciQuery option.

        Args:
            oci_query_options_proto: A protobuf representation of a DataSource

        Returns:
            A OciQueryOptions object based on the oci_query_options protobuf.
        """
        oci_query_options = cls(
            table=oci_query_options_proto.table,
            query=oci_query_options_proto.query,
            database=oci_query_options_proto.database,
            data_source=oci_query_options_proto.data_source,
        )

        return oci_query_options

    def to_proto(self) -> DataSourceProto.OciQueryOptions:
        """
        Converts an OciQueryOptionsProto object to its protobuf representation.

        Returns:
            A OciQueryOptionsProto protobuf.
        """
        oci_query_options_proto = DataSourceProto.OciQueryOptions(
            table=self.table,
            query=self.query,
            database=self.database,
            data_source=self.data_source,
        )

        return oci_query_options_proto


class SavedDatasetOciQueryStorage(SavedDatasetStorage):
    _proto_attr_name = "oci_query_storage"

    oci_query_options: OciQueryOptions

    def __init__(
            self,
            table_ref: str,
            query: str = None,
            database: str = None,
            data_source: str = None,
    ):
        self.oci_query_options = OciQueryOptions(
            table=table_ref, query=query, database=database, data_source=data_source
        )

    @staticmethod
    def from_proto(storage_proto: SavedDatasetStorageProto) -> SavedDatasetStorage:

        return SavedDatasetOciQueryStorage(
            table_ref=OciQueryOptions.from_proto(storage_proto.oci_query_storage).table
        )

    def to_proto(self) -> SavedDatasetStorageProto:
        return SavedDatasetStorageProto(oci_query_storage=self.oci_query_options.to_proto())

    def to_data_source(self) -> DataSource:
        return OciQuerySource(table=self.oci_query_options.table)


class OciQueryLoggingDestination(LoggingDestination):
    _proto_kind = "oci_query_destination"

    table_name: str

    def __init__(self, *, table_name: str):
        self.table_name = table_name

    @classmethod
    def from_proto(cls, config_proto: LoggingConfigProto) -> "LoggingDestination":
        return OciQueryLoggingDestination(
            table_name=config_proto.oci_query_destination.table_name,
        )

    def to_proto(self) -> LoggingConfigProto:
        return LoggingConfigProto(
            oci_query_destination=LoggingConfigProto.OciQueryDestination(
                table_name=self.table_name
            )
        )

    def to_data_source(self) -> DataSource:
        return OciQuerySource(table=self.table_name)
