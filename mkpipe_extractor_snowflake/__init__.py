from typing import Optional

from mkpipe.spark.base import BaseExtractor
from mkpipe.models import ConnectionConfig, ExtractResult, TableConfig
from mkpipe.utils import get_logger

JAR_PACKAGES = [
    'net.snowflake:spark-snowflake_2.13:3.1.0',
    'net.snowflake:snowflake-jdbc:3.24.0',
]

logger = get_logger(__name__)


class SnowflakeExtractor(BaseExtractor, variant='snowflake'):
    def __init__(self, connection: ConnectionConfig):
        self.connection = connection
        self.host = connection.host
        self.port = connection.port or 443
        self.username = connection.user
        self.password = str(connection.password or '')
        self.database = connection.database
        self.schema = connection.schema or 'PUBLIC'
        self.warehouse = connection.warehouse
        self.private_key_file = connection.private_key_file
        self.private_key_file_pwd = connection.private_key_file_pwd

    @staticmethod
    def _read_pem_key(key_path: str, passphrase: Optional[str] = None) -> str:
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.backends import default_backend

        with open(key_path, 'rb') as f:
            key_data = f.read()

        pwd_bytes = passphrase.encode() if passphrase else None
        private_key = serialization.load_pem_private_key(
            key_data, password=pwd_bytes, backend=default_backend()
        )
        key_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        # Strip PEM header/footer and newlines to get raw base64 content
        key_str = (
            key_bytes.decode('utf-8')
            .replace('-----BEGIN PRIVATE KEY-----', '')
            .replace('-----END PRIVATE KEY-----', '')
            .strip()
        )
        return key_str

    def _base_options(self) -> dict:
        import os

        opts = {
            'sfURL': f'{self.host}:{self.port}',
            'sfUser': self.username,
            'sfDatabase': self.database,
            'sfSchema': self.schema,
            'sfWarehouse': self.warehouse,
        }
        if self.private_key_file:
            key_path = os.path.expanduser(self.private_key_file)
            opts['pem_private_key'] = self._read_pem_key(
                key_path, self.private_key_file_pwd
            )
        else:
            opts['sfPassword'] = self.password
        return opts

    def _build_reader(self, spark, dbtable: str, extra_opts: Optional[dict] = None):
        opts = {**self._base_options(), 'dbtable': dbtable}
        if extra_opts:
            opts.update(extra_opts)
        reader = spark.read.format('net.snowflake.spark.snowflake')
        for k, v in opts.items():
            reader = reader.option(k, v)
        return reader.load()

    def _resolve_custom_query(self, table: TableConfig) -> Optional[str]:
        import os

        if table.custom_query:
            return table.custom_query
        if table.custom_query_file:
            path = os.path.abspath(
                os.path.join(os.getcwd(), 'sql', table.custom_query_file)
            )
            with open(path) as f:
                return f.read()
        return None

    def extract(
        self, table: TableConfig, spark, last_point: Optional[str] = None
    ) -> ExtractResult:
        logger.info(
            {
                'table': table.target_name,
                'status': 'extracting',
                'replication_method': table.replication_method.value,
            }
        )

        custom_query = self._resolve_custom_query(table)

        if table.replication_method.value == 'incremental' and table.iterate_column:
            if last_point:
                write_mode = 'append'
                if table.iterate_column_type == 'int':
                    filter_clause = f'WHERE {table.iterate_column} >= {last_point}'
                else:
                    filter_clause = f"WHERE {table.iterate_column} >= '{last_point}'"
            else:
                write_mode = 'overwrite'
                filter_clause = 'WHERE 1=1'

            if custom_query:
                sql = custom_query.replace('{query_filter}', filter_clause)
            else:
                sql = f'SELECT * FROM {table.name} {filter_clause}'

            dbtable = f'({sql}) q'
            df = self._build_reader(spark, dbtable)

            if not df.take(1):
                logger.info({'table': table.target_name, 'status': 'no_new_data'})
                return ExtractResult(df=None, write_mode=write_mode)

            from pyspark.sql import functions as F

            row = df.agg(F.max(table.iterate_column).alias('max_val')).first()
            last_point_value = (
                str(row['max_val']) if row and row['max_val'] is not None else None
            )
        else:
            write_mode = 'overwrite'
            if custom_query:
                sql = custom_query.replace('{query_filter}', 'WHERE 1=1')
                dbtable = f'({sql}) q'
            else:
                dbtable = table.name
            df = self._build_reader(spark, dbtable)
            last_point_value = None

        logger.info(
            {
                'table': table.target_name,
                'status': 'extracted',
                'write_mode': write_mode,
            }
        )
        return ExtractResult(
            df=df, write_mode=write_mode, last_point_value=last_point_value
        )
