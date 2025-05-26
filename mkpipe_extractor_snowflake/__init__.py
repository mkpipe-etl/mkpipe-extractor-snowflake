from mkpipe.functions_spark import BaseExtractor


class SnowflakeExtractor(BaseExtractor):
    def __init__(self, config, settings):
        super().__init__(
            config,
            settings,
            driver_name='snowflake',
            driver_jdbc='net.snowflake.client.jdbc.SnowflakeDriver',
        )

    def build_jdbc_url(self):
        base = f'jdbc:{self.driver_name}://{self.host}:{self.port}/?user={self.username}&warehouse={self.warehouse}&db={self.database}&schema={self.schema}'
        if self.private_key_file:
            if self.private_key_file_pwd:
                return f'{base}&private_key_file={self.private_key_file}&private_key_file_pwd={self.private_key_file_pwd}'
            else:
                return f'{base}&private_key_file={self.private_key_file}'
        else:
            return f'{base}&password={self.password}'