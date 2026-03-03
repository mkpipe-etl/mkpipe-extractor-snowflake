from mkpipe.spark import JdbcExtractor

JAR_PACKAGES = ['net.snowflake:snowflake-jdbc:3.24.0']


class SnowflakeExtractor(JdbcExtractor, variant='snowflake'):
    driver_name = 'snowflake'
    driver_jdbc = 'net.snowflake.client.jdbc.SnowflakeDriver'

    def build_jdbc_url(self):
        base = (
            f'jdbc:{self.driver_name}://{self.host}:{self.port}/'
            f'?user={self.username}'
            f'&warehouse={self.warehouse}'
            f'&db={self.database}'
            f'&schema={self.schema}'
        )
        if self.private_key_file:
            base += f'&private_key_file={self.private_key_file}'
            if self.private_key_file_pwd:
                base += f'&private_key_file_pwd={self.private_key_file_pwd}'
        else:
            base += f'&password={self.password}'
        return base