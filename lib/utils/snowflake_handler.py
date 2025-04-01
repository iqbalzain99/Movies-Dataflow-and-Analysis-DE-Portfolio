import snowflake.connector
import pandas as pd

class SnowflakeHandler:
    def __init__(self, snow_creds: dict, db_name, db_schema):
        """
        Initialize the SnowflakeHandler with Snowflake credentials.
        Credentials can be provided as arguments or read from environment variables.
        
        Args:
            creds (dict):  Dictionary containing Snowflake credentials and details.
                user (str): Snowflake username.
                password (str): Snowflake password.
                account (str): Snowflake account.
                database (str): Snowflake database name.
                schema (str): Snowflake schema name.
                warehouse (str): Snowflake warehouse name.
        """
        self.snow_creds = snow_creds
        self.db_name = db_name
        self.db_schema = db_schema
        # Connect into snowflake
        self._connect()
        # Set active schema
        self._set_active_schema()
    
    def _connect(self):
        """Establish connection with snowflake"""
        self.conn = snowflake.connector.connect(**self.snow_creds)
        
    def close(self):
        """Close connection with snowflake"""
        self.conn.close()
        
    def execute_query(self, query):
        """
        Fetch data from snowflake
        Args:
            query (str): Select query that want to be executed.
        Returns:
            Dataframe
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(query)

                # Return dataframe as pd.DataFrame for SELECT query
                if query.strip().lower().startswith("select"):
                    return cur.fetch_pandas_all()
                else:
                    self.conn.commit()
                    return None  # No DataFrame needed for INSERT/UPDATE/DELETE
        except Exception as e:
            print(f"Error fetching data: {e}")
    
    def load(self, table_name: str, stage_dir: str, file_name: str) -> None:
        """
        Load data from S3 Parquet file into the target table.
        
        Args:
            table_name: Name of the target table
            stage_dir: S3 stage directory path
            file_name: Name of the Parquet file to load
            
        Raises:
            DatabaseError: If any database operation fails
        """
        try:            
            # Truncate target table
            self._truncate_table(table_name)
            
            # Load data from Parquet
            self._execute_copy_command(
                table_name,
                stage_dir,
                file_name,
            )
            
            print(f'Successfully loaded data into {table_name}')
            
        except Exception as e:
            print(f'Failed to load data into {table_name}: {str(e)}')
            raise  # Re-raise for error handling upstream

    def _set_active_schema(self) -> str:
        """Retrieve and format column mappings for COPY command."""
        
        schema_query = f"""USE SCHEMA {self.db_name}.{self.db_schema}"""
        
        self.execute_query(schema_query)

    def _truncate_table(self, table_name: str) -> None:
        """Safely truncate target table."""
        truncate_query = f"""TRUNCATE TABLE {self.db_name}.{self.db_schema}.{table_name}"""
        print(f'Executing truncate query: {truncate_query}')
        self.execute_query(truncate_query)

    def _execute_copy_command(
        self,
        table_name: str,
        stage_dir: str,
        file_name: str,
    ) -> None:
        """Execute the COPY INTO command with proper formatting."""
        copy_query = f"""
            COPY INTO {self.db_name}.{self.db_schema}.{table_name}
            FROM {stage_dir}{file_name}
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
        """
        print(f'Executing copy query: {copy_query}')
        self.execute_query(copy_query)


    def create_table(self, table_name: str, df: pd.DataFrame):
        """
        Generate DDL statement for Snowflake table from dataframe. and execute
        the DDL statement for creating table with drop and create method
        """
        def _sanitize_column_name(col_name: str) -> str:
            """Escape reserved keywords for Snowflake column names."""
            return f'"{col_name}"' if col_name.lower() == "order" else col_name
        
        def _map_dtype_to_snowflake(dtype) -> str:
            """Map pandas/numpy dtypes to Snowflake column types."""
            if pd.api.types.is_datetime64_any_dtype(dtype):
                return "TIMESTAMP_NTZ(9)"
            elif pd.api.types.is_float_dtype(dtype):
                return "FLOAT"
            elif pd.api.types.is_string_dtype(dtype) or dtype == object:
                return "STRING"
            elif pd.api.types.is_bool_dtype(dtype):
                return "BOOLEAN"
            elif  pd.api.types.is_integer_dtype(dtype):
                return "INT"
            else:
                return "VARCHAR(16777216)"
        
        columns_def = [
            f"{_sanitize_column_name(col_name)} {_map_dtype_to_snowflake(col_type)}"
            for col_name, col_type in df.dtypes.items()
        ]

        columns_str = ",\n    ".join(columns_def)
        
        ddl_create = f"""CREATE TABLE IF NOT EXISTS {self.db_name}.{self.db_schema}.{table_name}(\n    {columns_str}\n)"""
        # Creating table if not exist
        self.execute_query(ddl_create)
        
        print(f"Table {self.db_name}.{self.db_schema}.{table_name} created sucessfully")