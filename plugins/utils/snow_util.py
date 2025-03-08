import snowflake.connector


def fetch_data(query, creds):
    """
    Fetch data from snowflake

    Args:
        query (str): Select query that want to be executed.
        creds (dict):  Dictionary containing Snowflake credentials and details.
            user (str): Snowflake username.
            password (str): Snowflake password.
            account (str): Snowflake account.
            database (str): Snowflake database name.
            schema (str): Snowflake schema name.
            warehouse (str): Snowflake warehouse name.
    """
    try:
        conn = snowflake.connector.connect(**creds)
        cur = conn.cursor()
        cur.execute(query)
        result = cur.fetch_pandas_all()
        cur.close()
        conn.close()
        return result
    except Exception as e:
        print(f"Error fetching data: {e}")