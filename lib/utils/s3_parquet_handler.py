import io
import boto3
import pandas as pd


class S3ParquetHandler:
    def __init__(self, aws_creds: dict):
        """
        Initialize the S3ParquetHandler with AWS credentials.
        Credentials can be provided as arguments or read from environment variables.
        """
        self.aws_access_key_id = aws_creds['access_key']
        self.aws_secret_access_key = aws_creds['secret_key']
        self.region_name = aws_creds['region']
        
        if not self.aws_access_key_id or not self.aws_secret_access_key:
            raise ValueError("AWS credentials not found. Please set the environment variables "
                             "'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY', or pass them as arguments.")
        
        self.session = boto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name
        )
        self.s3_client = self.session.client("s3")

    def read_parquet_from_s3(self, bucket: str, key: str) -> pd.DataFrame:
        """
        Read a Parquet file from S3 and load it into a pandas DataFrame.
        
        Args:
            bucket (str): Name of the S3 bucket.
            key (str): Object key (path) for the Parquet file in the bucket.
        Returns:
            pandas DataFrame with the loaded data.
        """
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            data = response["Body"].read()
            buffer = io.BytesIO(data)
            df = pd.read_parquet(buffer)
            return df
        except Exception as e:
            raise RuntimeError(f"Failed to read Parquet file from s3://{bucket}/{key}: {e}")

    def write_parquet_to_s3(self, df: pd.DataFrame, bucket: str, key: str, index: bool = False):
        """
        Write a pandas DataFrame to S3 as a Parquet file.
        
        Args:
            df (pd.DataFrame): Data to be written
            bucket (str): Name of the S3 bucket.
            key (str): Object key (path) where the Parquet file will be stored.
            index (bool): Whether to include the DataFrame's index in the output file.
        """
        try:
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=index)
            buffer.seek(0)  # Rewind the buffer to the beginning
            self.s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
        except Exception as e:
            raise RuntimeError(f"Failed to write Parquet file to s3://{bucket}/{key}: {e}")
