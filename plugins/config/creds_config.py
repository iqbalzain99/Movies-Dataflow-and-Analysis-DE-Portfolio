import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Access environment variables
snow_creds={
    'user': os.getenv('snow.user'),
    'password' : os.getenv('snow.password'),
    'account': os.getenv('snow.account'),
    'warehouse' : os.getenv('snow.warehouse'),
    'database': os.getenv('snow.database'),
    'schema' : os.getenv('snow.schema')
}

kaggle_creds={
    'username': os.getenv('kaggle.username'),
    'key': os.getenv('kaggle.key')
}
