import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Access environment variables
snow_creds={
    'user': os.getenv('snow.user'),
    'password' : os.environ['snow.password'],
    'account': os.getenv('snow.account'),
    'warehouse' : os.environ['snow.warehouse'],
    'database': os.getenv('snow.database'),
    'schema' : os.environ['snow.schema']
}
