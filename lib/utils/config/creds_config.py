import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Access environment variables
snow_creds={
    'user': os.getenv('SNOW_USER'),
    'password' : os.getenv('SNOW_PASSWORD'),
    'account': os.getenv('SNOW_ACCOUNT'),
    'warehouse' : os.getenv('SNOW_WAREHOUSE')
}

kaggle_creds={
    'username': os.getenv('KAGGLE_USERNAME'),
    'key': os.getenv('KAGGLE_KEY')
}

aws_creds={
    'access_key': os.getenv('AWS_ACCESS_KEY'),
    'secret_key': os.getenv('AWS_SECRET_KEY'),
    'region': os.getenv('AWS_REGION')
}