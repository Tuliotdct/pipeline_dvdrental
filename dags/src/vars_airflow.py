import os
from dotenv import load_dotenv


def get_variable(var_name, default=None):
    # This function allows access to variables defined in Airflow and in the .env file
    
    load_dotenv()
    try:
        from airflow.sdk import Variable
        return Variable.get(var_name, default=default or os.getenv(var_name))
    
    except (ImportError, Exception):
        return default or os.getenv(var_name)
