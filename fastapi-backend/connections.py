import boto3
import configparser
from pymongo import MongoClient
import snowflake.connector
from openai import OpenAI

config = configparser.ConfigParser()
config.read('configuration.properties')

def aws_connection():
    try:
        # s3 connection details
        aws_access_key = config['AWS']['access_key']
        aws_secret_key = config['AWS']['secret_key']
        bucket_name = config['s3-bucket']['bucket']

        s3_client = boto3.client(
            's3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)

        return s3_client, bucket_name

    except Exception as e:
        print("Exception in aws_connection function: ", e)
        return

def mongo_connection():
    try:
        client = MongoClient(config['MONGODB']['MONGODB_URL'])
        db = client[config['MONGODB']["DATABASE_NAME"]]
        return db
    except Exception as e:
        print("Exception in mongodb_connection function: ",e)
        
def snowflake_connection():
    try:
        user = config['SNOWFLAKE']['user']
        password = config['SNOWFLAKE']['password']
        account = config['SNOWFLAKE']['account']
        role = config['SNOWFLAKE']['role']
        warehouse = config['SNOWFLAKE']['warehouse']
        database = config['SNOWFLAKE']['database']
        schema = config['SNOWFLAKE']['schema']
        table = config['SNOWFLAKE']['jobsTable']

        conn = snowflake.connector.connect(
                    user=user,
                    password=password,
                    account=account,
                    warehouse=warehouse,
                    database=database,
                    schema=schema,
                    role=role
                    )
        
        return conn, table
    except Exception as e:
        print("Exception in snowflake_connection function: ",e)
        return 
        
def pinecone_connection():
    try:
        pinecone_api_key = config['PINECONE']['pinecone_api_key']
        index_name = config['PINECONE']['index']
        return pinecone_api_key, index_name
    except Exception as e:
        print("Exception in pinecone_connection function: ",e)
        return  
    
def openai_connection():
    try:
        api_key = config['OPENAI']['api_key']
        
        return api_key
    except Exception as e:
        print("Exception in openai_connection function: ",e)
        return