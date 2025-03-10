import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
from config import tables_config
from google.cloud.sql.connector import Connector, IPTypes
import time
import logging
import pg8000
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import json
from datetime import datetime 

# Load environment variables from .env file
load_dotenv()

# Global dictionary to store uploaded files. To be written to the manifest.json
uploaded_files = {}

compression = 'zstd'

# Determine the logging level
debug_env_var = os.getenv("DEBUG")
logging_level = logging.DEBUG if debug_env_var else logging.INFO

# Configure logging
logging.basicConfig(
    level=logging_level,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

logger.info(f"DEBUG environment variable: {debug_env_var}")


def get_google_conn() -> pg8000.dbapi.Connection:
    # Initialize Connector object
    connector = Connector()
    instance_connection_name = os.getenv("INSTANCE_CONNECTION_NAME")
    if not instance_connection_name:
        raise ValueError("Instance connection name is not set in the environment variables.")
    logger.info(f"Instance connection name: {instance_connection_name}")
    try:
        conn: pg8000.dbapi.Connection = connector.connect(
            instance_connection_name,
            "pg8000",
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            db=os.getenv('DB_NAME'), 
            ip_type=IPTypes.PUBLIC
        )
        logger.info("Successfully created Google Cloud SQL connection")
        return conn
    except Exception as e:
        logger.error(f"Error creating Google Cloud SQL connection: {e}")
        raise

def create_sqlalchemy_engine() -> Engine:
    logger.info("Inside: Creating engine for database")
    # Get database connection parameters from environment variables
    db_params = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT')
    }

    # Check and print environment variables
    cloud_run_job = os.getenv("CLOUD_RUN_JOB")
    google_cloud_project = os.getenv("GOOGLE_CLOUD_PROJECT")
    k_service = os.getenv("K_SERVICE")
    
    logger.info(f"CLOUD_RUN_JOB: {cloud_run_job}")
    logger.info(f"GOOGLE_CLOUD_PROJECT: {google_cloud_project}")
    logger.info(f"K_SERVICE: {k_service}")
    
    if google_cloud_project or cloud_run_job or k_service:
        logger.info(f"Running on Google Cloud Project, connecting to the database using the Google Cloud Connector")
        engine = create_engine("postgresql+pg8000://", creator=get_google_conn)
    else:
        logger.info(f"Running locally, using the TCP socket")
        connection_string = f"postgresql+pg8000://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
        engine = create_engine(connection_string)
    return engine

def get_output_file(table_name, compression):
    if compression:
        return f"{table_name}_{compression}.parquet"
    else:
        return f"{table_name}.parquet"

def convert_memoryview_to_bytes(data):
    return data.tobytes() if isinstance(data, memoryview) else data

def write_manifest():
    timestamp = int(datetime.now().timestamp() * 1000)
    date_str = datetime.now().isoformat() + "Z"
    manifest = {
        "timestamp": timestamp,
        "dateStr": date_str,
        "files": uploaded_files
    }
    with open('manifest.json', 'w') as f:
        json.dump(manifest, f, indent=2)
    logger.info("Manifest file written successfully.")


def process_df(df, dtypes):
    for col in ['created_at', 'updated_at']:
        if col in df.columns and isinstance(df[col].dtype, pd.DatetimeTZDtype):
            df[col] = df[col].dt.tz_convert('UTC').dt.tz_localize(None)
    for col in df.columns:
        if col in dtypes:
            dtype = dtypes[col]
            if dtype == 'bytes':
                df[col] = df[col].apply(convert_memoryview_to_bytes)
            elif dtype == 'json': # "json" is not a numpy/pandas datatype but we want to preserve the original JSON and not convert it to Python object notation (e.g. with True instead of true). See https://github.com/verifier-alliance/parquet-export/issues/1
                df[col] = df[col].apply(json.dumps)
                dtype = 'string' # change it to "string"
            else:
                df[col] = df[col].astype(dtype)
            df[col] = df[col].astype(pd.UInt16Dtype() if dtype == 'UInt16' else dtype)
    return df

def get_pyarrow_type(dt):
    match dt:
        case 'bool':
            return pa.bool_()
        case 'Int32':
            return pa.int32()
        case 'Int64':
            return pa.int64()
        case 'string':
            return pa.string()
        case 'object':
            return pa.binary()
        case 'datetime64[ns]':
            return pa.timestamp('ns')
        case 'json':
            return pa.string()
        case _:
            raise ValueError("Type not supported")

def get_pyarrow_schema(dtypes):
    return pa.schema([pa.field(col, get_pyarrow_type(dt)) for col, dt in dtypes.items()])

def upload_to_s3(file_path, bucket_name, object_name):
    logger.info(f"Uploading {object_name} to S3")
    if os.getenv("DEBUG"):
        logger.debug("DEBUG: NOT uploading to S3 in DEBUG mode")
        return
    s3_client = boto3.client(
        's3',
        endpoint_url=os.getenv('S3_ENDPOINT_URL'),
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        logger.info(f"Successfully uploaded {file_path} to {bucket_name}/{object_name}")
        os.remove(file_path)
        logger.info(f"Deleted local file {file_path}")
    except FileNotFoundError:
        logger.error(f"The file {file_path} was not found")
    except NoCredentialsError:
        logger.error("Credentials not available")
    except PartialCredentialsError:
        logger.error("Incomplete credentials provided")

def fetch_and_write(table_config, engine):
    postgres_schema_name = os.getenv('DB_SCHEMA')
    table_name = table_config['name']
    dtypes = table_config['datatypes']
    schema = get_pyarrow_schema(dtypes)
    chunk_size = table_config['chunk_size']
    if os.getenv('DEBUG'):
        logger.debug(f"DEBUG: Setting chunk_size to 1/100 of {chunk_size} = {chunk_size // 100}")
        chunk_size = chunk_size // 100

    compression = table_config.get('compression', None)
    num_chunks_per_file = table_config['num_chunks_per_file']
    rows_per_file = chunk_size * num_chunks_per_file
    chunk_counter = 0
    file_counter = 0
    writer = None

    # Use stream_results=True to fetch data in chunks
    logger.info(f"Connecting to the DB for the table: {table_name}")
    with engine.connect().execution_options(stream_results=True) as connection:


        query = text(f"SELECT * FROM {postgres_schema_name}.{table_name}")
        if os.getenv('DEBUG_OFFSET'):
            query = text(f"SELECT * FROM {postgres_schema_name}.{table_name} OFFSET {os.getenv('DEBUG_OFFSET')}")
        logger.info(f"Executing query for table {table_name}: {query}")

        start_time = time.time()

        for chunk_dataframe in pd.read_sql_query(query, connection, chunksize=chunk_size):
            if os.getenv('DEBUG') and file_counter > 0:
                logger.debug(f"DEBUG: Breaking after writing 1 file")
                break
            end_time = time.time()
            logger.info(f"Retrieved {chunk_dataframe.shape[0]} rows chunk in {end_time - start_time:.2f} seconds")

            df = process_df(chunk_dataframe, dtypes)  # Process the dataframe to apply dtype conversion

            logger.info(f"Processed chunk {chunk_counter} of file {file_counter}")
            logger.info(f"DataFrame size: {df.memory_usage(deep=True).sum() / (1024 * 1024):.2f} MB")
            chunk_table = pa.Table.from_pandas(df, schema=schema) # Convert the dataframe to a PyArrow table

            if writer is None:
                # file name: contracts_0_10000_zstd.parquet, contracts_10000_20000_zstd.parquet, etc.
                output_file = get_output_file(f"{table_name}_{file_counter * rows_per_file}_{(file_counter + 1) * rows_per_file}", compression)
                writer = pq.ParquetWriter(output_file, chunk_table.schema, compression=compression)

            logger.info(f"Writing chunk {chunk_counter} of file {file_counter} to {output_file}")

            writer.write_table(chunk_table)

            chunk_counter += 1

            # If the number of chunks per file is reached, close the writer and upload the file
            if chunk_counter >= num_chunks_per_file:
                writer.close()
                logger.info(f"Written {output_file}")

                # Upload the file to S3
                object_name = f"{table_name}/{output_file}"
                upload_to_s3(output_file, os.getenv('S3_BUCKET_NAME'), object_name)

                # Append the file to the uploaded files list to be written to the manifest.json
                if table_name not in uploaded_files:
                    uploaded_files[table_name] = []
                uploaded_files[table_name].append(object_name)

                file_counter += 1
                chunk_counter = 0
                writer = None  # Reset the writer for the next file

            start_time = time.time()

        # Finally write the last remaining file if there are no remaining chunks
        if writer is not None:
            writer.close()
            logger.info(f"Written {output_file}")
            
            # Upload the file to S3
            object_name = f"{table_name}/{output_file}"
            upload_to_s3(output_file, os.getenv('S3_BUCKET_NAME'), object_name)
            
            # Append the file to the uploaded files list to be written to the manifest.json
            if table_name not in uploaded_files:
                uploaded_files[table_name] = []
            uploaded_files[table_name].append(object_name)


if __name__ == "__main__":
    logger.info("Creating engine for database")
    engine = create_sqlalchemy_engine()

    debug_table = os.getenv('DEBUG_TABLE') # To debug a specific table
    if debug_table:
        for table_config in tables_config:
            if table_config['name'] == debug_table:
                logger.info(f"Fetching and writing table: {table_config['name']}")
                fetch_and_write(table_config, engine)
                break
    else:
        for table_config in tables_config:
            logger.info(f"Fetching and writing table: {table_config['name']}")
            fetch_and_write(table_config, engine)
    write_manifest()  # Write the manifest file after processing all tables
    upload_to_s3('manifest.json', os.getenv('S3_BUCKET_NAME'), 'manifest.json')