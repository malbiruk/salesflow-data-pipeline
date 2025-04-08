import os

from dotenv import load_dotenv

load_dotenv()

connection_string = os.getnev("BLOB_CONNECTION_STRING")
container_name = os.getenv("BLOB_CONTAINER_NAME")
