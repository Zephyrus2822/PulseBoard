import os
from dotenv import load_dotenv
from fastapi import HTTPException
from pymongo import MongoClient
from logger import get_logger
from typing import List, Dict, Any
import traceback
import hashlib
import requests
import weaviate
from weaviate.util import generate_uuid5
from weaviate.classes.init import Auth
from datetime import datetime, timedelta, timezone
import redis
import uuid
from pymongo import ReturnDocument
import ast

logger = get_logger()
load_dotenv()
# mongo
mongo_user = os.getenv("MONGODB_USERNAME")
mongo_password = os.getenv("MONGODB_PASSWORD")
mongo_host = os.getenv("MONGODB_HOST")
mongo_port = os.getenv("MONGODB_PORT")
mongo_auth_mechanism = os.getenv("MONGODB_AUTHMECHANISM")

weaviate_api_key = os.getenv("WEAVIATE_API_KEY")
weaviate_host = os.getenv("WEAVIATE_HOST")
weaviate_port = os.getenv("WEAVIATE_PORT")
weaviate_is_secure = os.getenv("WEAVIATE_SECURE")
weaviate_grpc_host = os.getenv("WEAVIATE_GRPC_HOST")
weaviate_grpc_port = os.getenv("WEAVIATE_GRPC_PORT")
DATASET_TYPE = "PRODUCT CATALOG"
# Construct the MongoDB connection string
MONGO_CONNECTION_STRING = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/?authMechanism={mongo_auth_mechanism}"


def connect_to_weaviatedb():
    """
    Connects to the Weaviate DB instance using the provided connection string.

    Returns:
        WeaviateClient: The MongoDB client instance.
    """
    try:
        if not all(
            [weaviate_host, weaviate_port, weaviate_grpc_host, weaviate_grpc_port]
        ):
            raise ValueError("Missing required Weaviate connection parameters.")

        # Use secure or insecure connection based on configuration
        auth = Auth.api_key(weaviate_api_key) if weaviate_api_key else None

        client = weaviate.connect_to_custom(
            http_host=weaviate_host,
            http_port=weaviate_port,
            http_secure=weaviate_is_secure,
            grpc_host=weaviate_grpc_host,
            grpc_port=weaviate_grpc_port,
            grpc_secure=weaviate_is_secure,
            auth_credentials=auth,
        )

        if client.is_ready():
            logger.debug("Connected successfully to WeaviateDB!")
            return client
        else:
            logger.error("Weaviate client is not ready.")
            return None
    except Exception as e:
        logger.error(f"Failed to connect to WeaviateDB: {e}")
        return None


def connect_to_mongodb():
    """
    Connects to the MongoDB instance using the provided connection string.

    Returns:
        MongoClient: The MongoDB client instance.
    """
    try:
        client = MongoClient(MONGO_CONNECTION_STRING)
        logger.debug("Connected successfully to MongoDB!")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        return None