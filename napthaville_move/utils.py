import os
import json
import logging
import logging.config
import backoff
import ipfshttpclient
from typing import Dict, Any
from requests.exceptions import ConnectionError, ReadTimeout

IPFS_GATEWAY_URL = os.getenv("IPFS_GATEWAY_URL", None)
BASE_OUTPUT_DIR = os.getenv("BASE_OUTPUT_DIR", None)
PERSONAS_FOLDER = f"{BASE_OUTPUT_DIR}/napthaville/step-3-3/personas"
MAX_TRIES = 5
MAX_TIME = 180
BASE_MAZE_IPFS_HASH = "QmWrCkdJHVb5MfQuL1yXh6Wt2Dxp7ajJPDH7cRRdEuBvAK"
MAZE_FOLDER = f"{BASE_OUTPUT_DIR}/maze/{BASE_MAZE_IPFS_HASH}/matrix"


def setup_logging(default_level=logging.INFO, log_file="app.log"):
    """
    Setup logging configuration
    """
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            },
        },
        "handlers": {
            "console": {
                "level": "DEBUG",
                "class": "logging.StreamHandler",
                "formatter": "standard",
            },
            "file": {
                "level": "INFO",
                "class": "logging.FileHandler",
                "formatter": "standard",
                "filename": log_file,
            },
        },
        "loggers": {
            "": {
                "handlers": ["console", "file"],
                "level": default_level,
                "propagate": True,
            },
        },
    }

    logging.config.dictConfig(logging_config)


@backoff.on_exception(
    backoff.expo,
    (
        ipfshttpclient.exceptions.ErrorResponse,
        ipfshttpclient.exceptions.TimeoutError,
        ConnectionError,
        ReadTimeout,
    ),
    max_tries=MAX_TRIES,
    max_time=MAX_TIME,
)
def upload_maze_json_to_ipfs(maze_json: Dict[str, Any]) -> str:
    """
    Upload maze_json to IPFS and return the IPFS hash.
    Includes exponential backoff and retry mechanism.

    Args:
    maze_json (Dict[str, Any]): The maze JSON to upload

    Returns:
    str: The IPFS hash of the uploaded maze_json
    """
    try:
        with ipfshttpclient.connect(IPFS_GATEWAY_URL) as client:
            # Convert maze_json to a JSON string
            maze_json_str = json.dumps(maze_json)

            # Add the JSON string to IPFS
            res = client.add_str(maze_json_str)

            # Return the IPFS hash
            return res
    except (
        ipfshttpclient.exceptions.ErrorResponse,
        ipfshttpclient.exceptions.TimeoutError,
    ) as e:
        print(f"Error uploading to IPFS (will retry): {str(e)}")
        raise
    except Exception as e:
        print(f"Unexpected error uploading to IPFS: {str(e)}")
        raise


@backoff.on_exception(
    backoff.expo,
    (
        ipfshttpclient.exceptions.ErrorResponse,
        ipfshttpclient.exceptions.TimeoutError,
        ConnectionError,
        ReadTimeout,
    ),
    max_tries=MAX_TRIES,
    max_time=MAX_TIME,
)
def retrieve_maze_json_from_ipfs(ipfs_hash: str) -> Dict[str, Any]:
    """
    Retrieve maze_json from IPFS using the provided hash.
    Includes exponential backoff and retry mechanism.

    Args:
    ipfs_hash (str): The IPFS hash of the maze_json to retrieve

    Returns:
    Dict[str, Any]: The retrieved maze JSON
    """
    try:
        with ipfshttpclient.connect(IPFS_GATEWAY_URL) as client:
            # Get the JSON string from IPFS
            maze_json_str = client.cat(ipfs_hash)

            # Parse the JSON string back into a dictionary
            maze_json = json.loads(maze_json_str)

            return maze_json
    except (
        ipfshttpclient.exceptions.ErrorResponse,
        ipfshttpclient.exceptions.TimeoutError,
    ) as e:
        print(f"Error retrieving from IPFS (will retry): {str(e)}")
        raise
    except Exception as e:
        print(f"Unexpected error retrieving from IPFS: {str(e)}")
        raise


def get_folder_from_ipfs(ipfs_hash: str, base_folder: str):
    try:
        with ipfshttpclient.connect(IPFS_GATEWAY_URL) as client:
            client.get(ipfs_hash, base_folder)
    except Exception as e:
        print(f"Error getting persona folder: {str(e)}")
        raise


def retrieve_json_from_ipfs(ipfs_hash: str):
    try:
        with ipfshttpclient.connect(IPFS_GATEWAY_URL) as client:
            res = client.get_json(ipfs_hash)
            return res
    except Exception as e:
        print(f"Error getting persona folder: {str(e)}")
        raise


def upload_json_file_to_ipfs(file_path: str):
    try:
        with open(file_path, "r") as file:
            json_data = json.load(file)

        with ipfshttpclient.connect(IPFS_GATEWAY_URL) as client:
            res = client.add_json(json_data)
            return res
    except Exception as e:
        print(f"Error getting persona folder: {str(e)}")
        raise


if not os.path.exists(MAZE_FOLDER):
    get_folder_from_ipfs(BASE_MAZE_IPFS_HASH, f"{BASE_OUTPUT_DIR}/maze")
