import gzip
import io
from minio import Minio
from dotenv import load_dotenv
from typing import Literal
import os

LakeLayer = Literal["bronze", "silver", "gold"]


def make_minio_client():
    """
    Creates and returns a Minio client object.

    The function reads the necessary configuration values from the environment variables
    and uses them to initialize the Minio client.

    Note: This is just example code and should not be used in production.
    In reality you shouldn't directly use the root user credentials for Minio.
    This is provided in a config.env file that is version controlled for the sake of the example.
    You should have the environment variables set in a secure way via CI/CD or other means.
    On top of that, you should use HTTPS instead of HTTP.

    Returns:
        Minio: A Minio client object.

    Raises:
        ValueError: If the required environment variables are not set.
    """
    if os.path.exists("config.env"):
        load_dotenv("config.env")
    access_key = os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD")
    minio_uri = os.getenv("MINIO_URI")
    if not access_key:
        raise ValueError("MINIO_ROOT_USER not set")
    if not secret_key:
        raise ValueError("MINIO_ROOT_PASSWORD not set")
    if not minio_uri:
        minio_uri = "localhost:9000"

    return Minio(
        endpoint=minio_uri,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )


def ungzip_file(file_bytes: bytes) -> str:
    """
     Reads bytes representing a gzipped file and returns the decompressed content as a string.

    Args:
        file_bytes (bytes): The gzipped file content.

    Returns:
        str: The decompressed content as a string.
    """
    with gzip.open(io.BytesIO(file_bytes), mode="rb") as f_in:
        decompressed = f_in.read()
    return decompressed.decode("utf-8")


def read_bronze(source: str) -> dict[str, str]:
    """
      Read gzipped files from the bronze layer of the data lake.

    Args:
        source (str): The name of the source to read from. (e.g. "title.akas")

    Returns:
        dict[str, str]: A dictionary where the keys are the file paths and the values are the file contents.
    """
    files_to_process = download_files_from_bucket(source, "bronze")
    return {
        file_path: ungzip_file(file_bytes) for file_path, file_bytes in files_to_process
    }


def get_file_names_from_bucket(source: str, layer: LakeLayer | None) -> list[str]:
    """
    Get the names of the files in the bucket for a given source and layer.
    This is more or less the same as "ls" in a Unix-like system. but for a Minio bucket.

    Args:
        source (str): The source name. (e.g. "title.akas")
        layer (LakeLayer | None): The layer of the data lake to read from. (e.g. "bronze")

    Returns:
        list[str]: A list of file names.
    """
    prefix = f"{layer}/{source}" if layer else source
    client = make_minio_client()
    objects = client.list_objects("imdb-data", prefix=prefix, recursive=True)
    return [
        obj.object_name
        for obj in objects
        if obj.object_name and not obj.is_dir and obj.object_name.endswith(".gz")
    ]


def write_to_bucket(file_name: str, data: io.BytesIO) -> None:
    """
    Write a file to the bronze layer of the data lake.
    The function requires a BytesIO object as the file content.
    These are bytes that can be treated as a file.

    Args:
        file_name (str): The name of the file to write.
        data (io.BytesIO): The file content as a BytesIO object.
    """
    client = make_minio_client()
    if not client.bucket_exists("imdb-data"):
        client.make_bucket("imdb-data")
    part_size = 10 * 1024 * 1024
    client.put_object("imdb-data", file_name, data, length=-1, part_size=part_size)


def download_files_from_bucket(
    source: str, layer: LakeLayer | None
) -> list[tuple[str, bytes]]:
    """
    Download files from the data lake.

    Args:
        source (str): The name of the source to read from.
        layer (LakeLayer | None): The layer of the data lake to read from.
        if None, all layers are read.

    Returns:
        list[tuple[str, bytes]]: A list of tuples where each tuple contains the file name and the file content.
    """
    client = make_minio_client()
    file_names = get_file_names_from_bucket(source, layer)
    res = []
    for file_name in file_names:
        response = client.get_object("imdb-data", file_name)
        file_data = response.read()
        response.close()
        response.release_conn()
        res.append((file_name, file_data))
    return res
