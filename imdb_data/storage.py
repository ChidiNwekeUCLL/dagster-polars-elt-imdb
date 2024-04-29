import gzip
import io
from minio import Minio
from dotenv import load_dotenv
from typing import Literal
import os

LakeLayer = Literal["bronze", "silver", "gold"]


def make_minio_client():
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
    with gzip.open(io.BytesIO(file_bytes), mode="rb") as f_in:
        decompressed = f_in.read()
    return decompressed.decode("utf-8")


def read_bronze(source: str) -> dict[str, str]:
    files_to_process = download_files_from_bucket(source, "bronze")
    return {
        file_path: ungzip_file(file_bytes) for file_path, file_bytes in files_to_process
    }


def get_file_names_from_bucket(source: str, layer: LakeLayer | None) -> list[str]:
    prefix = f"{layer}/{source}" if layer else source
    client = make_minio_client()
    objects = client.list_objects("imdb-data", prefix=prefix, recursive=True)
    return [
        obj.object_name
        for obj in objects
        if obj.object_name and not obj.is_dir and obj.object_name.endswith(".gz")
    ]


def write_to_bucket(file_name: str, data: io.BytesIO) -> None:
    client = make_minio_client()
    part_size = 10 * 1024 * 1024
    client.put_object("imdb-data", file_name, data, length=-1, part_size=part_size)


def download_files_from_bucket(
    source: str, layer: LakeLayer | None
) -> list[tuple[str, bytes]]:
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
