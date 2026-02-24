import os
from dotenv import load_dotenv


load_dotenv()

# Размеры сегментов, для которых создаём таблицы
CHUNK_SIZES = [4, 32, 128, 1024]

# Размер фрагмента файла для хэширования
FILE_READ_SIZE = 1048576

# Алгоритмы
HASH_ALGORITHMS = ["md5", "sha256", "sha512"]


# Конфигурация PostgreSQL
def get_postgres_config() -> dict:
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", 5432)),
        "database": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "options": "-c client_encoding=UTF8",
    }
    
# Конфигурация MinIO    
def get_minio_config() -> dict:
    return {
        "host": os.getenv("MINIO_HOST", "localhost:9000"),
        "access_key": os.getenv("MINIO_ACCESS_KEY"),
        "secret_key": os.getenv("MINIO_SECRET_KEY"),
        "secure": os.getenv("MINIO_SECURE")
    }