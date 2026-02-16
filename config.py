import os
from dotenv import load_dotenv

load_dotenv()

# Размеры сегментов, для которых создаём таблицы
CHUNK_SIZES = [4, 1024, 4096, 65536, 1048576]


def get_postgres_config() -> dict:
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", 5432)),
        "database": os.getenv("POSTGRES_DB", "db_dedupl"),
        "user": os.getenv("POSTGRES_USER", "artem"),
        "password": os.getenv("POSTGRES_PASSWORD", "artem"),
    }