"""
Инициализация схемы БД.

"""

import psycopg2
from psycopg2 import sql
from config import get_postgres_config, CHUNK_SIZES


def create_schema(conn):
    
    # Таблица 1: files - реестр обработанных файлов
    # file_id
    # file_name
    # file_hash - общий хэш файла
    # file_size
    # chunk_sizes_done - словарь размеров хэшируемых сегментов, по которым был хэширован файл
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS files (
                file_id          SERIAL    PRIMARY KEY,
                file_name        TEXT      NOT NULL,
                file_hash        TEXT      NOT NULL UNIQUE,
                file_size        BIGINT    NOT NULL,
                chunk_sizes_done INTEGER[] DEFAULT '{}'
            );
        """)
        print("Таблица файлов создана")


        # Для каждого chunk_size создаём пару таблиц
        for size in CHUNK_SIZES:

            fc = f"file_chunks_{size}"
            us = f"unique_segments_{size}"

            # Таблица 2: file_chunks_{size} - рецепт сборки файла
            
            # file_id
            # chunk_index - порядковый номер сегмента (0, 1, 2, ...)
            # segment_hash  - хэш сегмента, по нему достаём данные из MinIO
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {table} (
                    file_id       INTEGER NOT NULL REFERENCES files(file_id),
                    chunk_index   INTEGER NOT NULL,
                    segment_hash  TEXT    NOT NULL,
                    PRIMARY KEY (file_id, chunk_index)
                );
            """).format(table=sql.Identifier(fc)))
            print(f"Таблица для {fc} создана")

            # Таблица 3: unique_segments_{size} — каталог уникальных сегментов
            # segment_hash  — хэш сегмента (он же ключ объекта в MinIO)
            # segment_size  — реальный размер в байтах (последний чанк < chunk_size)
            # ref_count     — сколько раз встретился
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {table} (
                    segment_hash   TEXT    PRIMARY KEY,
                    segment_size   INTEGER NOT NULL,
                    storage_offset BIGINT  NOT NULL,
                    ref_count      INTEGER NOT NULL DEFAULT 1
                );
            """).format(table=sql.Identifier(us)))
            print(f"Таблица для {us} создана")


def main():
    print("Подключение к PostgreSQL...")
    try:
        conn = psycopg2.connect(**get_postgres_config())
        conn.autocommit = True
        print("Подключено\n")
    except Exception as e:
        print(f"Ошибка: {e}")
        return

    print("Создание таблиц:")
    create_schema(conn)
    conn.close()

    print(f"\nГотово: 1 + {len(CHUNK_SIZES) * 2} таблиц создано.")


if __name__ == "__main__":
    main()