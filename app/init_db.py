"""
Инициализация схемы БД.

"""
import os
os.environ["PGCLIENTENCODING"] = "UTF8"

import psycopg2
from psycopg2 import sql
from app.config import get_postgres_config, CHUNK_SIZES, HASH_ALGORITHMS


def create_schema(conn):
    
    # Таблица 1: files - реестр обработанных файлов
    # file_id
    # file_name
    # file_hash - общий хэш файла
    # file_size
    # processing_done - словарь комбинаций размеров хэшируемых сегментов и алгоритмов, по которым был хэширован файл
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS files (
                file_id          SERIAL    PRIMARY KEY,
                file_name        TEXT      NOT NULL,
                file_hash        TEXT      NOT NULL UNIQUE,
                file_size        BIGINT    NOT NULL,
                processing_done TEXT[] DEFAULT '{}'
            );
        """)
        print("Таблица файлов создана")

        # Таблица storage_index_{size}
        for size in CHUNK_SIZES:
            si = f"storage_index_{size}"
            cur.execute(sql.SQL("""
                                CREATE TABLE IF NOT EXISTS {table} (
                                    content_hash TEXT PRIMARY KEY,
                                    storage_offset BIGINT NOT NULL,
                                    segment_size INTEGER NOT NULL
                                );
                                """).format(table=sql.Identifier(si)))
            print(f"Таблица для {si} создана")
        
        # Для каждой пары "chunk_size - algo" создаём пару таблиц
        for size in CHUNK_SIZES:
            for algo in HASH_ALGORITHMS:
                suffix = f"{size}_{algo}"
                us = f"unique_segments_{suffix}"
                fc = f"file_chunks_{suffix}"
                
                # Таблица 2: unique_segments_{size}_{algo} - каталог уникальных сегментов
                # segment_hash  - хэш сегмента (он же ключ объекта в MinIO)
                # segment_size  - реальный размер в байтах (последний чанк < chunk_size)
                # position_in_storage - позиция в хранилище
                # ref_count     - сколько раз встретился
                cur.execute(sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {table} (
                        
                        segment_hash        TEXT    PRIMARY KEY,
                        segment_size        INTEGER NOT NULL,
                        storage_offset      BIGINT  NOT NULL,
                        repits              INTEGER NOT NULL DEFAULT 1
                    );
                """).format(table=sql.Identifier(us)))
                print(f"Таблица для {us} создана")
                
                
                # Таблица 3: file_chunks_{size} - рецепт сборки файла
                
                # file_id
                # chunk_index - порядковый номер сегмента (0, 1, 2, ...)
                # segment_hash  - хэш сегмента, по нему достаём данные из MinIO
                cur.execute(sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {fc} (
                file_id       INTEGER NOT NULL REFERENCES files(file_id),
                chunk_index   INTEGER NOT NULL,
                segment_hash  TEXT    NOT NULL REFERENCES {us}(segment_hash),
                PRIMARY KEY (file_id, chunk_index));
                """).format(fc=sql.Identifier(fc), us=sql.Identifier(us)))
                
                print(f"Таблица для {fc} создана")

    tables_count = 1 + len(CHUNK_SIZES) + len(CHUNK_SIZES) * len(HASH_ALGORITHMS) * 2
    return tables_count
        
def main():
    print("Подключение к PostgreSQL...")
    
    try:
        conn = psycopg2.connect(**get_postgres_config())
        conn.autocommit = True
        print("Подключено")
    except Exception as e:
        print(f"Ошибка: {e}")
        return

    print("Создание таблиц:")
    tables_count = create_schema(conn)
    conn.close()

    print(f"\nГотово: {tables_count} таблиц создано.")


if __name__ == "__main__":

    main()