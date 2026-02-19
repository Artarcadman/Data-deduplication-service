import psycopg2
from psycopg2 import sql

class DBManager:
    def __init__(self, config):
        self.conn = psycopg2.connect(**config)
        self.conn.autocommit = True

    def file_exists(self, file_hash: str) -> bool:
        """Проверка: обрабатывали ли мы этот файл раньше по его хэшу"""
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM files WHERE file_hash = %s", (file_hash,))
            return cur.fetchone() is not None


    def register_file(self, file_name, file_hash, chunk_size):
        """Регистрация файла в базе"""
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO files (file_name, file_hash, file_size)
                VALUES (%s, %s, %s)
                ON_CONFLICT (file_hash) DO UPDATE SET file_name = EXCLUDED.file_name 
                RETURNING file_id,
                """,
                (file_name, file_hash, chunk_size)                        
                        
            )
            return cur.fetchone()[0]

    def get_segment_offset(self, chunk_size: int, segment_hash: str) -> int:
        """Поиск: есть ли у нас уже такой уникальный сегмент. Если да - возвращает storage_offset"""
        
        table = sql.Identifier(f"unique_segments_{chunk_size}")
        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT storage_offset FROM {table} WHERE segment_hash = %s")
                .format(table=table),
                (segment_hash,),
            )
            row = cur.fetchone()
            return row[0] if row else None

    def save_unique_segment(self, chunk_size: int, segment_hash: str, storage_offset: int, segment_size: int):
        """Запись нового уникального хэша и его позиции в файле"""
        
        table = sql.Identifier(f"unique_segments_{chunk_size}")
        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL("""
                        INSERT INTO {table} (segment_hash, storage_offset, segment_size) 
                        VALUES (%s, %s) ON CONFLICT DO NOTHING")
                        ON CONFLICTS DO NOTHING
                        """).format(table=table),
                (segment_hash, storage_offset, segment_size)
            )

    def increment_ref_count(self, chunk_hash):
        """Увеличение счетчика повторений"""
        with self.conn.cursor() as cur:
            cur.execute("UPDATE unique_segments SET ref_count = ref_count + 1 WHERE hash = %s", (chunk_hash,))

    def save_file_structure(self, file_id, chunk_index, hash_ref):
        """Запись 'рецепта' сборки файла"""
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO file_structure (file_id, chunk_index, hash_ref) VALUES (%s, %s, %s)",
                (file_id, chunk_index, hash_ref)
            )

    def get_file_recipe(self, file_name):
        """Получение всех сегментов для восстановления файла"""
        with self.conn.cursor() as cur:
            query = """
                SELECT us.storage_offset, pf.chunk_size
                FROM file_structure fs
                JOIN unique_segments us ON fs.hash_ref = us.hash
                JOIN processed_files pf ON fs.file_id = pf.id
                WHERE pf.file_name = %s
                ORDER BY fs.chunk_index ASC
            """
            cur.execute(query, (file_name,))
            return cur.fetchall()

    def close(self):
        self.conn.close()