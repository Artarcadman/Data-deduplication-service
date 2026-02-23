import psycopg2
from psycopg2 import sql

class DBManager:
    def __init__(self, config):
        self.conn = psycopg2.connect(**config)
        self.conn.autocommit = True


    @staticmethod
    def _suffix(chunk_size: int, algo: str) -> str:
        """Суффикс для имён таблиц: '4096_sha256'"""
        return f"{chunk_size}_{algo}"
    

    # Файлы
    def file_exists(self, file_hash: str) -> bool:
        """Проверка: обрабатывали ли мы этот файл раньше по его хэшу"""
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM files WHERE file_hash = %s", (file_hash,))
            return cur.fetchone() is not None
        
        
    def file_has_processing(self, file_hash: str, chunk_size: int, algo: str) -> bool:
        """Проверка: обрабатывался ли файл с данной парой chunk_size - algo"""
        
        key = self._suffix(chunk_size, algo)
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM files WHERE file_hash = %s AND %s = ANY(processing_done)",
                (file_hash, key)
            )
            return cur.fetchone() is not None


    def mark_processing_done(self, file_hash, chunk_size, algo):
        """Отметить что файл обработан с данным chunk_size"""
        key = self._suffix(chunk_size, algo)
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE files
                SET processing_done = array_append(processing_done, %s)
                WHERE file_hash = %s AND NOT (%s = ANY(processing_done))
                """,
                (key, file_hash, key)
            )
        

    def register_file(self, file_name, file_hash, file_size):
        """Регистрация файла в базе"""
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO files (file_name, file_hash, file_size)
                VALUES (%s, %s, %s)
                ON CONFLICT (file_hash) DO UPDATE SET file_name = EXCLUDED.file_name 
                RETURNING file_id
                """,
                (file_name, file_hash, file_size)                        
                        
            )
            return cur.fetchone()[0]

    def get_file_id(self, file_hash: str) -> int | None:
        """Получить file_id по хэшу файла."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT file_id FROM files WHERE file_hash = %s", (file_hash,))
            row = cur.fetchone()
            return row[0] if row else None


    # Сегменты

    def get_segment_offset(self, chunk_size: int, algo: str, segment_hash: str) -> int:
        """Поиск: есть ли у нас уже такой уникальный сегмент. Если да - возвращает storage_offset"""
        
        table = sql.Identifier(f"unique_segments_{self._suffix(chunk_size, algo)}")
        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT storage_offset FROM {table} WHERE segment_hash = %s")
                .format(table=table),
                (segment_hash,),
            )
            row = cur.fetchone()
            return row[0] if row else None


    def save_segment(self, chunk_size: int, algo: str, segment_hash: str, storage_offset: int, segment_size: int):
        """Запись нового уникального сегмента - хэша, размера и его позиции в файле"""
        
        table = sql.Identifier(f"unique_segments_{self._suffix(chunk_size, algo)}")
        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL("""
                        INSERT INTO {table} (segment_hash, storage_offset, segment_size) 
                        VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING
                        """).format(table=table),
                (segment_hash, storage_offset, segment_size)
            )


    def increment_ref_count(self, chunk_size: int, algo: str, segment_hash: str):
        """Увеличение счетчика повторений если сегмент встретился повторно"""
        
        table = sql.Identifier(f"unique_segments_{self._suffix(chunk_size, algo)}")
        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL("UPDATE {table} SET repits = repits + 1 WHERE segment_hash = %s")
                        .format(table=table),
                        (segment_hash,),
                        )
                

    def save_file_structure(self, chunk_size: int, algo: str, file_id: int, chunk_index: int,  segment_hash: str):
        """Запись строки в контракт сборки"""
        table = sql.Identifier(f"file_chunks_{self._suffix(chunk_size, algo)}")
        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("""
                                INSERT INTO {table} (file_id, chunk_index, segment_hash) 
                                VALUES (%s, %s, %s)
                                """).format(table=table), 
                (file_id, chunk_index, segment_hash),)

    
    # Восстановление
    
    def get_file_recipe(self, file_id: int, chunk_size: int, algo: str) -> list[tuple[str, int, int]]:
        """
        Рецепт сборки файла.
        Возвращает [(segment_hash, storage_offset, segment_size), ...]
        по порядку chunk_index.
        """
        fc = sql.Identifier(f"file_chunks_{self._suffix(chunk_size, algo)}")
        us = sql.Identifier(f"unique_segments_{self._suffix(chunk_size, algo)}")

        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL("""
                    SELECT fc.segment_hash, us.storage_offset, us.segment_size
                    FROM {fc} fc
                    JOIN {us} us ON fc.segment_hash = us.segment_hash
                    WHERE fc.file_id = %s
                    ORDER BY fc.chunk_index ASC
                """).format(fc=fc, us=us),
                (file_id,),
            )
            return cur.fetchall()


    



        
    def close(self):
        self.conn.close()