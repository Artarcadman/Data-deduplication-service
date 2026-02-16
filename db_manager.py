import psycopg2

class DBManager:
    def __init__(self, config):
        self.conn = psycopg2.connect(**config)
        self.conn.autocommit = True

    def check_file_exists(self, file_hash):
        """Проверка: обрабатывали ли мы этот файл раньше по его хэшу"""
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM processed_files WHERE file_hash = %s", (file_hash,))
            return cur.fetchone() is not None
    def check_file_hash_exists(self, file_hash, chunk_size):
        """Проверка: обрабатывался ли этот файл с заданным чанком"""
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM processed_files WHERE file_hash = %s, %s", (file_hash, chunk_size))
            return 

    def register_file(self, file_name, file_hash, chunk_size):
        """Регистрация файла в базе"""
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO processed_files (file_name, file_hash, chunk_size) VALUES (%s, %s, %s) RETURNING id",
                (file_name, file_hash, chunk_size)
            )
            return cur.fetchone()[0]

    def get_segment_offset(self, chunk_hash):
        """Поиск: есть ли у нас уже такой уникальный сегмент"""
        with self.conn.cursor() as cur:
            cur.execute("SELECT storage_offset FROM unique_segments WHERE hash = %s", (chunk_hash,))
            res = cur.fetchone()
            return res[0] if res else None

    def save_unique_segment(self, chunk_hash, offset):
        """Запись нового уникального хэша и его позиции в файле"""
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO unique_segments (hash, storage_offset) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (chunk_hash, offset)
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