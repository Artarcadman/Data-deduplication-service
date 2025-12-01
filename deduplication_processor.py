import psycopg2
import hashlib
import os

# --- КОНФИГУРАЦИЯ БАЗЫ ДАННЫХ (PostgreSQL) ---
DB_CONFIG = {
    "host": "localhost",
    "database": "deduplication_db",  # Имя базы, которую вы создали
    "user": "postgres",            # Ваше имя пользователя
    "password": "artem"  # ОБЯЗАТЕЛЬНО ЗАМЕНИТЕ ЭТО НА ВАШ ПАРОЛЬ
}

# --- КОНФИГУРАЦИЯ АЛГОРИТМА ---
INPUT_FILENAME = "text_data.txt"
CHUNK_SIZE = 4  # Размер сегмента в байтах (по условию курсового)

# --- Инициализация ---
def connect_db():
    """Устанавливает соединение с PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        return None

def process_file(conn, filename, chunk_size):
    """Читает файл, вычисляет хэши и обновляет/вставляет данные в БД"""
    cursor = conn.cursor()
    file_size = os.path.getsize(filename)
    segment_offset = 0

    print(f"Обработка файла: {filename} (размер: {file_size} байт)")

    with open(filename, 'rb') as f:
        while True:
            # 1. Читаем сегмент (chunk)
            chunk = f.read(chunk_size)
            if not chunk:
                break

            # 2. Вычисляем хэш
            hash_object = hashlib.sha256(chunk)
            hash_value = hash_object.hexdigest()

            # 3. Логика INSERT OR UPDATE (UPSERT)
            # Сначала пытаемся обновить счетчик (если хэш уже есть)
            cursor.execute(
                """
                UPDATE file_chunks
                SET repetition_count = repetition_count + 1
                WHERE hash_value = %s
                """,
                (hash_value,)
            )

            # Если обновление не затронуло ни одной строки (хэш новый), делаем INSERT
            if cursor.rowcount == 0:
                cursor.execute(
                    """
                    INSERT INTO file_chunks (hash_value, file_name, segment_offset, repetition_count)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (hash_value, filename, segment_offset, 1)
                )

            segment_offset += chunk_size
    
    conn.commit() # Фиксируем все изменения транзакцией
    print(f"Обработка завершена. Всего обработано сегментов: {segment_offset // chunk_size}")


if __name__ == "__main__":
    db_connection = connect_db()
    
    if db_connection:
        process_file(db_connection, INPUT_FILENAME, CHUNK_SIZE)
        db_connection.close()
        print("Соединение с БД закрыто.")