import time
import psycopg2
import hashlib
import os
from dotenv import load_dotenv

# --- Load /.env variables --- 
load_dotenv()
HOST = os.getenv("HOST")
DATABASE = os.getenv("DATABASE")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")

# --- PosgreSQL Configuration ---
DB_CONFIG = {
    "host": HOST,
    "database": DATABASE,  
    "user": USER,            
    "password": PASSWORD
}

# --- Input file choice ---
def select_file(directory = "./origin_data"):
    """
    Makes list of files in "./origin_data" directory
    """
    try:
        files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    except FileNotFoundError:
        print("Directory not found")
        return None
    if not files:
        print(f'Directory "{directory}" has no files')
        return None
    
    print(f'Available files in "{directory}":')
    for i, file in enumerate(files):
        print(f"{i+1}. {file}")

    while True:
        choice = input(f"Choose file (1 - {len(files)})")
        try:
            index = int(choice)-1
            if 0 <= index <= len(files):
                return os.path.join(directory, files[index])
            else:
                print("This file not exists. Try your BEST again")
        except ValueError:
            print("Only numbers")
                 
INPUT_FILENAME = select_file(directory=".\origin_data")
if INPUT_FILENAME:
    print(f"You work with {INPUT_FILENAME}.")
else:
    exit()


# --- Algorithm ---

CHUNK_SIZE = 4  # Размер сегмента в байтах (по условию курсового)

def connect_db():
    """Устанавливает соединение с PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Ошибка подключения к БД. Проверьте: запущен ли PostgreSQL, верны ли user/password. Ошибка: {e}")
        return None

def process_file(conn, filename, chunk_size):
    """
    Чтение бинарного файла по сегментам (4 байта), вычисление хэша,
    хранение самого сегмента (chunk_data) и обновление/вставка данных.
    """
    if not os.path.exists(filename):
        print(f"Ошибка: Файл '{filename}' не найден.")
        return

    cursor = conn.cursor()
    file_size = os.path.getsize(filename)
    segment_offset = 0  # Смещение в байтах
    processed_count = 0
    start_time = time.time()
    
    # 1. ОТКРЫТИЕ ФАЙЛА В БИНАРНОМ РЕЖИМЕ ('rb')
    with open(filename, 'rb') as f: 
        while True:
            # Читаем фиксированный сегмент (4 байта)
            chunk = f.read(chunk_size)
            if not chunk:
                break

            # Вычисляем хэш
            hash_object = hashlib.sha256(chunk)
            hash_value = hash_object.hexdigest()

            # UPSERT: Сначала пытаемся обновить счетчик (если хэш уже есть)
            # Внимание: при повторении данных, мы не обновляем chunk_data, 
            # так как он будет одинаковым.
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
                # ВСТАВЛЯЕМ НОВЫЙ СТОЛБЕЦ chunk_data
                cursor.execute(
                    """
                    INSERT INTO file_chunks (hash_value, file_name, segment_offset, repetition_count, chunk_data)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    # Передаем сам chunk (бинарные данные)
                    (hash_value, filename, segment_offset, 1, chunk) 
                )

            segment_offset += chunk_size
            processed_count += 1

            if processed_count % 10000 == 0:
                print(f"Прогресс: обработано {processed_count} сегментов...")

    conn.commit() # Фиксируем все изменения
    end_time = time.time()
    
    print("-" * 40)
    print(f"   Обработка завершена.")
    print(f"   Файл: {filename}")
    print(f"   Размер сегмента: {chunk_size} байт")
    print(f"   Общее время: {end_time - start_time:.2f} сек.")
    print(f"   Общее количество сегментов: {processed_count}")
    print("-" * 40)


if __name__ == "__main__":
    db_connection = connect_db()
    
    if db_connection:
        process_file(db_connection, INPUT_FILENAME, CHUNK_SIZE)
        db_connection.close()