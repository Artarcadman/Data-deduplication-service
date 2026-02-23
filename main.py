import os
import hashlib
import time
from dotenv import load_dotenv
from db_manager import DBManager
from storage_manager import StorageManager
from config import CHUNK_SIZES, FILE_READ_SIZE

load_dotenv()


def select_file(directory="./origin_data"):
    """Выбор файла из директории"""
    if not os.path.exists(directory): 
        os.makedirs(directory)
    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f)) and not f.startswith('.')]
    if not files:
        print(f"Папка {directory} пуста!")
        return None
    
    print("\nДоступные файлы:")
    for i, f in enumerate(files, 1): 
        print(f"{i}. {f}")  # noqa: E701
    
    while True:
        try:
            choice = int(input(f"\nВыберите файл (1-{len(files)}): "))
            if 1 <= choice <= len(files): 
                return os.path.join(directory, files[choice-1])
        except ValueError: 
            print("Введите только число!")


def get_full_file_hash(filepath):
    """Получить хэш всего файла"""
    hasher = hashlib.sha256()
    with open(filepath, 'rb') as f:
        while chunk := f.read(FILE_READ_SIZE): 
            hasher.update(chunk)
    return hasher.hexdigest()


def select_chunk_size():
    """Выбор размера сегмента для хэширования"""
    print("\nДоступные размеры сегментов:")
    for i, size in enumerate(CHUNK_SIZES, 1):
        print(f" {i}. {size} байт")
        
    while True:
        try:
            choice = int(input(f"\nВыберите размер (1-{len(CHUNK_SIZES)}): "))
            if 1 <= choice <= len(CHUNK_SIZES):
                return CHUNK_SIZES[choice-1]
        except ValueError:
            print("Введите число")


def process_file(filepath, chunk_size, db, storage):
    """Обработать файл по заданным размерам сегментам: хэширование, дедупликация"""
    file_name = os.path.basename(filepath)
    file_size = os.path.getsize(filepath)
    full_hash = get_full_file_hash(filepath)
    
    # 1. Проверка на дубликат всего файла
    if db.file_has_chunk_size(full_hash, chunk_size):
        print(f"Файл '{file_name}' с chunk_size = {chunk_size} уже был обработан ранее!")
        return file_name

    # 2. Регистрация нового файла
    
    file_id = db.register_file(file_name, full_hash, file_size)
    
    print(f"Начинаем обработку: {file_name}")
    start_time = time.time()
    
    with open(filepath, "rb") as f:
        idx = 0
        while True:
            chunk_data = f.read(chunk_size)
            if not chunk_data: 
                break
            
            c_hash = hashlib.sha256(chunk_data).hexdigest()
            offset = db.get_segment_offset(chunk_size, c_hash)
            
            if offset is None:
                # Новый уникальный сегмент
                offset = storage.write_segment(chunk_size, chunk_data)
                db.save_segment(chunk_size, c_hash, offset, len(chunk_data))
            else:
                # Дубликат сегмента
                db.increment_ref_count(chunk_size, c_hash)
            
            # Сохраняем структуру
            db.save_file_structure(file_id, idx, chunk_size, c_hash)
            idx += 1
            if idx % 1000 == 0: 
                print(f"Обработано {idx} сегментов...")
                
    db.mark_chunk_size_done(full_hash, chunk_size)
    print(f"Готово!\nВремя: {time.time() - start_time:.2f} сек.\nСегментов: {idx}")


def restore_file(file_id, file_name, chunk_size, db, storage):
    """Восстановление файла из сегментов"""
    
    print(f"\nВосстановление файла: {file_name} из {chunk_size}-байтных сегментов")
    
    recipe = db.get_file_recipe(file_id, chunk_size)
    if not recipe:
        print("Ошибка: контракт восстановления файла не найден в БД!")
        return

    out_path = f"restored_data/RESTORED_{file_name}"
    os.makedirs("restored_data", exist_ok=True)
    
    with open(out_path, "wb") as f:
        for seg_hash, offset, size in recipe:
            f.write(storage.read_segment(chunk_size, offset, size))
    
    print(f"Файл успешно восстановлен в: {out_path}")


def select_file_from_db(db: DBManager) -> tuple | None:
    """Выбор файла для восстановления из таблицы метаданных files в PostgreSQL"""
    
    with db.conn.cursor() as cur:
        cur.execute("SELECT file_id, file_name, file_hash, chunk_sizes_done FROM files ORDER BY file_id")
        rows = cur.fetchall()
        
    if not rows:
        print("В базе данных нет обработанных файлов...")
        return None
    
    print("\nОбработанные файлы:")
    for i, (fid, fname, fhash, sizes_done) in enumerate(rows, 1):
        print(f" {i}, {fname} (id: {fid}, chunk_sizes: {sizes_done})")
    
    while True:
        try:
            choice = int(input(f"\nВыберите файл (1-{len(rows)})"))
            if 1 <= choice <= len(rows):
                return rows[choice-1]
        except ValueError:
            print("Введите число")
            

def select_chunk_size_from_done(chunk_sizes_done: list) -> int | None:
    """
    Выбор размера сегмента для восстановления файла из доступных
    Нельзя восстановить файл по размеру сегмента, по которому он не был обработан
    """

    if not chunk_sizes_done:
        print("Файл не был обработан ни с одним размером сегмента")
        return None
    print("Доступные размеры сегментов для этого файла: ")
    for i, size in enumerate(chunk_sizes_done, 1):
        print(f" {i}. {size} байт")
        
    while True:
        try:
            choice = int(input(f"Выберите по каким сегментам начать восстановление (1-{len(chunk_sizes_done)})"))
            if 1 <= choice <= len(chunk_sizes_done):
                return chunk_sizes_done[choice - 1]
        except ValueError:
            print("Введите число")


if __name__ == "__main__":
    
    db_config = {
        "host": os.getenv("POSTGRES_HOST"),
        "database": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD")
    }
    
    db = DBManager(db_config)
    storage = StorageManager()
    
    # 1. Записать или восстановить
    # 2. Выбрать файл
    # 2. Выбрать размер сегмента
    # 3. Выбрать алгоритм
     
    inp = input("Выберите действие: \n1 - Записать файл \n2 - Восстановить файл \n")

    if inp == "1":
        selected = select_file()
        if selected:
            chunk_size = select_chunk_size()
            process_file(selected, chunk_size, db, storage)
            
    elif inp == "2":
        file_info = select_file_from_db(db)
        if file_info:
            file_id, file_name, file_hash, chunk_sizes_done = file_info
            chunk_size = select_chunk_size_from_done(chunk_sizes_done)
            if chunk_size:
                restore_file(file_id, file_name, chunk_size, db, storage)

        
    db.close()