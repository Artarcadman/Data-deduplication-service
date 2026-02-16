import os
import hashlib
import time
from dotenv import load_dotenv
from db_manager import DBManager
from storage_manager import StorageManager

load_dotenv()
SIZES = [4, 1024, 4096, 65536, 1048576]
CHUNK_SIZE = SIZES[3]
FILE_CHUNK_SIZE = 1048576


def select_file(directory="./origin_data"):
    if not os.path.exists(directory): os.makedirs(directory)
    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f)) and not f.startswith('.')]
    if not files:
        print(f"Папка {directory} пуста!")
        return None
    
    print("\nДоступные файлы:")
    for i, f in enumerate(files, 1): print(f"{i}. {f}")  # noqa: E701
    
    while True:
        try:
            choice = int(input(f"\nВыберите файл (1-{len(files)}): "))
            if 1 <= choice <= len(files): return os.path.join(directory, files[choice-1])
        except ValueError: print("Введите только число!")

def get_full_file_hash(filepath):
    hasher = hashlib.sha256()
    with open(filepath, 'rb') as f:
        while chunk := f.read(FILE_CHUNK_SIZE): hasher.update(chunk)
    return hasher.hexdigest()

def process_file_logic(filepath, db, storage):
    file_name = os.path.basename(filepath)
    full_hash = get_full_file_hash(filepath)
    
    # 1. Проверка на дубликат всего файла
    if db.check_file_exists(full_hash):
        print(f"Файл '{file_name}' уже был обработан ранее!")
        return file_name

    # 2. Регистрация нового файла
    print(f"Начинаем обработку: {file_name}")
    file_id = db.register_file(file_name, full_hash, CHUNK_SIZE)
    
    start_time = time.time()
    with open(filepath, "rb") as f:
        idx = 0
        while True:
            chunk_data = f.read(CHUNK_SIZE)
            if not chunk_data: break
            
            c_hash = hashlib.sha256(chunk_data).hexdigest()
            offset = db.get_segment_offset(c_hash)
            
            if offset is None:
                # Новый уникальный сегмент
                offset = storage.write_segment(chunk_data)
                db.save_unique_segment(c_hash, offset)
            else:
                # Дубликат сегмента
                db.increment_ref_count(c_hash)
            
            # Сохраняем структуру
            db.save_file_structure(file_id, idx, c_hash)
            idx += 1
            if idx % 1000 == 0: print(f"Обработано {idx} сегментов...")

    print(f"Готово! Время: {time.time() - start_time:.2f} сек. Сегментов: {idx}")
    return file_name

def restore_logic(file_name, db, storage):
    print(f"\nВосстановление файла: {file_name}")
    recipe = db.get_file_recipe(file_name)
    if not recipe:
        print("Ошибка: файл не найден в БД!")
        return

    out_path = f"restored_data/RESTORED_{file_name}"
    os.makedirs("restored_data", exist_ok=True)
    
    with open(out_path, "wb") as f:
        for offset, length in recipe:
            f.write(storage.read_segment(offset, length))
    
    print(f"Файл успешно восстановлен в: {out_path}")

if __name__ == "__main__":
    db_config = {
        "host": os.getenv("HOST"),
        "database": os.getenv("DATABASE"),
        "user": os.getenv("USER"),
        "password": os.getenv("PASSWORD")
    }
    
    db = DBManager(db_config)
    storage = StorageManager()
    
    selected = select_file()
    if selected:
        fname = process_file_logic(selected, db, storage)
        restore_logic(fname, db, storage)
    
    db.close()