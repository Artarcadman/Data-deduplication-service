import os
import hashlib
import time
from dotenv import load_dotenv
from db_manager import DBManager
from storage_manager import StorageManager
from config import CHUNK_SIZES, FILE_READ_SIZE, HASH_ALGORITHMS, get_postgres_config

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
            print("Введите число!")



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


def select_algo() -> str:
    """Выбор алгоритма хэширования."""
    print("\nДоступные алгоритмы хэширования:")
    for i, algo in enumerate(HASH_ALGORITHMS, 1):
        print(f"  {i}. {algo}")

    while True:
        try:
            choice = int(input(f"\nВыберите алгоритм (1-{len(HASH_ALGORITHMS)}): "))
            if 1 <= choice <= len(HASH_ALGORITHMS):
                return HASH_ALGORITHMS[choice - 1]
        except ValueError:
            print("Введите число!")
            

def get_full_file_hash(filepath):
    """Получить хэш всего файла алгоритмом SHA256"""
    hasher = hashlib.sha256()
    with open(filepath, 'rb') as f:
        while chunk := f.read(FILE_READ_SIZE): 
            hasher.update(chunk)
    return hasher.hexdigest()


def process_file(filepath, chunk_size, algo, db, storage):
    """Обработать файл: хэширование, дедупликация"""
    file_name = os.path.basename(filepath)
    file_size = os.path.getsize(filepath)
    full_hash = get_full_file_hash(filepath)
    
    # 1. Проверка на дубликат всего файла по паре chunk_size-algo
    if db.file_has_processing(full_hash, chunk_size, algo):
        print(f"Файл '{file_name}' с комбинацией '{chunk_size}_{algo}' уже был обработан ранее!")
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
            
            seg_hash = hashlib.new(algo, chunk_data).hexdigest()
            offset = db.get_segment_offset(chunk_size, algo, seg_hash)
            
            if offset is None:
                # Новый уникальный сегмент
                offset = storage.write_segment(chunk_size, chunk_data)
                db.save_segment(chunk_size, algo, seg_hash, offset, len(chunk_data))
            else:
                # Дубликат сегмента
                db.increment_ref_count(chunk_size, algo, seg_hash)
            
            # Сохраняем структуру
            db.save_file_structure(chunk_size, algo, file_id, idx, seg_hash)
            idx += 1
            if idx % 1000 == 0: 
                print(f"Обработано {idx} сегментов...")
                
    db.mark_processing_done(full_hash, chunk_size, algo)
    print(f"Готово!\nВремя обработки: {time.time() - start_time:.2f} сек.\nСегментов: {idx}")


def restore_file(file_id, file_name, chunk_size, algo, db, storage):
    """Восстановление файла из сегментов"""
    
    print(f"\nВосстановление файла: {file_name} из {chunk_size}-байтных сегментов алгоритма {algo}")
    
    recipe = db.get_file_recipe(file_id, chunk_size, algo)
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
        cur.execute("SELECT file_id, file_name, file_hash, processing_done FROM files ORDER BY file_id")
        rows = cur.fetchall()
        
    if not rows:
        print("В базе данных нет обработанных файлов...")
        return None
    
    print("\nОбработанные файлы:")
    for i, (fid, fname, fhash, done) in enumerate(rows, 1):
        print(f" {i}, {fname} (id: {fid}, обработки: {done})")
    
    while True:
        try:
            choice = int(input(f"\nВыберите файл (1-{len(rows)})"))
            if 1 <= choice <= len(rows):
                return rows[choice-1]
        except ValueError:
            print("Введите число")
            

def select_processing_from_done(processing_done: list) -> int | None:
    """
    Выбор пары 'chunk_size - algo' для восстановления файла из доступных
    Нельзя восстановить файл по паре, по которой он не был обработан
    """

    if not processing_done:
        print("Файл не был обработан")
        return None
    print("Доступные варианты: ")
    for i, key in enumerate(processing_done, 1):
        print(f" {i}. {key}")
        
    while True:
        try:
            choice = int(input(f"Выберите как начать восстановление (1-{len(processing_done)})"))
            if 1 <= choice <= len(processing_done):
                key = processing_done[choice - 1]
                parts = key.split("_", 1)
                return int(parts[0]), parts[1]
        except ValueError:
            print("Введите число")


if __name__ == "__main__":
    
    
    db = DBManager(get_postgres_config())
    storage = StorageManager()
    
    # 1. Записать или восстановить
    # 2. Выбрать файл
    # 3. Выбрать размер сегмента
    # 4. Выбрать алгоритм
     
    inp = input("Выберите действие: \n1 - Записать файл \n2 - Восстановить файл \n")

    if inp == "1":
        selected = select_file()
        if selected:
            chunk_size = select_chunk_size()
            algo = select_algo()
            process_file(selected, chunk_size, algo, db, storage)
            
    elif inp == "2":
        file_info = select_file_from_db(db)
        if file_info:
            file_id, file_name, file_hash, processing_done = file_info
            result = select_processing_from_done(processing_done)
        
            if result:
                chunk_size, algo = result
                restore_file(file_id, file_name, chunk_size, algo, db, storage)

        
    db.close()