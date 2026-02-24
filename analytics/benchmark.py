"""
Бенчмарк: прогон всех файлов по всем парам (chunk_size x algo).

Оптимизация: файл читается ОДИН раз на chunk_size.
Все алгоритмы обрабатываются за один проход по сегментам.
"""

import os
import hashlib
import time
import csv
from app.config import get_postgres_config, CHUNK_SIZES, HASH_ALGORITHMS, FILE_READ_SIZE
from app.db_manager import DBManager
from app.storage_manager import StorageManager

ORIGIN_DIR = "./origin_data"
RESULTS_FILE = "analytics/benchmark_results.csv"


def get_full_file_hash(filepath: str) -> str:
    hasher = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(FILE_READ_SIZE):
            hasher.update(chunk)
    return hasher.hexdigest()


def process_file_all_algos(filepath: str, chunk_size: int, algos: list[str],
                           db: DBManager, storage: StorageManager) -> list[dict]:
    """
    Один проход по файлу — все алгоритмы сразу.

    Для каждого сегмента:
      1. Читаем данные один раз
      2. Считаем content_hash (sha256) - для storage_index
      3. Считаем хэши всех алгоритмов - для unique_segments
      4. Пишем в хранилище один раз (через storage_index)
      5. Заполняем таблицы каждого алгоритма
    """
    file_name = os.path.basename(filepath)
    file_size = os.path.getsize(filepath)
    file_hash = get_full_file_hash(filepath)

    # Фильтруем: какие алгоритмы ещё не обработаны
    algos_todo = [a for a in algos if not db.file_has_processing(file_hash, chunk_size, a)]
    if not algos_todo:
        return []

    file_id = db.register_file(file_name, file_hash, file_size)

    # Метрики для каждого алгоритма
    metrics = {}
    for algo in algos_todo:
        metrics[algo] = {
            "unique": 0,
            "duplicate": 0,
            "time_hashing": 0.0,
        }

    storage_writes = 0
    start_total = time.time()

    with open(filepath, "rb") as f:
        idx = 0
        while True:
            data = f.read(chunk_size)
            if not data:
                break

            # 1. Content hash — один раз, для хранилища
            content_hash = hashlib.sha256(data).hexdigest()

            # 2. Проверяем/пишем в хранилище — один раз
            stored = db.get_storage_offset(chunk_size, content_hash)
            if stored is not None:
                offset, seg_size = stored
            else:
                offset = storage.write_segment(chunk_size, data)
                db.save_storage_index(chunk_size, content_hash, offset, len(data))
                storage_writes += 1

            # 3. Для каждого алгоритма — хэш + таблицы
            for algo in algos_todo:
                t0 = time.time()
                if algo == "sha256":
                    algo_hash = content_hash  # уже посчитан
                else:
                    algo_hash = hashlib.new(algo, data).hexdigest()
                metrics[algo]["time_hashing"] += time.time() - t0

                existing = db.get_segment_offset(chunk_size, algo, algo_hash)
                if existing is None:
                    db.save_segment(chunk_size, algo, algo_hash, offset, len(data))
                    metrics[algo]["unique"] += 1
                else:
                    db.increment_ref_count(chunk_size, algo, algo_hash)
                    metrics[algo]["duplicate"] += 1

                db.save_file_structure(chunk_size, algo, file_id, idx, algo_hash)

            idx += 1
            if idx % 1000 == 0:
                print(f"    {idx} сегментов...")

    elapsed_total = time.time() - start_total

    # Отмечаем обработку
    for algo in algos_todo:
        db.mark_processing_done(file_hash, chunk_size, algo)

    # Формируем результаты
    results = []
    for algo in algos_todo:
        results.append({
            "file_name": file_name,
            "file_size": file_size,
            "chunk_size": chunk_size,
            "algo": algo,
            "total_segments": idx,
            "unique_segments": metrics[algo]["unique"],
            "duplicate_segments": metrics[algo]["duplicate"],
            "storage_writes": storage_writes,
            "time_hashing": round(metrics[algo]["time_hashing"], 6),
            "time_total": round(elapsed_total, 4),
            "storage_size": storage.storage_size(chunk_size),
        })

    return results


def run_benchmark():
    db = DBManager(get_postgres_config())
    storage = StorageManager()

    if not os.path.exists(ORIGIN_DIR):
        os.makedirs(ORIGIN_DIR)
    files = [
        os.path.join(ORIGIN_DIR, f)
        for f in sorted(os.listdir(ORIGIN_DIR))
        if os.path.isfile(os.path.join(ORIGIN_DIR, f)) and not f.startswith(".")
    ]

    if not files:
        print(f"Папка {ORIGIN_DIR} пуста!")
        return

    total_passes = len(files) * len(CHUNK_SIZES)
    print(f"Файлов: {len(files)}")
    print(f"Размеров: {len(CHUNK_SIZES)}, алгоритмов: {len(HASH_ALGORITHMS)}")
    print(f"Проходов по файлам: {total_passes} (вместо {total_passes * len(HASH_ALGORITHMS)})")
    print("=" * 60)

    all_results = []

    for filepath in files:
        fname = os.path.basename(filepath)
        for chunk_size in CHUNK_SIZES:
            print(f"  {fname} | {chunk_size} | все алгоритмы ... ", end="", flush=True)

            results = process_file_all_algos(filepath, chunk_size, HASH_ALGORITHMS, db, storage)

            if not results:
                print("пропуск")
                continue

            all_results.extend(results)
            r = results[0]
            print(f"{r['time_total']}с, сегментов: {r['total_segments']}, "
                  f"записей в storage: {r['storage_writes']}")

            for r in results:
                print(f"      {r['algo']}: уник={r['unique_segments']}, "
                      f"дубл={r['duplicate_segments']}, "
                      f"хэширование={r['time_hashing']}с")

    if all_results:
        with open(RESULTS_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_results[0].keys())
            writer.writeheader()
            writer.writerows(all_results)
        print(f"\nCSV: {RESULTS_FILE}")

    print("\nРазмеры хранилищ:")
    for chunk_size in CHUNK_SIZES:
        size = storage.storage_size(chunk_size)
        print(f"  storage_{chunk_size}.bin: {size:,} байт")

    db.close()


if __name__ == "__main__":
    run_benchmark()