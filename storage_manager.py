import os

class StorageManager:
    def __init__(self, storage_path="data_storage/storage.bin"):
        self.storage_path = storage_path
        os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)

    def write_segment(self, segment_data) -> int:
        """Дописывает кусок в конец и возвращает смещение (offset)"""
        with open(self.storage_path, "ab") as f:
            offset = f.tell()
            f.write(segment_data)
        return offset

    def read_segment(self, offset, length) -> bytes:
        """Читает кусок по адресу"""
        if not os.path.exists(self.storage_path):
            return b""
        with open(self.storage_path, "rb") as f:
            f.seek(offset)
            return f.read(length)