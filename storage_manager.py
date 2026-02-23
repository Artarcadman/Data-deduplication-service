import os

STORAGE_DIR = "data_storage"

class StorageManager:
    def __init__(self):
        os.makedirs(STORAGE_DIR, exist_ok=True)


    @staticmethod
    def _path(chunk_size: int) -> str:
        """data_storage/storage_1024.bin, data_storage/storage_4096.bin, ..."""
        return os.path.join(STORAGE_DIR, f"storage_{chunk_size}.bin")
    
    
    def write_segment(self, chunk_size, segment_data) -> int:
        """Дописать сегмент в конец и вернуть смещение (offset)"""
        path = self._path(chunk_size)
        with open(path, "ab") as f:
            f.seek(0, 2)
            offset = f.tell()
            f.write(segment_data)
        return offset

    def read_segment(self, chunk_size, offset, length) -> bytes:
        """Прочитать сегмент по адресу"""
        path=self._path(chunk_size)
        if not os.path.exists(path):
            return b""
        with open(path, "rb") as f:
            f.seek(offset)
            return f.read(length)
        
    def storage_size(self, chunk_size):
        """Размер хранилища в байтах"""
        path = self._path(chunk_size)
        if os.path.exists(path):
            return os.path.getsize(path)
        return 0