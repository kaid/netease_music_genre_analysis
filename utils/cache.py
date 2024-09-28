import pickle
from os import path
from typing import Any
class Cache(dict):
    def __init__(self, file_path: str):
        self.file_path = file_path

        if not path.exists(file_path):
            self.store = {}
        else:
            self.store = pickle.load(open(file_path, 'rb'))

    def save(self):
        with open(self.file_path, 'wb') as f:
            pickle.dump(self.store, f)

    def __getitem__(self, key: object) -> dict[str, Any]:
        return self.store.get(key)

    def __setitem__(self, key: object, value: object | None):
        self.store[key] = value
    
    def __contains__(self, key: object) -> bool:
        return key in self.store
