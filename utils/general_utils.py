import os
import shutil
import gzip
import base64
import json

class GeneralUtils():
    @staticmethod
    def remove_file(path: str):
        try:
            os.remove(path)
        except FileNotFoundError as e:
            print("File not found")
            pass

    @staticmethod
    def remove_dir(path: str):
        try:
            shutil.rmtree(path)
        except FileNotFoundError as e:
            print("File not found")
            pass

    @staticmethod
    def path_exist(path: str) -> bool:
        return os.path.exists(path)
    
    @staticmethod
    def gzip_dict(data: dict) -> str:
        parsed_str = json.dumps(data)

        gzipped = gzip.compress(parsed_str.encode('utf-8'))
        encoded = base64.b64encode(gzipped)

        return encoded.decode('utf-8')