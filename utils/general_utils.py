import os
import shutil

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