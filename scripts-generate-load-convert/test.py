import importlib
try:
    fio = importlib.import_module("google.cloud.storage.fileio")
    print("module:", fio)
    print("has TextIOWrapper:", hasattr(fio, "TextIOWrapper"))
    # if present, show its repr
    if hasattr(fio, "TextIOWrapper"):
        print(fio.TextIOWrapper)
except Exception as e:
    print("import error:", e)