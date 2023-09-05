import re
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
from datetime import datetime
from config import FILE_LOG

log_path = Path(__file__).parent.parent / "data" / FILE_LOG


def es_valido(nro_expediente: str) -> bool:
    valid = re.compile(r"^EX-[0-9]{4}-[0-9]{1,8}- -GDEBA-[A-Z0-9#]+$")
    if not valid.match(nro_expediente):
        return False
    return True


def escribir_log(msg: str) -> None:
    """Escribe el archivo de log
    Args:
        msg (str): texto que se desea loguear
    """
    with open(log_path, "a") as file:
        file.write(f"{datetime.now()} - {msg}\n")


def comprimir(path: str) -> None:
    """Comprime un expediente en formato ZIP
    Args:
        path (str): Path del expediente
    """
    try:
        with ZipFile(f"{path}.zip", "w", ZIP_DEFLATED, compresslevel=9) as archive:
            directory = Path(path)
            for file_path in directory.rglob("*"):
                archive.write(file_path, arcname=file_path.relative_to(directory))
    except:
        raise Exception("Error al comprimir expediente.")
