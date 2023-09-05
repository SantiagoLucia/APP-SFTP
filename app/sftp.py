import pysftp
from config import SFTP_HOST, SFTP_USER, SFTP_PASS, SFTP_DIR
from pathlib import Path

path_descargas = Path(__file__).parent.parent / "data" / "descargas"


def servidor_disponible() -> bool:
    """Valida que el servidor remoto se encuentre disponible
    Returns:
        bool: True si se encuentra disponible
    """
    try:
        with pysftp.Connection(SFTP_HOST, username=SFTP_USER, password=SFTP_PASS):
            return True
    except:
        return False


def enviar_expediente(nro_expediente: str) -> None:
    """Envia expediente comprimido en zip por sftp
    Args:
        nro_expediente (str): nro de expediente GDEBA
    """
    try:
        with pysftp.Connection(
            SFTP_HOST, username=SFTP_USER, password=SFTP_PASS
        ) as sftp:
            with sftp.cd(SFTP_DIR):
                sftp.put(f"{path_descargas}/{nro_expediente}.zip")
    except:
        raise Exception("Error al enviar por SFTP.")


def buscar_expediente(nro_expediente: str) -> bool:
    """Busca un expediente en el servidor remoto.
    Args:
        nro_expediente (str): Nro de expediente GDEBA
    Returns:
        bool: True si encuentra el expediente
    """
    try:
        with pysftp.Connection(
            SFTP_HOST, username=SFTP_USER, password=SFTP_PASS
        ) as sftp:
            with sftp.cd(SFTP_DIR):
                sftp.exists(f"{SFTP_DIR}/{nro_expediente}.zip")
            return True
    except:
        raise Exception("Error al buscar expediente en el servidor remoto.")
