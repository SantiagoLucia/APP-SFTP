import os, sys, shutil
import asyncio
import helpers
import database
import services
import sftp
from tqdm import tqdm
from config import MAX_ERRORS, DEBUG
from pathlib import Path

pool = database.crear_pool_conexiones()
path_descargas = f"{Path(__file__).parent.parent}/data/descargas"


async def descargar_expediente(nro_expediente: str) -> str:
    """Descarga un expediente y lo comprime en la carpeta descarga
    Args:
        expediente (str): numero de expediente GDEBA
    """

    if not helpers.es_valido(nro_expediente):
        raise Exception("El numero de expediente no es válido.")

    # si no existe la carpeta descarga la crea
    if not os.path.exists(path_descargas):
        os.mkdir(path_descargas)

    # crea la carpeta para el expediente si no existe
    if not os.path.exists(f"{path_descargas}/{nro_expediente}"):
        os.mkdir(f"{path_descargas}/{nro_expediente}")

    # descargo la lista de documentos oficiales del expediente
    try:
        lista_documentos = services.get_documentos_oficiales(nro_expediente)
    except Exception as e:
        raise e

    tasks = []
    async with asyncio.TaskGroup() as tg:
        for nro_documento in lista_documentos:
            path_documento = f"{path_descargas}/{nro_expediente}/{nro_documento}.pdf"
            if not os.path.exists(path_documento):
                usuario_firmante = database.get_usuario_firmante(pool, nro_documento)
                tasks.append(
                    tg.create_task(
                        services.get_documento(
                            nro_documento, usuario_firmante, path_documento
                        )
                    )
                )

    for task in tasks:
        if task.result() not in lista_documentos:
            errores += 1
            if errores > MAX_ERRORS:
                error_str = "\nDemasiados errores 500 al intentar descargar los documentos.\nCompruebe que el usuario GDEBA sea correcto."
                helpers.escribir_log(error_str)
                raise Exception(error_str)

    # si se descargaron todos los documentos del expediente
    if sum(
        1
        for element in os.scandir(f"{path_descargas}/{nro_expediente}")
        if element.is_file()
    ) == len(lista_documentos):
        # comprimo expediente
        try:
            helpers.comprimir(f"{path_descargas}/{nro_expediente}")
        except Exception as e:
            helpers.escribir_log(f"{e} {nro_expediente}")
            raise e

        # elimino carpeta descomprimida
        try:
            shutil.rmtree(f"{path_descargas}/{nro_expediente}")
        except:
            raise Exception("Error al eliminar carpeta descomprimida.")


async def iniciar() -> None:
    """Inicia el proceso de descarga y envio de expedientes"""

    # Valido que el servidor remoto se encuentre disponible
    if not sftp.servidor_disponible():
        raise Exception("No se pudo establecer la conexión con el servidor remoto.")

    # obtengo listado de expedientes a generar
    try:
        lista_expedientes = database.get_expedientes()
    except Exception as e:
        helpers.escribir_log(e)
        raise e

    cant = len(lista_expedientes)

    # si no hay expedientes termino la ejecución
    if cant == 0:
        helpers.escribir_log(
            "############ No hay expedientes para procesar ############"
        )
        sys.exit()

    # si no existe la carpeta descarga la crea
    if not os.path.exists(path_descargas):
        os.mkdir(path_descargas)

    helpers.escribir_log(
        f"############ COMIENZA PROCESO - {cant} EXPEDIENTES ############"
    )

    # recorro listado de expedientes
    for nro_expediente in (
        pbar := tqdm(
            lista_expedientes,
            total=cant,
            colour="GREEN",
            desc="Procesando",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
        )
    ):
        # descargo expediente
        try:
            await descargar_expediente(nro_expediente)
        except Exception as e:
            helpers.escribir_log(e)
            continue

        # envio por SFTP
        if not DEBUG:
            try:
                sftp.enviar_expediente(nro_expediente)
            except Exception as e:
                helpers.escribir_log(f"{e} {nro_expediente}")
                continue

        # eliminar archivo comprimido
        try:
            os.remove(f"{path_descargas}/{nro_expediente}.zip")
        except:
            helpers.escribir_log(
                f"Error al eliminar archivo comprimido {nro_expediente}"
            )
            continue

        # actualizo estado en la base de datos
        try:
            database.actualizar_estado(nro_expediente)
        except Exception as e:
            helpers.escribir_log(f"{e} {nro_expediente}")
            continue

        helpers.escribir_log(f"Expediente enviado correctamente {nro_expediente}")
