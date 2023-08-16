import os, sys, shutil
import sqlite3
import oracledb
import pathlib
import re
import requests as r
import configparser
import argparse
import csv
import xml.etree.ElementTree as ET
import multiprocessing as mp
from zipfile import ZipFile, ZIP_DEFLATED
from base64 import b64decode
from datetime import datetime
from tqdm import tqdm
from string import Template
import pysftp

# archivo de configuracion
config = configparser.ConfigParser()
config.read("config.ini")

# APP_SFTP
DEBUG = DEBUG = config["APP_SFTP"].getboolean("DEBUG")
AMBIENTE = config["APP_SFTP"]["AMBIENTE"]
FILE_LOG = config["APP_SFTP"]["FILE_LOG"]
MAX_ERRORS = int(config["APP_SFTP"]["MAX_ERRORS"])
SQLITE_DB = config["APP_SFTP"]["SQLITE_DB"]

# SFTP
SFTP_HOST = config["SFTP"]["SFTP_HOST"]
SFTP_USER = config["SFTP"]["SFTP_USER"]
SFTP_PASS = config["SFTP"]["SFTP_PASS"]
SFTP_DIR = config["SFTP"]["SFTP_DIR"]

# AMBIENTE
if AMBIENTE == "PROD":
    URL_TOKEN = config["PROD"]["URL_TOKEN"]
    USUARIO_TOKEN = config["PROD"]["USUARIO_TOKEN"]
    PASS_TOKEN = config["PROD"]["PASS_TOKEN"]
    URL_CONSULTA_EXPEDIENTE = config["PROD"]["URL_CONSULTA_EXPEDIENTE"]
    URL_DESCARGA_DOCUMENTO = config["PROD"]["URL_DESCARGA_DOCUMENTO"]
    ORACLE_HOST = config["PROD"]["ORACLE_HOST"]
    ORACLE_PORT = int(config["PROD"]["ORACLE_PORT"])
    ORACLE_SERVICE = config["PROD"]["ORACLE_SERVICE"]
    ORACLE_USER = config["PROD"]["ORACLE_USER"]
    ORACLE_PASS = config["PROD"]["ORACLE_PASS"]

if AMBIENTE == "PRE3":
    URL_TOKEN = config["PRE3"]["URL_TOKEN"]
    USUARIO_TOKEN = config["PRE3"]["USUARIO_TOKEN"]
    PASS_TOKEN = config["PRE3"]["PASS_TOKEN"]
    URL_CONSULTA_EXPEDIENTE = config["PRE3"]["URL_CONSULTA_EXPEDIENTE"]
    URL_DESCARGA_DOCUMENTO = config["PRE3"]["URL_DESCARGA_DOCUMENTO"]
    ORACLE_HOST = config["PRE3"]["ORACLE_HOST"]
    ORACLE_PORT = int(config["PRE3"]["ORACLE_PORT"])
    ORACLE_SERVICE = config["PRE3"]["ORACLE_SERVICE"]
    ORACLE_USER = config["PRE3"]["ORACLE_USER"]
    ORACLE_PASS = config["PRE3"]["ORACLE_PASS"]


# Templates
request_expediente = Template(
    """<Envelope xmlns="http://schemas.xmlsoap.org/soap/envelope/">
            <Body>
                <consultarExpedienteDetallado xmlns="https://iop.gba.gob.ar/ws_gdeba_consultaExpediente">
                    <numeroExpediente xmlns="">${numeroExpediente}</numeroExpediente>
                </consultarExpedienteDetallado>
            </Body>
        </Envelope>"""
)

request_documento = Template(
    """<Envelope xmlns="http://schemas.xmlsoap.org/soap/envelope/">
            <Body>
                <buscarPDFPorNumero xmlns="https://iop.gba.gob.ar/gdeba_consultaDocumento">
                    <request xmlns="">
                        <assignee>false</assignee>
                        <numeroDocumento>${numeroDocumento}</numeroDocumento>
                        <usuarioConsulta>${usuarioConsulta}</usuarioConsulta>
                    </request>
                </buscarPDFPorNumero>
            </Body>
        </Envelope>"""
)


# Funcion para validar numero de expediente
def es_valido(nro_expediente: str) -> bool:
    valid = re.compile(r"^EX-[0-9]{4}-[0-9]{1,8}- -GDEBA-[A-Z0-9#]+$")
    if not valid.match(nro_expediente):
        return False
    return True


# Argumentos
def get_arguments():
    parser = argparse.ArgumentParser(
        prog="SFTP_APP",
        description="Aplicación para descarga y envío de expedientes por SFTP",
    )
    parser.add_argument(
        "--iniciar",
        action="store_true",
        help="Inicia el proceso de descarga y envío de archivos por SFTP",
    )
    parser.add_argument(
        "--cargar",
        action="store",
        help="Nombre del archivo CSV (--cargar 'expedientes.csv')",
    )
    parser.add_argument(
        "--descargar",
        action="store",
        help="descarga manual de expediente comprimido (--decargar 'EX-2023-4616333- -GDEBA-TESTGDEBA')",
    )
    parser.add_argument(
        "--enviar",
        action="store",
        help="Envio manual de expediente comprimido (--enviar 'EX-2023-4616333- -GDEBA-TESTGDEBA.zip')",
    )
    parser.add_argument(
        "--buscar",
        action="store",
        help="Busca si existe un expediente comprimido en el destino (--buscar 'EX-2023-4616333- -GDEBA-TESTGDEBA.zip')",
    )
    parser.add_argument(
        "--pendientes",
        action="store_true",
        help="Retorna la cantidad de expedientes que faltan procesar",
    )
    parser.add_argument(
        "--verificar_servidor",
        action="store_true",
        help="Retorna el estado de conexion con el servidor remoto",
    )
    return parser.parse_args()


class App:
    """Clase principal"""

    def __init__(self) -> None:
        self._errores = 0

        # Argumentos
        args = get_arguments()

        if args.iniciar:
            try:
                self._iniciar()
            except Exception as e:
                print(e)
            finally:
                sys.exit()

        if args.cargar:
            archivo = args.cargar
            resp = input(
                "Esta operación borrará toda la tabla para insertar los nuevos expedientes.\nDesea continuar? (S/N): "
            )
            if resp.upper() != "S":
                print("Operación cancelada.")
                sys.exit()

            if not os.path.exists(archivo):
                print(f"No se encuentra el archivo {archivo}.")
                sys.exit()

            try:
                self._cargar(archivo)
                print("Expedientes cargados correctamente.")
            except:
                print("Error al cargar expedientes.")
            finally:
                sys.exit()

        if args.descargar:
            expediente = args.descargar
            try:
                self._descargar_expediente(expediente)
                print("Expediente descargado correctamente.")
            except Exception as e:
                print(f"Error al descargar expediente.\n{e}")
            finally:
                sys.exit()

        if args.enviar:
            expediente = args.enviar
            try:
                self._enviar_expediente(expediente)
                print("Expediente enviado correctamente.")
            except:
                print("Error al enviar expediente.")
            finally:
                sys.exit()

        if args.buscar:
            expediente = args.buscar
            try:
                existe = self._buscar_expediente(expediente)
                if not existe:
                    print("Expediente no encontrado en el servidor remoto.")
                print("El expediente se encuentra en el servidor remoto.")
            except Exception as e:
                print(e)
            finally:
                sys.exit()

        if args.pendientes:
            try:
                cant = self._consultar_pendientes()
                print(f"Faltan procesar {cant} expediente/s.")
            except:
                print("Error al consultar expedientes pendientes.")
            finally:
                sys.exit()

        if args.verificar_servidor:
            if not self._servidor_disponible():
                print("No se pudo establecer la conexión con el servidor remoto.")
            print("El servidor remoto se encuentra disponible.")

    def _iniciar(self) -> None:
        """Inicia el proceso de descarga y envio de expedientes"""

        # Valido que el servidor remoto se encuentre disponible
        if not self._servidor_disponible():
            raise Exception("No se pudo establecer la conexión con el servidor remoto.")

        # obtengo listado de expedientes a generar
        try:
            lista_expedientes = self._get_expedientes()
        except Exception as e:
            self._escribir_log(e)
            raise e

        cant = len(lista_expedientes)

        # si no hay expedientes termino la ejecución
        if cant == 0:
            self._escribir_log(
                "############ No hay expedientes para procesar ############"
            )
            sys.exit()

        # si no existe la carpeta descarga la crea
        if not os.path.exists(f"./descarga"):
            os.mkdir(f"./descarga")

        # creo una conexion a la base de datos Oracle
        try:
            conn_pool = oracledb.ConnectionPool(
                min=10,
                max=10,
                getmode=oracledb.POOL_GETMODE_WAIT,
                user=ORACLE_USER,
                password=ORACLE_PASS,
                host=ORACLE_HOST,
                port=ORACLE_PORT,
                service_name=ORACLE_SERVICE,
            )
            oracle_connection = oracledb.Connection(pool=conn_pool)
        except:
            self._escribir_log(
                "Error al intentar conectarse a la base de datos de GDEBA."
            )
            raise Exception("Error al intentar conectarse a la base de datos de GDEBA.")

        self._escribir_log(
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
                self._descargar_expediente(nro_expediente, oracle_connection)
            except Exception as e:
                self._escribir_log(e)
                continue

            # envio por SFTP
            if not DEBUG:
                try:
                    self._enviar_expediente(nro_expediente)
                except Exception as e:
                    self._escribir_log(f"{e} {nro_expediente}")
                    continue

            # eliminar archivo comprimido
            try:
                os.remove(f"./descarga/{nro_expediente}.zip")
            except:
                self._escribir_log(
                    f"Error al eliminar archivo comprimido {nro_expediente}"
                )
                continue

            # actualizo estado en la base de datos
            try:
                self._actualizar_estado(nro_expediente)
            except Exception as e:
                self._escribir_log(f"{e} {nro_expediente}")
                continue

            self._escribir_log(f"Expediente enviado correctamente {nro_expediente}")

    def _cargar(self, archivo: str) -> None:
        """Carga el archivo CSV en la base de datos local

        Args:
            archivo (str): archivo CSV
        """
        delete_str = "DELETE FROM Expedientes;"
        create_str = "CREATE TABLE Expedientes (numero TEXT, estado INT);"
        insert_str = "INSERT INTO Expedientes VALUES (?,0);"
        with sqlite3.connect(SQLITE_DB) as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(delete_str)
            except:
                cursor.execute(create_str)
            with open(archivo) as file:
                contenido = csv.reader(file)
                cursor.executemany(insert_str, contenido)
            connection.commit()
            cursor.close()

    def _descargar_expediente(
        self, nro_expediente: str, oracle_connection: oracledb.Connection | None = None
    ) -> None:
        """Descarga un expediente y lo comprime en la carpeta descarga

        Args:
            expediente (str): numero de expediente GDEBA
        """

        if not es_valido(nro_expediente):
            raise Exception("El numero de expediente no es válido.")

        # si no existe la carpeta descarga la crea
        if not os.path.exists(f"./descarga"):
            os.mkdir(f"./descarga")

        # creo una conexion a la base de datos Oracle
        if not oracle_connection:
            try:
                conn_pool = oracledb.ConnectionPool(
                    min=10,
                    max=10,
                    getmode=oracledb.POOL_GETMODE_WAIT,
                    user=ORACLE_USER,
                    password=ORACLE_PASS,
                    host=ORACLE_HOST,
                    port=ORACLE_PORT,
                    service_name=ORACLE_SERVICE,
                )
                oracle_connection = oracledb.Connection(pool=conn_pool)
            except:
                raise Exception(
                    "Error al intentar conectarse a la base de datos de GDEBA."
                )

        # crea la carpeta para el expediente si no existe
        if not os.path.exists(f"./descarga/{nro_expediente}"):
            os.mkdir(f"./descarga/{nro_expediente}")

        try:
            token = self._get_token(USUARIO_TOKEN, PASS_TOKEN)
        except r.exceptions.HTTPError as e:
            raise e

        # descargo la lista de documentos oficiales del expediente
        try:
            lista_documentos = self._get_documentos_oficiales(token, nro_expediente)
        except r.exceptions.HTTPError as e:
            raise e

        # creo un proceso para cada documento
        with mp.Pool() as pool:
            procesos = []
            for nro_documento in lista_documentos:
                if not os.path.exists(
                    f"./descarga/{nro_expediente}/{nro_documento}.pdf"
                ):
                    try:
                        token = self._get_token(USUARIO_TOKEN, PASS_TOKEN)
                    except r.exceptions.HTTPError as e:
                        raise e

                    try:
                        usuario_firmante = self._get_usuario_firmante(
                            oracle_connection, nro_documento
                        )
                    except Exception as e:
                        self._escribir_log(f"{e} {nro_documento}")
                        continue

                    path_documento = f"./descarga/{nro_expediente}/{nro_documento}.pdf"
                    procesos.append(
                        (token, nro_documento, usuario_firmante, path_documento)
                    )

            for resultado in pool.starmap(self._get_documento, procesos):
                if resultado == 1:
                    self._errores += 1
                    if self._errores > MAX_ERRORS:
                        error_str = "\nDemasiados errores 500 al intentar descargar los documentos.\nCompruebe que el usuario GDEBA sea correcto."
                        self._escribir_log(error_str)
                        raise Exception(error_str)

        # si se descargaron todos los documentos del expediente
        if sum(
            1
            for element in os.scandir(f"./descarga/{nro_expediente}")
            if element.is_file()
        ) == len(lista_documentos):
            # comprimo expediente
            try:
                self._comprimir(f"./descarga/{nro_expediente}")
            except Exception as e:
                self._escribir_log(f"{e} {nro_expediente}")
                raise e

            # elimino carpeta descomprimida
            try:
                shutil.rmtree(f"./descarga/{nro_expediente}")
            except:
                raise Exception("Error al eliminar carpeta descomprimida.")

    def _buscar_expediente(self, nro_expediente: str) -> bool:
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

    def _consultar_pendientes(self) -> int:
        """Retorna la cantidad de expedientes que faltan procesar

        Returns:
            int: cantidad de expedientes que faltan procesar
        """
        create_str = "CREATE TABLE Expedientes (numero TEXT, estado INT);"
        select_str = "SELECT count(*) FROM Expedientes WHERE estado = 0;"
        with sqlite3.connect(SQLITE_DB) as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(select_str)
                cant = cursor.fetchone()[0]
            except:
                cursor.execute(create_str)
                cant = 0
            finally:
                cursor.close()
                return cant

    def _escribir_log(self, msg: str) -> None:
        """Escribe el archivo de log

        Args:
            msg (str): texto que se desea loguear
        """
        with open(FILE_LOG, "a") as file:
            file.write(f"{datetime.now()} - {msg}\n")

    def _get_token(self, usr: str, pasw: str) -> str:
        """Retorna un token para consumir servicios del ESB

        Args:
            usr (str): usuario
            pasw (str): contraseña

        Returns:
            str: token
        """
        response = r.post(URL_TOKEN, auth=(usr, pasw))
        response.raise_for_status()
        if (
            response.content.decode("utf-8")
            == "{No se pudo obtener el nombre de usuario}"
        ):
            raise r.exceptions.HTTPError(
                "401 Client Error: Unauthorized for url: https://iop.gba.gob.ar/servicios/JWT/1/REST/jwt"
            )
        return response.content.decode("utf-8")

    def _get_documentos_oficiales(self, token: str, nro_exp: str) -> list:
        """Retorna listado de documentos oficiales de un expediente

        Args:
            token (str): token para poder consumir el servicio
            nro_exp (str): numero de expediente gdeba

        Returns:
            list: lista de documentos oficiales
        """
        HEADER = {
            "Content-Type": "text/xml; charset=utf-8",
            "Authorization": f"Bearer {token}",
        }

        request = request_expediente.substitute(numeroExpediente=nro_exp)
        response = r.post(URL_CONSULTA_EXPEDIENTE, headers=HEADER, data=request)
        response.raise_for_status()
        root = ET.fromstring(response.content)
        return list(map(lambda el: el.text, root.findall(".//documentosOficiales")))

    def _get_documento(
        self, token: str, nro_doc: str, usuario_gdeba: str, path: str
    ) -> int:
        """Descarga el documento en formato PDF

        Args:
            token (str): token para poder consumir el servicio
            nro_doc (str): numero de documento GDEBA
            usuario_gdeba (str): usuario GDEBA
            path (str): ubicación donde se generará el documento
        """

        HEADER = {
            "Content-Type": "text/xml; charset=utf-8",
            "Authorization": f"Bearer {token}",
        }

        request = request_documento.substitute(
            numeroDocumento=nro_doc,
            usuarioConsulta=usuario_gdeba,
        )
        try:
            response = r.post(URL_DESCARGA_DOCUMENTO, headers=HEADER, data=request)
            response.raise_for_status()
        except r.exceptions.HTTPError:
            return 1

        root = ET.fromstring(response.content)
        base64 = root.find(".//response").text
        bytes = b64decode(base64, validate=True)

        with open(path, "wb") as file:
            file.write(bytes)
        return 0

    def _comprimir(self, path: str) -> None:
        """Comprime un expediente en formato ZIP

        Args:
            path (str): Path del expediente
        """
        try:
            with ZipFile(f"{path}.zip", "w", ZIP_DEFLATED, compresslevel=9) as archive:
                directory = pathlib.Path(path)
                for file_path in directory.rglob("*"):
                    archive.write(file_path, arcname=file_path.relative_to(directory))
        except:
            raise Exception("Error al comprimir expediente.")

    def _get_expedientes(self) -> list:
        """retorna el listado de expedientes que se van a generar

        Returns:
            list: lista de expedientes a generar
        """
        try:
            with sqlite3.connect(SQLITE_DB) as connection:
                cursor = connection.cursor()
                query = "SELECT numero FROM Expedientes WHERE estado = 0;"
                result = cursor.execute(query).fetchall()
                return [row[0] for row in result]
        except:
            raise Exception("Error al obtener los expedientes de la base de datos.")

    def _servidor_disponible(self) -> bool:
        """Valida que el servidor remoto se encuentre disponible

        Returns:
            bool: True si se encuentra disponible
        """
        try:
            with pysftp.Connection(SFTP_HOST, username=SFTP_USER, password=SFTP_PASS):
                return True
        except:
            return False

    def _enviar_expediente(self, nro_expediente: str) -> None:
        """Envia expediente comprimido en zip por sftp

        Args:
            nro_expediente (str): nro de expediente GDEBA
        """
        try:
            with pysftp.Connection(
                SFTP_HOST, username=SFTP_USER, password=SFTP_PASS
            ) as sftp:
                with sftp.cd(SFTP_DIR):
                    sftp.put(f"./descarga/{nro_expediente}.zip")
        except:
            raise Exception("Error al enviar por SFTP.")

    def _actualizar_estado(self, nro_expediente: str) -> None:
        """Actualiza estado del expediente en la base de datos

        Args:
            nro_expediente (str): numero GDEBA del expediente
        """
        try:
            with sqlite3.connect(SQLITE_DB) as connection:
                cursor = connection.cursor()
                cursor.execute(
                    "UPDATE Expedientes SET estado = 1 WHERE numero = ?;",
                    (nro_expediente,),
                )
                connection.commit()
        except:
            raise Exception("Error al actualizar el estado del expediente.")

    def _get_usuario_firmante(
        self, conn: oracledb.Connection, nro_documento: str
    ) -> str:
        """Retorna el usuario firmante de un documento de la base de datos de GDEBA

        Args:
            conn (OracleConnection): conexion a la BBDD Oracle
            nro_documento (str): numero GDEBA del documento

        Returns:
            str: usuario firmante
        """
        try:
            with conn.cursor() as cursor:
                sql = """SELECT USUARIOGENERADOR 
                FROM GEDO_GED.GEDO_DOCUMENTO 
                WHERE NUMERO = :num"""

                cursor.execute(sql, num=nro_documento)
                row = cursor.fetchone()
                return row[0]
        except:
            raise Exception("Error al obtener usuario firmante.")


if __name__ == "__main__":
    app = App()
