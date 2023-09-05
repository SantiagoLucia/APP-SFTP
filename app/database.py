import sqlite3
import oracledb
import csv
from config import (
    SQLITE_DB,
    ORACLE_HOST,
    ORACLE_PASS,
    ORACLE_PORT,
    ORACLE_SERVICE,
    ORACLE_USER,
)
from pathlib import Path

path_db = Path(__file__).parent.parent / "data" / SQLITE_DB


def cargar_csv(archivo: str) -> None:
    """Carga el archivo CSV en la base de datos local

    Args:
        archivo (str): archivo CSV
    """
    delete_str = "DELETE FROM Expedientes;"
    create_str = "CREATE TABLE Expedientes (numero TEXT, estado INT);"
    insert_str = "INSERT INTO Expedientes VALUES (?,0);"
    with sqlite3.connect(path_db) as connection:
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


def get_expedientes() -> list:
    """retorna el listado de expedientes que se van a generar
    Returns:
        list: lista de expedientes a generar
    """
    try:
        with sqlite3.connect(path_db) as connection:
            cursor = connection.cursor()
            query = "SELECT numero FROM Expedientes WHERE estado = 0;"
            result = cursor.execute(query).fetchall()
            return [row[0] for row in result]
    except:
        raise Exception("Error al obtener los expedientes de la base de datos.")


def consultar_pendientes() -> int:
    """Retorna la cantidad de expedientes que faltan procesar
    Returns:
        int: cantidad de expedientes que faltan procesar
    """
    create_str = "CREATE TABLE Expedientes (numero TEXT, estado INT);"
    select_str = "SELECT count(*) FROM Expedientes WHERE estado = 0;"
    with sqlite3.connect(path_db) as connection:
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


def actualizar_estado(nro_expediente: str) -> None:
    """Actualiza estado del expediente en la base de datos
    Args:
        nro_expediente (str): numero GDEBA del expediente
    """
    try:
        with sqlite3.connect(path_db) as connection:
            cursor = connection.cursor()
            cursor.execute(
                "UPDATE Expedientes SET estado = 1 WHERE numero = ?;",
                (nro_expediente,),
            )
            connection.commit()
    except:
        raise Exception("Error al actualizar el estado del expediente.")


def crear_pool_conexiones():
    return oracledb.ConnectionPool(
        min=10,
        max=10,
        getmode=oracledb.POOL_GETMODE_WAIT,
        user=ORACLE_USER,
        password=ORACLE_PASS,
        host=ORACLE_HOST,
        port=ORACLE_PORT,
        service_name=ORACLE_SERVICE,
    )


def get_usuario_firmante(conn_pool: oracledb.ConnectionPool, nro_documento: str) -> str:
    """Retorna el usuario firmante de un documento de la base de datos de GDEBA
    Args:
        conn (OracleConnection): conexion a la BBDD Oracle
        nro_documento (str): numero GDEBA del documento

    Returns:
        str: usuario firmante
    """
    try:
        conn = oracledb.Connection(pool=conn_pool)
        with conn.cursor() as cursor:
            sql = """SELECT USUARIOGENERADOR 
            FROM GEDO_GED.GEDO_DOCUMENTO 
            WHERE NUMERO = :num"""
            cursor.execute(sql, num=nro_documento)
            row = cursor.fetchone()
            return row[0]
    except:
        raise Exception("Error al obtener usuario firmante.")
