import argparse

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

options = parser.parse_args()
