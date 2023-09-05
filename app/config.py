import configparser
from pathlib import Path

config = configparser.ConfigParser()
config.read(Path(__file__).parent.parent / "data" / "config.ini")

# APP_SFTP
DEBUG = config["APP_SFTP"].getboolean("DEBUG")
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
