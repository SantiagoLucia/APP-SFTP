import requests
import zeep
from config import (
    URL_TOKEN,
    USUARIO_TOKEN,
    PASS_TOKEN,
    URL_CONSULTA_EXPEDIENTE,
    URL_DESCARGA_DOCUMENTO,
)


def get_token() -> str:
    """Retorna un token para consumir servicios del ESB
    Returns:
        str: token
    """
    response = requests.post(URL_TOKEN, auth=(USUARIO_TOKEN, PASS_TOKEN))
    response.raise_for_status()
    if response.content.decode("utf-8") == "{No se pudo obtener el nombre de usuario}":
        raise requests.exceptions.HTTPError(
            "401 Client Error: Unauthorized for url: https://iop.gba.gob.ar/servicios/JWT/1/REST/jwt"
        )
    return response.content.decode("utf-8")


def get_documentos_oficiales(nro_exp: str) -> list:
    """Retorna listado de documentos oficiales de un expediente
    Args:
        nro_exp (str): numero de expediente gdeba
    Returns:
        list: lista de documentos oficiales
    """
    client_expediente = zeep.Client(wsdl=URL_CONSULTA_EXPEDIENTE)
    with client_expediente.settings(
        extra_http_headers={"Authorization": "Bearer " + get_token()}
    ):
        response = client_expediente.service.consultarExpedienteDetallado(
            numeroExpediente=nro_exp
        )
    return response.documentosOficiales


async def get_documento(nro_doc: str, usuario_gdeba: str, path: str) -> int:
    """Descarga el documento en formato PDF
    Args:
        nro_doc (str): numero de documento GDEBA
        usuario_gdeba (str): usuario GDEBA
        path (str): ubicación donde se generará el documento
    """
    client_documento = zeep.AsyncClient(wsdl=URL_DESCARGA_DOCUMENTO)
    with client_documento.settings(
        extra_http_headers={"Authorization": "Bearer " + get_token()}
    ):
        request = {
            "assignee": False,
            "numeroDocumento": nro_doc,
            "usuarioConsulta": usuario_gdeba,
        }
        response = await client_documento.service.buscarPDFPorNumero(request=request)

    with open(path, "wb") as file:
        file.write(response)
    return nro_doc
