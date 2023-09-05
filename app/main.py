from options import options
import core, database, sftp
import sys, os
import asyncio


async def main():
    if options.iniciar:
        try:
            await core.iniciar()
        except Exception as e:
            print(e)
        finally:
            sys.exit()

    if options.cargar:
        archivo = options.cargar
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
            database.cargar_csv(archivo)
            print("Expedientes cargados correctamente.")
        except:
            print("Error al cargar expedientes.")
        finally:
            sys.exit()

    if options.descargar:
        expediente = options.descargar
        try:
            await core.descargar_expediente(expediente)
            print("Expediente descargado correctamente.")
        except Exception as e:
            print(f"Error al descargar expediente.\n{e}")
        finally:
            sys.exit()

    if options.enviar:
        expediente = options.enviar
        try:
            sftp.enviar_expediente(expediente)
            print("Expediente enviado correctamente.")
        except:
            print("Error al enviar expediente.")
        finally:
            sys.exit()

    if options.buscar:
        expediente = options.buscar
        try:
            existe = sftp.buscar_expediente(expediente)
            if not existe:
                print("Expediente no encontrado en el servidor remoto.")
            print("El expediente se encuentra en el servidor remoto.")
        except Exception as e:
            print(e)
        finally:
            sys.exit()

    if options.pendientes:
        try:
            cant = database.consultar_pendientes()
            print(f"Faltan procesar {cant} expediente/s.")
        except:
            print("Error al consultar expedientes pendientes.")
        finally:
            sys.exit()

    if options.verificar_servidor:
        if not sftp.servidor_disponible():
            print("No se pudo establecer la conexión con el servidor remoto.")
        print("El servidor remoto se encuentra disponible.")


if __name__ == "__main__":
    asyncio.run(main())
