from fastapi import FastAPI, UploadFile, File, Form, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any
import zipfile
import os
import json
import io # Importante para manejar archivos en memoria
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, PyMongoError
import aiofiles # Para manejar la escritura de archivos de forma asíncrona
import oracledb # Importa el driver de Oracle
import pytz

colombia_tz = pytz.timezone('America/Bogota')
# --- Cargar configuración desde config.json ---
CONFIG_FILE = "config.json"
config: Dict[str, Any] = {}

def load_config():
    """Carga la configuración desde el archivo config.json."""
    try:
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: El archivo de configuración '{CONFIG_FILE}' no se encontró. Asegúrate de que existe en el mismo directorio.")
        exit(1) # Termina la aplicación si no se puede cargar la configuración
    except json.JSONDecodeError:
        print(f"Error: El archivo de configuración '{CONFIG_FILE}' no es un JSON válido.")
        exit(1)

config = load_config()

# Inicializa la aplicación FastAPI
app = FastAPI(
    title="Validador de Zips",
    description="API para validar el nombre y contenido de archivos ZIP subidos.",
    version="1.0.0"
)

# Configura CORS para permitir solicitudes desde cualquier origen.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["POST"],
    allow_headers=["*"],
)

# --- Configuración de MongoDB ---
MONGO_URI = config['database']['mongo_uri']
MONGO_DB_NAME = config['database']['mongo_db_name']
mongo_client: MongoClient | None = None

def get_mongo_client():
    """
    Retorna una instancia del cliente de MongoDB.
    Si la conexión falla, lanza una HTTPException.
    """
    global mongo_client
    if mongo_client is None:
        try:
            mongo_client = MongoClient(MONGO_URI)
            # El comando ismaster es una forma ligera de comprobar la conexión y la autenticación
            mongo_client.admin.command('ismaster')
            # print("Conexión a MongoDB exitosa.")
        except ConnectionFailure as e:
            # Aquí se lanza la HTTPException si la conexión inicial falla
            print(f"No se pudo conectar a MongoDB: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"No se pudo conectar a la base de datos."
            )
    return mongo_client

# --- Configuración de Oracle DB ---
ORACLE_DB_CONFIG = config['database']['oracle_db']
ORACLE_USER = ORACLE_DB_CONFIG['user']
ORACLE_PASSWORD = ORACLE_DB_CONFIG['password']
ORACLE_DSN = ORACLE_DB_CONFIG['dsn']

# Variable global para asegurar que init_oracle_client se llama solo una vez
_oracle_client_initialized = False

def get_oracle_connection():
    """
    Establece y retorna una conexión a Oracle DB.
    Si la conexión falla, lanza una HTTPException.
    """
    global _oracle_client_initialized
    try:
        if not _oracle_client_initialized:
            # Es crucial que Oracle Instant Client esté instalado y configurado.
            # Su ruta debe estar en la variable de entorno PATH (Windows) o LD_LIBRARY_PATH (Linux).
            # o se especifica explícitamente:
            # oracledb.init_oracle_client(lib_dir="/ruta/donde/descomprimiste/instantclient_21_x")
            oracledb.init_oracle_client() # Inicializa el cliente Oracle en modo Thick
            _oracle_client_initialized = True
            # print("Cliente Oracle inicializado en modo Thick.")

        # Conexión sin los parámetros 'encoding' ni 'nencoding'.
        # La codificación se espera que sea manejada por la variable de entorno NLS_LANG
        connection = oracledb.connect(
            user=ORACLE_USER,
            password=ORACLE_PASSWORD,
            dsn=ORACLE_DSN
        )
        # print("Conexión a Oracle DB exitosa.")
        return connection
    except oracledb.Error as e:
        error_obj, = e.args
        print(f"Error al conectar a Oracle DB: {error_obj.message}")
        if "DPI-1047" in error_obj.message:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"No se pudo conectar a la base de datos Oracle: {error_obj.message}. "
                       f"Asegúrate de que Oracle Instant Client esté instalado y configurado en PATH/LD_LIBRARY_PATH."
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"No se pudo conectar a la base de datos Oracle: {error_obj.message}"
            )
    except Exception as e:
        print(f"Error inesperado durante la inicialización del cliente Oracle o conexión: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error inesperado al conectar a Oracle DB: {e}"
        )

# --- Directorio de subida ---
UPLOAD_FOLDER = config['directories']['upload_folder']
# Asegúrate de que la carpeta de subida exista
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def validar_nombre_zip(zip_filename: str, facturas_json: List[dict]) -> str | None:
    """
    Valida si el nombre del archivo ZIP coincide con alguna de las facturas esperadas.
    El formato esperado para el nombre del ZIP es "NIT_FACTURA.zip".

    Args:
        zip_filename (str): El nombre original del archivo ZIP.
        facturas_json (List[dict]): Una lista de diccionarios, cada uno con 'nit' y 'factura'.

    Returns:
        str | None: Un mensaje de error si la validación falla, o None si es exitosa.
    """
    # Extrae el nombre base del archivo sin la extensión (ej. "NIT_FACTURA")
    nombre_base = os.path.splitext(zip_filename)[0]
    factura_encontrada = None

    for factura in facturas_json:
        nit_esperado = str(factura["nit"])
        codigo_factura_esperado = factura["factura"]
        nombre_esperado_completo = f"{nit_esperado}_{codigo_factura_esperado}"

        # Verifica si el nombre base del ZIP coincide exactamente con el formato esperado.
        if nombre_base == nombre_esperado_completo:
            factura_encontrada = codigo_factura_esperado
            break

    if not factura_encontrada:
        return f"El nombre del archivo ZIP ({zip_filename}) no está relacionado en ninguna de las facturas esperadas o no sigue el formato 'NIT_FACTURA.zip'."

    return None

def validar_contenido_zip(zip_content_bytes: bytes, original_zip_filename: str):
    """
    Valida el contenido de un archivo ZIP en memoria.
    Asegura que contenga solo PDFs, que no estén vacíos y que el nombre del PDF coincida
    con la parte 'factura' del nombre del ZIP.

    Args:
        zip_content_bytes (bytes): El contenido binario del archivo ZIP.
        original_zip_filename (str): El nombre original del archivo ZIP.

    Returns:
        str | None: Un mensaje de error si la validación falla, o None si es exitosa.
    """
    try:
        # 1. Abre el ZIP desde los bytes en memoria usando io.BytesIO
        with zipfile.ZipFile(io.BytesIO(zip_content_bytes), 'r') as zip_ref:
            nombres_zip = zip_ref.namelist()
            factura_encontrada_en_pdf_interno = False

            if not nombres_zip:
                return "El archivo ZIP está vacío."

            # 2. Extrae la parte de la factura del nombre del ZIP original
            # Se asume que el formato del ZIP es "NIT_RECIBO_FACTURA.zip" para extraer el tercer componente.
            # Si tu formato es "NIT_FACTURA.zip", cambia el índice a [1].
            try:
                # Extrae "FACTURA" de "NIT_RECIBO_FACTURA.zip"
                expected_invoice_from_zip_name = os.path.splitext(original_zip_filename)[0].split('_')[1]
            except IndexError:
                return (f"El nombre del archivo ZIP '{original_zip_filename}' no cumple con el formato "
                        f"'NIT_RECIBO_FACTURA.zip' necesario para validar el contenido interno.")

            for nombre_archivo in nombres_zip:
                if not nombre_archivo.lower().endswith(".pdf"):
                    return (f"El archivo/carpeta '{nombre_archivo}' dentro del ZIP ({original_zip_filename.replace('temp_','')}) "
                            f"no es un archivo PDF.")
                
                info = zip_ref.getinfo(nombre_archivo) # Obtiene información del archivo dentro del zip
                if info.file_size <= 0: # Valida si el tamaño del archivo es mayor a 0
                    return (f"El archivo '{nombre_archivo}' dentro del ZIP está vacío (tamaño 0 bytes).")
                
                # Extrae el nombre base del PDF interno (ej. "FACTURA" de "FACTURA.pdf")
                pdf_base_name = os.path.splitext(nombre_archivo)[0]

                # Valida si el nombre del PDF interno coincide exactamente con la factura esperada del ZIP
                if expected_invoice_from_zip_name == pdf_base_name:
                    factura_encontrada_en_pdf_interno = True
                
                # Valida si el nombre del PDF interno NO contiene la factura esperada.
                # Esta condición podría ser redundante o generar falsos positivos si la anterior es una coincidencia exacta.
                # Considera si realmente necesitas ambas, o solo la coincidencia exacta.
                if expected_invoice_from_zip_name not in nombre_archivo:
                    return (f"El archivo '{nombre_archivo}' dentro del ZIP ({original_zip_filename.replace('temp_','')}) "
                            f"no está permitido o no contiene la factura esperada.")
                    
            if not factura_encontrada_en_pdf_interno:
                return (f"No se encontró la factura ({expected_invoice_from_zip_name}.pdf) en el archivo ZIP "
                        f"({original_zip_filename.replace('temp_','')}).")

            return None # No hay errores
    except zipfile.BadZipFile:
        return "El archivo ZIP está corrupto."
    except Exception as e:
        # Se incluye el nombre del archivo original para mejor depuración
        return f"Error inesperado al procesar el ZIP '{original_zip_filename}': {e}"


@app.post('/api/upload', summary="Subir y validar archivos ZIP", tags=["Archivos ZIP"])
async def upload_files_endpoint(
    zip_files: List[UploadFile] = File(..., description="Lista de archivos ZIP para validar."),
    json_data: str = Form(..., description="Cadena JSON con la lista de facturas esperadas.")
) -> Dict[str, Any]:
    """
    Recibe una lista de archivos ZIP y un JSON con datos de facturas esperadas.
    Valida cada archivo ZIP según las reglas de nombre y contenido.
    Si todos los archivos son válidos, los guarda y registra en MongoDB y luego notifica a Oracle DB.
    Retorna un diccionario con los resultados, el estado general y el conteo de tareas pendientes.
    Rechaza la petición si no puede conectarse a MongoDB u Oracle DB.
    """
    # --- Intentar conectar a MongoDB al inicio de la petición ---
    mongo_db = None
    invoices_collection = None
    try:
        mongo_db = get_mongo_client()[MONGO_DB_NAME]
        invoices_collection = mongo_db["invoices"]
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error inesperado al inicializar la conexión a MongoDB: {e}"
        )

    # --- Intentar conectar a Oracle DB al inicio de la petición ---
    oracle_conn = None
    try:
        oracle_conn = get_oracle_connection()
    except HTTPException as e:
        raise e
    except Exception as e:
        # Aquí se captura cualquier otro error durante la conexión a Oracle,
        # incluso los de inicialización del cliente si no se propagaron antes.
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error inesperado al inicializar la conexión a Oracle DB: {e}"
        )

    try:
        facturas_json = json.loads(json_data)

        if not isinstance(facturas_json, list):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='El JSON debe contener una lista de facturas.')
        for factura in facturas_json:
            if not all(key in factura for key in ("nit", "factura", "recibo")):
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='Cada factura en el JSON debe contener "nit", "factura" y "recibo".')
    except json.JSONDecodeError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='JSON inválido en la solicitud.')
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f'Error inesperado al procesar JSON: {e}')

    now_colombia = datetime.now(colombia_tz)
    print(f"[{now_colombia.strftime('%Y-%m-%d %H:%M:%S')}] Facturas esperadas (NIT: {facturas_json[0].get('nit') if facturas_json else 'N/A'}/{facturas_json[0].get('recibo') if facturas_json else 'N/A'}): {len(facturas_json)}. Zips recibidos: {len(zip_files)}")

    resultados = []
    errores_contador = 0
    oracle_response_code = "N/A" # Código de respuesta de Oracle
    oracle_response_message = "No se intentó la notificación a Oracle DB." # Mensaje de respuesta de Oracle

    valid_zips_to_process = []

    # --- Obtener conteo inicial de tareas pendientes en MongoDB ---
    pending_tasks_count = -1 # Valor por defecto en caso de error
    try:
        pending_tasks_count = invoices_collection.count_documents({"status": "pending"})
        # print(f"Conteo inicial de tareas pendientes en MongoDB: {pending_tasks_count}")
    except Exception as e:
        print(f"Advertencia: No se pudo obtener el conteo de tareas pendientes de MongoDB: {e}")
        # pending_tasks_count ya es -1, no es necesario reasignar

    for zip_file in zip_files:
        filename = zip_file.filename

        error_nombre = validar_nombre_zip(filename, facturas_json)
        if error_nombre:
            resultados.append({
                'nombre_zip': filename,
                'status': 'error',
                'mensaje': error_nombre
            })
            errores_contador += 1
            continue

        try:
            content = await zip_file.read() # Lee el contenido del ZIP de forma asíncrona
        except Exception as e:
            resultados.append({
                'nombre_zip': filename,
                'status': 'error',
                'mensaje': f"Error al leer el contenido del archivo ZIP: {e}"
            })
            errores_contador += 1
            continue

        # --- LLAMADA CORREGIDA a validar_contenido_zip ---
        # Ahora se le pasan tanto los bytes del contenido como el nombre original del archivo
        error_contenido = validar_contenido_zip(content, filename)
        
        if error_contenido:
            resultados.append({
                'nombre_zip': filename,
                'status': 'error',
                'mensaje': error_contenido
            })
            errores_contador += 1
            continue

        # Si todas las validaciones pasan, el archivo es válido.
        resultados.append({
            'nombre_zip': filename,
            'status': 'success',
            'mensaje': 'Archivo ZIP válido.'
        })

        # Encuentra la factura correspondiente para obtener nit y recibo
        # Asumiendo el formato "NIT_FACTURA.zip" para el nombre del archivo
        try:
            nit_from_filename = os.path.splitext(filename)[0].split('_')[0]
            factura_from_filename = os.path.splitext(filename)[0].split('_')[1] # Aquí asumo [1] para FACTURA
        except IndexError:
            # Esto puede ocurrir si el nombre del archivo no tiene el formato esperado
            resultados.append({
                'nombre_zip': filename,
                'status': 'error',
                'mensaje': "El nombre del archivo ZIP no tiene el formato 'NIT_FACTURA.zip' para extraer datos."
            })
            errores_contador += 1
            continue


        matched_invoice_data = None
        for inv in facturas_json:
            # Asegúrate de que las comparaciones de `nit` y `factura` sean de tipos consistentes.
            # `inv["nit"]` podría ser int, str(inv["nit"]) asegura que sea string.
            if str(inv["nit"]) == nit_from_filename and str(inv["factura"]) == factura_from_filename:
                matched_invoice_data = inv
                break

        if matched_invoice_data:
            valid_zips_to_process.append({
                'filename': filename,
                'content': content,
                'nit': matched_invoice_data['nit'],
                'recibo': matched_invoice_data['recibo'],
                'factura': matched_invoice_data['factura']
            })
        else:
            # Esto debería ser cubierto por validar_nombre_zip, pero es una advertencia útil.
            print(f"Advertencia: No se encontraron datos de factura correspondientes para {filename}, a pesar de la validación del nombre.")


    # --- Lógica para guardar en MongoDB y archivos si todo fue exitoso ---
    if errores_contador == 0:
        try:
            for zip_data in valid_zips_to_process:
                file_path = os.path.join(UPLOAD_FOLDER, zip_data['filename'])

                # Guardar el archivo ZIP físicamente
                async with aiofiles.open(file_path, 'wb') as f:
                    await f.write(zip_data['content'])
                # print(f"Archivo '{zip_data['filename']}' guardado en '{UPLOAD_FOLDER}'.")

                # Preparar y guardar el documento en MongoDB
                new_invoice_doc = {
                    "nit": zip_data['nit'],
                    "recibo": zip_data['recibo'],
                    "factura": zip_data['factura'],
                    "create_at": datetime.now(),
                    "status": "pending", # Marcado para ser procesado por un worker posterior
                    "uploaded_ftp": False,
                    "bad_zip_file": False,
                    "file_path": file_path
                }
                invoices_collection.insert_one(new_invoice_doc)
                # print(f"Metadatos de '{zip_data['filename']}' guardados en MongoDB.")

            # Si se insertaron documentos, volvemos a obtener el conteo para reflejar el cambio
            try:
                pending_tasks_count = invoices_collection.count_documents({"status": "pending"})
                print(f"Conteo final de tareas pendientes en MongoDB (después de inserciones): {pending_tasks_count}")
            except Exception as e:
                print(f"Advertencia: No se pudo obtener el conteo final de tareas pendientes de MongoDB: {e}")
                pending_tasks_count = -1

            # --- Llamada al procedimiento almacenado de Oracle DB ---
            if valid_zips_to_process: # Asegurarse de que haya ZIPs válidos para procesar
                # Obtenemos el NIT y Recibo del primer elemento válido, asumiendo que todos pertenecen a la misma transacción
                nit_principal = valid_zips_to_process[0]['nit']
                recibo_principal = valid_zips_to_process[0]['recibo']

                # Construir facturas_list_for_oracle: solo incluir facturas de los ZIPs que se validaron y procesaron correctamente
                facturas_list_for_oracle = [{"factura": f['factura']} for f in valid_zips_to_process]
                facturas_json_str = json.dumps(facturas_list_for_oracle)
                cantidad_facturas = len(facturas_list_for_oracle) # La cantidad debe ser de las facturas válidas

                cursor = None
                try:
                    cursor = oracle_conn.cursor()

                    # 1. Declarar la variable de entrada CLOB para v_pfacturas
                    v_pfacturas_clob_var = cursor.var(oracledb.DB_TYPE_CLOB)
                    v_pfacturas_clob_var.setvalue(0, facturas_json_str)

                    # 2. Declarar la variable de salida CLOB para v_json_row
                    v_json_row_var = cursor.var(oracledb.DB_TYPE_CLOB)

                    # Parámetros para el procedimiento almacenado, incluyendo las variables CLOB
                    cursor.callproc("PQ_GENESIS_RIPS_DIGITAL.P_ACTUALIZA_ESTADO_VALIDACION", [
                        str(nit_principal),
                        str(recibo_principal),
                        v_pfacturas_clob_var, # Pasa la variable CLOB de entrada
                        cantidad_facturas,
                        v_json_row_var        # Pasa la variable CLOB de salida
                    ])
                    oracle_conn.commit()
                    # print(f"Procedimiento PQ_GENESIS_RIPS_DIGITAL.P_ACTUALIZA_ESTADO_VALIDACION llamado exitosamente para NIT: {nit_principal}, Recibo: {recibo_principal}")

                    # Obtener el valor del objeto LOB de salida y luego leer su contenido
                    oracle_response_clob_obj = v_json_row_var.getvalue()
                    if oracle_response_clob_obj:
                        oracle_response_raw_string = oracle_response_clob_obj.read()
                        oracle_response_json = json.loads(oracle_response_raw_string)
                        oracle_response_code = oracle_response_json.get("codigo", "N/A")
                        oracle_response_message = oracle_response_json.get("mensaje", "Respuesta de Oracle vacía o no válida.")
                        print(f"Respuesta de Oracle: Código={oracle_response_code}, Mensaje='{oracle_response_message}'")

                        if oracle_response_code == "1": # Asumiendo que "1" indica error en la respuesta de Oracle
                            errores_contador += 1
                            resultados.append({
                                'nombre_operacion': 'Oracle_DB_Notificacion',
                                'status': 'error_oracle_response',
                                'mensaje': f"Oracle DB reportó un error: {oracle_response_message}"
                            })
                    else:
                        oracle_response_code = "2"
                        oracle_response_message = "Procedimiento Oracle no devolvió una respuesta CLOB."
                        print(f"Advertencia: {oracle_response_message}")
                        errores_contador += 1
                        resultados.append({
                            'nombre_operacion': 'Oracle_DB_Notificacion',
                            'status': 'warning_oracle_response',
                            'mensaje': oracle_response_message
                        })

                except oracledb.Error as e:
                    error_obj, = e.args
                    print(f"Error al llamar al procedimiento almacenado de Oracle DB: {error_obj.message}")
                    oracle_conn.rollback()
                    oracle_response_code = "1"
                    oracle_response_message = f"Error interno al comunicarse con Oracle DB: {error_obj.message}"
                    errores_contador += 1
                    resultados.append({
                        'nombre_operacion': 'Oracle_DB_Notificacion',
                        'status': 'error_oracle_db',
                        'mensaje': oracle_response_message
                    })
                except json.JSONDecodeError as e:
                    print(f"Error al decodificar la respuesta JSON de Oracle: {e}")
                    oracle_response_code = "1"
                    oracle_response_message = f"Respuesta JSON inválida de Oracle: {e}"
                    errores_contador += 1
                    resultados.append({
                        'nombre_operacion': 'Oracle_DB_Notificacion',
                        'status': 'error_oracle_response_parse',
                        'mensaje': oracle_response_message
                    })
                finally:
                    if cursor:
                        cursor.close()
            else:
                oracle_response_message = "No hay archivos ZIP válidos para notificar a Oracle DB."
                print(oracle_response_message)


        except PyMongoError as e:
            print(f"Error general al guardar en MongoDB o archivos: {e}")
            for res in resultados:
                if res['status'] == 'success':
                    res['status'] = 'error_db'
                    res['mensaje'] = f"Error general al guardar en la base de datos: {e}"
            errores_contador += len(valid_zips_to_process)
        except Exception as e:
            print(f"Error inesperado durante el guardado de archivos o inserción en DB: {e}")
            for res in resultados:
                if res['status'] == 'success':
                    res['status'] = 'error_file_save'
                    res['mensaje'] = f"Error inesperado al guardar archivos o insertar en DB: {e}"
            errores_contador += len(valid_zips_to_process)

    # else:
    #     oracle_response_message = "Errores de validación iniciales, no se procesaron archivos ZIP válidos."
    #     print(oracle_response_message)

    # Asegurarse de cerrar la conexión a Oracle DB al finalizar la petición
    if oracle_conn:
        try:
            oracle_conn.close()
            # print("Conexión a Oracle DB cerrada.")
        except oracledb.Error as e:
            print(f"Error al cerrar la conexión a Oracle DB: {e}")


    # Aquí es importante usar la lista original de facturas_json para el mensaje informativo
    # si se llegó a cargar el JSON inicial. Si no, manejar el caso.
    nit_info = facturas_json[0].get('nit') if facturas_json and len(facturas_json) > 0 else 'N/A'
    recibo_info = facturas_json[0].get('recibo') if facturas_json and len(facturas_json) > 0 else 'N/A'
    print(f"NIT: {nit_info}/{recibo_info}, Cantidad Errores: {errores_contador}")


    # Determinar el estado general de la respuesta
    overall_status = "success"
    if errores_contador > 0:
        overall_status = "partial_success" if errores_contador < len(zip_files) else "error"
    if len(zip_files) == 0:
        overall_status = "no_files_uploaded"


    # Retornar el diccionario con todos los datos
    return {
        "overall_status": overall_status,
        "pending_tasks_count": pending_tasks_count,
        "oracle_db_response": {
            "code": oracle_response_code,
            "message": oracle_response_message
        },
        "results": resultados
    }

# Para ejecutar la aplicación:
if __name__ == '__main__':
    API_PORT = config['api']['port']
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=API_PORT)