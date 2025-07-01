import os
import json
import zipfile
import shutil
import time
import threading
from datetime import datetime, timedelta
from pymongo import MongoClient
from bson.objectid import ObjectId
import paramiko
import requests
import subprocess
import oracledb

# --- Cargar Configuración desde config.json ---
def load_config(config_path="config.json"):
    """Carga las configuraciones desde un archivo JSON."""
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        raise Exception(f"Error: El archivo de configuración '{config_path}' no se encontró.")
    except json.JSONDecodeError:
        raise Exception(f"Error: No se pudo decodificar el archivo JSON '{config_path}'. Revisa su formato.")

# Cargamos la configuración una vez que el worker se inicia
CONFIG = load_config()

# --- Configuración de MongoDB ---
MONGO_URI = CONFIG["database"]["mongo_uri"]
MONGO_DB_NAME = CONFIG["database"]["mongo_db_name"]
client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]
invoices_collection = db["invoices"]

# --- Configuración de Oracle (AJUSTADO PARA LA NUEVA ESTRUCTURA) ---
# Accede a la subclave 'oracle_db'
ORACLE_USER = CONFIG["database"]["oracle_db"]["user"]
ORACLE_PASSWORD = CONFIG["database"]["oracle_db"]["password"]
ORACLE_DSN = CONFIG["database"]["oracle_db"]["dsn"]
# 'oracle_lib_dir' no está en tu config.json, así que oracledb usará el modo Thin por defecto.
# Si lo añadieras, el código lo leería como antes: ORACLE_LIB_DIR = CONFIG["database"].get("oracle_lib_dir")

# --- Configuración de Directorios (AJUSTADO PARA RUTAS RELATIVAS) ---
# Unir con el directorio de trabajo actual para asegurar que las carpetas se creen correctamente.
# Si prefieres rutas absolutas, puedes definirlas directamente en config.json
UPLOAD_FOLDER = os.path.join(os.getcwd(), CONFIG["directories"]["upload_folder"])
PROCESSING_FOLDER = os.path.join(os.getcwd(), CONFIG["directories"]["processing_folder"])
PROCESSED_FILES_FOLDER = os.path.join(os.getcwd(), CONFIG["directories"]["processed_files_folder"])
BAD_ZIP_FOLDER = os.path.join(os.getcwd(), CONFIG["directories"]["bad_zip_folder"])
SAVE_ZIP_FOLDER = os.path.join(os.getcwd(), CONFIG["directories"]["save_zip_folder"])

for folder in [UPLOAD_FOLDER, PROCESSING_FOLDER, PROCESSED_FILES_FOLDER, BAD_ZIP_FOLDER, SAVE_ZIP_FOLDER]:
    if not os.path.exists(folder):
        os.makedirs(folder)
        print(f"Directorio creado: {folder}")
    # else:
    #     print(f"Directorio ya existe: {folder}")

# --- Configuración Específica del Worker ---
POLL_INTERVAL_SECONDS = CONFIG["worker"]["poll_interval_seconds"]
MAX_CONCURRENT_TASKS = CONFIG["worker"]["max_concurrent_tasks"]
MAX_RETRIES = CONFIG["worker"]["max_retries"]
RETRY_INTERVAL_MINUTES = CONFIG["worker"]["retry_interval_minutes"]
NOTIFICATION_CHECK_INTERVAL_SECONDS = CONFIG["worker"].get("notification_check_interval_seconds", 300)

# --- Configuración SFTP ---
SFTP_CONFIG = CONFIG["sftp"]

# --- Configuración de Ghostscript y Compresión de PDF ---
GHOSTSCRIPT_PATH = CONFIG["pdf_compression"]["ghostscript_path"]
PDF_COMPRESSION_POWER = CONFIG["pdf_compression"]["power"]
PDF_MIN_SIZE_MB = CONFIG["pdf_compression"]["min_size_mb"]

# --- URLs para API de Errores y Notificación ---
# ERROR_API_URL = CONFIG["url"]["error"]
# NOTIFICATION_API_URL = CONFIG["url"].get("notification", ERROR_API_URL) # Usa la URL de notificación si existe, sino la de error.

# Variable global para asegurar que init_oracle_client se llama solo una vez
_oracle_client_initialized = False

# --- Funciones Auxiliares para SFTP con Paramiko ---
def sftp_mkdir_p(sftp_client, remote_path, thread_name="SFTP"):
    """
    Crea directorios recursivamente en el servidor SFTP usando Paramiko.
    Similar a 'mkdir -p' en sistemas Unix.
    """
    if not remote_path.strip():
        return

    # Normalizar la ruta para asegurar que sea absoluta si es necesario.
    # Si la ruta remota no empieza con '/', asume que es relativa al home del usuario SFTP.
    # Paramiko maneja esto bien. Aquí solo nos aseguramos de que las partes sean correctas.
    
    # Manejo de rutas que comienzan con '/' vs. relativas
    if remote_path.startswith('/'):
        current_dir_parts = [''] # Para construir la ruta absoluta correctamente
    else:
        current_dir_parts = [] # Para construir la ruta relativa

    # Filtrar partes vacías (ej. //) y asegurar consistencia
    parts = [part for part in remote_path.split('/') if part]

    for part in parts:
        current_dir_parts.append(part)
        current_dir = '/'.join(current_dir_parts)
        
        # Ajuste para rutas relativas al inicio si no empiezan con '/'
        if not remote_path.startswith('/') and current_dir.startswith('/'):
            current_dir = current_dir[1:] # Eliminar el '/' inicial añadido por join si la original no lo tenía

        try:
            sftp_client.stat(current_dir)
        except FileNotFoundError:
            # print(f"[{thread_name}] Creando directorio remoto: {current_dir}")
            sftp_client.mkdir(current_dir)
        except Exception as e:
            print(f"[{thread_name}] Error inesperado al verificar/crear directorio {current_dir}: {e}")
            notify_api_error(0, f"Error inesperado al verificar/crear directorio {current_dir}: {e}", '', status="error")
            raise


# --- Funciones de Compresión de PDF ---
def compress_pdf(input_path, output_path, power):
    """
    Comprime un archivo PDF utilizando Ghostscript.
    Args:
        input_path (str): Ruta al archivo PDF de entrada.
        output_path (str): Ruta al archivo PDF de salida comprimido.
        power (int): Nivel de compresión (0-4).
    Returns:
        tuple: (True, mensaje de éxito) o (False, mensaje de error).
    """
    quality = {
        0: '/default',
        1: '/prepress',
        2: '/printer',
        3: '/ebook',
        4: '/screen'
    }

    gs_command = [
        GHOSTSCRIPT_PATH,
        '-sDEVICE=pdfwrite',
        '-dCompatibilityLevel=1.4',
        f'-dPDFSETTINGS={quality[power]}',
        '-dNOPAUSE',
        '-dQUIET',
        '-dBATCH',
        f'-sOutputFile={output_path}',
        input_path
    ]

    try:
        result = subprocess.run(gs_command, check=True, capture_output=True, text=True, encoding='utf-8')
        return True, f"Comprimido: {input_path} -> {output_path}. Salida: {result.stdout}"
    except subprocess.CalledProcessError as e:
        if not os.path.exists(input_path):
            return False, f"Error: El archivo '{input_path}' no existe al intentar comprimir."
        # print(f"Error al comprimir '{input_path}': {e}")
        # print(f"Salida de Ghostscript (stderr):\n{e.stderr}")
        return False, f"Error al comprimir {input_path}: {e.stderr}"
    except FileNotFoundError:
        return False, f"Error: No se encontró el ejecutable de Ghostscript en la ruta especificada: '{GHOSTSCRIPT_PATH}'. Verifique la ruta."
    except Exception as e:
        return False, f"Error inesperado en compress_pdf para '{input_path}': {e}"


def process_pdf(input_path, subdir_for_temp_files, power, min_size_mb):
    """
    Procesa un archivo PDF: lo comprime y, si el resultado es más pequeño, reemplaza el original.
    Solo comprime si el tamaño del PDF es mayor que min_size_mb.
    """
    thread_name = threading.current_thread().name

    try:
        if not os.path.exists(input_path):
            print(f"[{thread_name}] Advertencia: Archivo PDF no encontrado para procesar compresión: '{input_path}'.")
            return

        original_size_bytes = os.path.getsize(input_path)
        original_size_mb = original_size_bytes / (1024 * 1024)

        if original_size_mb < min_size_mb:
            # print(f"[{thread_name}] Saltando compresión para '{os.path.basename(input_path)}': Tamaño ({original_size_mb:.2f} MB) es menor que el mínimo ({min_size_mb:.2f} MB).")
            return

        os.makedirs(subdir_for_temp_files, exist_ok=True)
        temp_output_path = os.path.join(subdir_for_temp_files, f"compressed_{os.path.basename(input_path)}")

        # print(f"[{thread_name}] Intentando comprimir PDF: {os.path.basename(input_path)} (Original: {original_size_mb:.2f} MB)")
        success, message = compress_pdf(input_path, temp_output_path, power)

        if success:
            if os.path.exists(temp_output_path):
                compressed_size_bytes = os.path.getsize(temp_output_path)
                compressed_size_mb = compressed_size_bytes / (1024 * 1024)

                if compressed_size_bytes < original_size_bytes:
                    try:
                        os.replace(temp_output_path, input_path)
                        print(f"[{thread_name}] Reemplazado: {os.path.basename(input_path)} (Original: {original_size_mb:.2f} MB, Comprimido: {compressed_size_mb:.2f} MB)")
                    except OSError as e:
                        print(f"[{thread_name}] Error al reemplazar '{input_path}': {e}")
                        os.remove(temp_output_path)
                else:
                    os.remove(temp_output_path)
                    # print(f"[{thread_name}] No reemplazado: {os.path.basename(input_path)} (El archivo comprimido no es más pequeño). Original: {original_size_mb:.2f} MB, Comprimido: {compressed_size_mb:.2f} MB)")
            else:
                print(f"[{thread_name}] ERROR: La compresión de '{os.path.basename(input_path)}' reportó éxito pero el archivo de salida '{temp_output_path}' no se encontró.")
                if os.path.exists(temp_output_path): os.remove(temp_output_path)
        else:
            print(f"[{thread_name}] Fallo al comprimir PDF: {message}")
            if os.path.exists(temp_output_path): os.remove(temp_output_path)

    except Exception as e:
        print(f"[{thread_name}] Error inesperado en process_pdf para '{input_path}': {e}")
        if 'temp_output_path' in locals() and os.path.exists(temp_output_path):
            os.remove(temp_output_path)


# --- Función para enviar notificación de error ---
def notify_api_error(invoice_id, message, file_name="", status="error"):
    """Realiza una petición GET a la API de notificación (error o éxito)."""
    global _oracle_client_initialized
    if not _oracle_client_initialized:
        # Es crucial que Oracle Instant Client esté instalado y configurado.
        # Su ruta debe estar en la variable de entorno PATH (Windows) o LD_LIBRARY_PATH (Linux).
        # o se especifica explícitamente:
        # oracledb.init_oracle_client(lib_dir="/ruta/donde/descomprimiste/instantclient_21_x")
        oracledb.init_oracle_client() # Inicializa el cliente Oracle en modo Thick
        _oracle_client_initialized = True
        # print("Cliente Oracle inicializado en modo Thick.")

    thread_name = threading.current_thread().name
    
    connection = None
    cursor = None
    
    try:
        connection = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN)
        cursor = connection.cursor()

        # 2. Declarar la variable de salida CLOB para v_json_row
        v_json_row_var = cursor.var(oracledb.DB_TYPE_CLOB)

        # Tu procedimiento espera: NIT, RECIBO, FACTURA, INVOICE_ID (del worker)
        # Asegúrate del orden correcto y tipos de datos esperados por el procedimiento
        # p_nit VARCHAR2, p_recibo VARCHAR2, p_factura VARCHAR2, p_invoice_id VARCHAR2
        cursor.callproc("PQ_GENESIS_RIPS_DIGITAL.P_NOTIFICA_ERROR_SFTP", [message, v_json_row_var])
        
        connection.commit()
        # notification_successful = True
        # print(f"[{thread_name}] Llamada al procedimiento Oracle PQ_GENESIS_RIPS_DIGITAL.P_NOTIFICA_ERROR_SFTP exitosa para invoice_id {invoice_id}.")
        # Obtener el valor del objeto LOB de salida y luego leer su contenido
        oracle_response_clob_obj = v_json_row_var.getvalue()
        if oracle_response_clob_obj:
            oracle_response_raw_string = oracle_response_clob_obj.read()
            oracle_response_json = json.loads(oracle_response_raw_string)
            oracle_response_code = oracle_response_json.get("codigo", "N/A")
            oracle_response_message = oracle_response_json.get("mensaje", "Respuesta de Oracle vacía o no válida.")
            print(f"Respuesta de Oracle: Código={oracle_response_code}, Mensaje='{oracle_response_message}'")

    except oracledb.Error as e:
        error_obj = e.args[0]
        print(f"[{thread_name}] ERROR al conectar/ejecutar procedimiento Oracle para invoice_id {invoice_id}:")
        # print(f"[{thread_name}] Oracle Error Code: {error_obj.code}")
        # print(f"[{thread_name}] Oracle Error Message: {error_obj.message}")
        if connection:
            connection.rollback()
    except Exception as e:
        print(f"[{thread_name}] ERROR inesperado al interactuar con Oracle para invoice_id {invoice_id}: {e}")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


# --- Nueva función para conectarse a Oracle y llamar al procedimiento ---
def call_oracle_procedure(invoice_id, nit, recibo, factura):
    """
    Se conecta a la base de datos Oracle y llama al procedimiento
    P_ACTUALIZA_ESTADO_FACTURA con los parámetros proporcionados.
    Lee la respuesta del procedimiento.
    """
    global _oracle_client_initialized
    if not _oracle_client_initialized:
        # Es crucial que Oracle Instant Client esté instalado y configurado.
        # Su ruta debe estar en la variable de entorno PATH (Windows) o LD_LIBRARY_PATH (Linux).
        # o se especifica explícitamente:
        # oracledb.init_oracle_client(lib_dir="/ruta/donde/descomprimiste/instantclient_21_x")
        oracledb.init_oracle_client() # Inicializa el cliente Oracle en modo Thick
        _oracle_client_initialized = True
        # print("Cliente Oracle inicializado en modo Thick.")

    thread_name = threading.current_thread().name
    # print(f"[{thread_name}] Intentando conectar a Oracle y llamar P_ACTUALIZA_ESTADO_FACTURA para invoice_id: {invoice_id}")
    
    connection = None
    cursor = None
    notification_successful = False
    
    try:
        connection = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN)
        cursor = connection.cursor()

        # 2. Declarar la variable de salida CLOB para v_json_row
        v_json_row_var = cursor.var(oracledb.DB_TYPE_CLOB)

        # Tu procedimiento espera: NIT, RECIBO, FACTURA, INVOICE_ID (del worker)
        # Asegúrate del orden correcto y tipos de datos esperados por el procedimiento
        # p_nit VARCHAR2, p_recibo VARCHAR2, p_factura VARCHAR2, p_invoice_id VARCHAR2
        cursor.callproc("PQ_GENESIS_RIPS_DIGITAL.P_ACTUALIZA_ESTADO_FACTURA", [nit, recibo, factura, invoice_id, v_json_row_var])
        
        connection.commit()
        # notification_successful = True
        # print(f"[{thread_name}] Llamada al procedimiento Oracle PQ_GENESIS_RIPS_DIGITAL.P_ACTUALIZA_ESTADO_FACTURA exitosa para invoice_id {invoice_id}.")
        # Obtener el valor del objeto LOB de salida y luego leer su contenido
        oracle_response_clob_obj = v_json_row_var.getvalue()
        if oracle_response_clob_obj:
            oracle_response_raw_string = oracle_response_clob_obj.read()
            oracle_response_json = json.loads(oracle_response_raw_string)
            oracle_response_code = oracle_response_json.get("codigo", "N/A")
            oracle_response_message = oracle_response_json.get("mensaje", "Respuesta de Oracle vacía o no válida.")
            print(f"Respuesta de Oracle: Código={oracle_response_code}, Mensaje='{oracle_response_message}'")

            if oracle_response_code == "0":
                notification_successful = True
            if oracle_response_code == "1":
                notification_successful = False
        else:
            notification_successful = False

    except oracledb.Error as e:
        error_obj = e.args[0]
        print(f"[{thread_name}] ERROR al conectar/ejecutar procedimiento Oracle para invoice_id {invoice_id}:")
        print(f"[{thread_name}] Oracle Error Code: {error_obj.code}")
        print(f"[{thread_name}] Oracle Error Message: {error_obj.message}")
        if connection:
            connection.rollback()
    except Exception as e:
        print(f"[{thread_name}] ERROR inesperado al interactuar con Oracle para invoice_id {invoice_id}: {e}")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        
    # Actualizar el estado de notificación en MongoDB basado en el éxito de la llamada a Oracle
    try:
        invoices_collection.update_one(
            {"_id": ObjectId(invoice_id)},
            {"$set": {"notification_sent_successfully": notification_successful}}
        )
        # print(f"[{thread_name}] Estado de notificación (Oracle) para invoice_id {invoice_id} actualizado a: {notification_successful}")
    except Exception as db_e:
        print(f"[{thread_name}] ERROR al actualizar 'notification_sent_successfully' para invoice_id {invoice_id}: {db_e}")

    return notification_successful


# --- Función para verificar y re-notificar facturas ---
def check_pending_notifications():
    """
    Busca facturas que ya han sido subidas al SFTP pero cuya notificación de éxito
    a la base de datos Oracle aún no se ha enviado correctamente o no está marcada.
    """
    thread_name = threading.current_thread().name
    # print(f"[{thread_name}] Iniciando verificación de notificaciones pendientes para Oracle...")

    query = {
        "$or": [
            {"status": "completed", "$or": [{"notification_sent_successfully": {"$ne": True}}, {"notification_sent_successfully": {"$exists": False}}]},
            {"status": "completed_bad_zip_uploaded", "$or": [{"notification_sent_successfully": {"$ne": True}}, {"notification_sent_successfully": {"$exists": False}}]}
        ]
    }

    pending_notifications = invoices_collection.find(query)
    count = 0
    for invoice_doc in pending_notifications:
        invoice_id = str(invoice_doc["_id"])
        nit = invoice_doc.get("nit", "N/A")
        recibo = invoice_doc.get("recibo", "N/A")
        factura = invoice_doc.get("factura", "N/A")

        print(f"[{thread_name}] Reintentando notificación Oracle para invoice_id: {invoice_id} (NIT: {nit}, Factura: {factura})")
        success = call_oracle_procedure(invoice_id, nit, recibo, factura)
        if success:
            count += 1
        time.sleep(1) # Pequeña pausa para no sobrecargar la DB Oracle

    if count > 0:
        print(f"[{thread_name}] Finalizada verificación de notificaciones pendientes (Oracle). Se re-notificaron {count} facturas.")
    else:
        print(f"[{thread_name}] No se encontraron facturas con notificaciones pendientes para Oracle.")


# --- Función de Tarea del Worker ---
def process_single_invoice(invoice_doc):
    """
    Procesa una única factura: copia el ZIP a la carpeta de procesamiento, intenta descomprimir/procesar.
    Si es un ZIP defectuoso, lo sube directamente a SFTP en la ruta base.
    Maneja reintentos para errores recuperables y notifica al finalizar.
    """
    invoice_id = str(invoice_doc["_id"])
    thread_name = threading.current_thread().name

    nit = invoice_doc.get("nit", "N/A")
    recibo = invoice_doc.get("recibo", "N/A")
    factura = invoice_doc.get("factura", "N/A")

    factura_log_info = f"{nit}/{factura}"
    print(f"[{thread_name}] Iniciando procesamiento para factura: {factura_log_info}")

    original_zip_path = invoice_doc.get("file_path")

    current_retries = invoice_doc.get("retries", 0)
    update_fields = {
        "status": "processing",
        "processing_start_time": datetime.now(),
        "retries": current_retries + 1,
        "last_retry_at": datetime.now(),
        "notification_sent_successfully": False # Resetear en cada intento de procesamiento
    }
    invoices_collection.update_one(
        {"_id": ObjectId(invoice_id)},
        {"$set": update_fields}
    )
    print(f"[{thread_name}] Procesando (Intento #{current_retries + 1}) para factura: {factura_log_info}")

    should_delete_original_zip = False
    should_delete_processing_zip = False
    should_delete_extraction_folder = False

    processing_zip_path = ""
    local_extraction_folder = ""
    zip_filename = os.path.basename(original_zip_path) if original_zip_path else "UNKNOWN_FILE"

    transport = None
    sftp = None

    try:
        if not original_zip_path or not os.path.exists(original_zip_path):
            error_message = "Archivo ZIP original no encontrado o accesible."
            # print(f"[{thread_name}] {error_message} en {original_zip_path}.")
            invoices_collection.update_one(
                {"_id": ObjectId(invoice_id)},
                {"$set": {"status": "failed", "error_message": error_message}}
            )
            # notify_api_error(invoice_id, error_message, original_zip_path, status="error")
            return

        processing_zip_path = os.path.join(PROCESSING_FOLDER, zip_filename)
        shutil.copy(original_zip_path, processing_zip_path)
        should_delete_processing_zip = True
        # print(f"[{thread_name}] Copiado '{zip_filename}' a '{processing_zip_path}'.")

        base_name_without_ext = os.path.splitext(zip_filename)[0]
        parts = base_name_without_ext.split('_')

        if len(parts) != 2:
            raise ValueError(f"Formato de nombre de archivo ZIP inválido: {zip_filename}. Se esperaba NIT_RECIBO.zip")

        nit_folder = parts[0]
        # El recibo es el segundo elemento, pero tu ruta SFTP usa factura_folder.
        # Asumiendo que factura_folder también se refiere al segundo elemento.
        # Si 'factura' es diferente del recibo del nombre del ZIP, ajusta aquí.
        factura_folder = parts[1] 

        local_extraction_folder = os.path.join(PROCESSED_FILES_FOLDER, base_name_without_ext)
        os.makedirs(local_extraction_folder, exist_ok=True)
        should_delete_extraction_folder = True

        with zipfile.ZipFile(processing_zip_path, 'r') as zip_ref:
            zip_ref.extractall(local_extraction_folder)

        pdf_count = 0
        for root, _, files in os.walk(local_extraction_folder):
            for file in files:
                if file.lower().endswith('.pdf'):
                    pdf_path = os.path.join(root, file)
                    process_pdf(pdf_path, root, PDF_COMPRESSION_POWER, PDF_MIN_SIZE_MB)
                    pdf_count += 1
        # if pdf_count > 0:
        #     print(f"[{thread_name}] Finalizada la compresión de {pdf_count} PDF(s) en '{local_extraction_folder}'.")
        # else:
        #     print(f"[{thread_name}] No se encontraron archivos PDF para comprimir en '{local_extraction_folder}'.")

        # Asegúrate de que SFTP_CONFIG["remote_base_path"] termina en un separador si es una carpeta base
        # o que os.path.join maneje correctamente la unión de rutas si no es así.
        sftp_remote_path = os.path.join(SFTP_CONFIG["remote_base_path"], nit_folder, factura_folder).replace("\\", "/")
        # print(f"[{thread_name}] Intentando subir a SFTP a la ruta remota: {sftp_remote_path}")

        transport = paramiko.Transport((SFTP_CONFIG["host"], SFTP_CONFIG.get("port", 22)))
        
        try:
            private_key = paramiko.RSAKey.from_private_key_file(SFTP_CONFIG["priv_key"])
        except paramiko.ssh_exception.SSHException as e:
            raise Exception(f"Error al cargar la clave privada en '{SFTP_CONFIG['priv_key']}': {e}")
            
        transport.connect(username=SFTP_CONFIG["username"], pkey=private_key)
        sftp = paramiko.SFTPClient.from_transport(transport)

        sftp_mkdir_p(sftp, sftp_remote_path, thread_name)

        for root, _, files in os.walk(local_extraction_folder):
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_file_path, local_extraction_folder)
                remote_file_path = os.path.join(sftp_remote_path, relative_path).replace("\\", "/")
                
                sftp.put(local_file_path, remote_file_path)
                # print(f"[{thread_name}] Subido: {remote_file_path}")

        # print(f"[{thread_name}] Todos los archivos de '{zip_filename}' subidos a SFTP exitosamente.")

        invoices_collection.update_one(
            {"_id": ObjectId(invoice_id)},
            {"$set": {"status": "completed", "uploaded_ftp": True, "processed_at": datetime.now(), "sftp_path": sftp_remote_path}}
        )
        print(f"[{thread_name}] Factura ID {invoice_id} procesada y completada. Ruta SFTP: {sftp_remote_path}")
        should_delete_original_zip = True

        # --- Notificar subida exitosa llamando al procedimiento Oracle ---
        call_oracle_procedure(invoice_id, nit, recibo, factura)
        # También notificar a la API de éxito si es diferente de la de error
        # notify_api_error(invoice_id, f"Factura {factura_log_info} procesada y subida a SFTP.", zip_filename, status="success")

    except zipfile.BadZipFile:
        error_msg = f"'{zip_filename}' es un archivo ZIP inválido. Se intentará subir el ZIP directamente a SFTP en la ruta base."
        # print(f"[{thread_name}] {error_msg}")

        try:
            # Subir ZIP defectuoso a la ruta base SFTP.
            # Asegúrate de que el path para bad_zip_folder se construye correctamente en el SFTP.
            # Aquí lo estoy subiendo a la misma remote_base_path pero con el nombre del zip directamente.
            sftp_bad_zip_target_path = os.path.join(SFTP_CONFIG["remote_base_path"], zip_filename).replace("\\", "/")

            if transport is None or not transport.is_active():
                transport = paramiko.Transport((SFTP_CONFIG["host"], SFTP_CONFIG.get("port", 22)))
                try:
                    private_key = paramiko.RSAKey.from_private_key_file(SFTP_CONFIG["priv_key"])
                except paramiko.ssh_exception.SSHException as e:
                    raise Exception(f"Error al cargar la clave privada en '{SFTP_CONFIG['priv_key']}': {e}")
                transport.connect(username=SFTP_CONFIG["username"], pkey=private_key)
                sftp = paramiko.SFTPClient.from_transport(transport)
            
            sftp_mkdir_p(sftp, os.path.dirname(sftp_bad_zip_target_path), thread_name)

            sftp.put(processing_zip_path, sftp_bad_zip_target_path)
            print(f"[{thread_name}] Archivo ZIP defectuoso subido a SFTP: {sftp_bad_zip_target_path}")

            invoices_collection.update_one(
                {"_id": ObjectId(invoice_id)},
                {"$set": {
                    "status": "completed_bad_zip_uploaded",
                    "bad_zip_file": True,
                    "error_message": error_msg,
                    "sftp_bad_zip_path": sftp_bad_zip_target_path
                }}
            )
            # notify_api_error(invoice_id, error_msg, zip_filename, status="error")
            should_delete_original_zip = True
            should_delete_processing_zip = True
            should_delete_extraction_folder = True

            # --- Notificar subida exitosa de ZIP defectuoso llamando al procedimiento Oracle ---
            call_oracle_procedure(invoice_id, nit, recibo, factura)

            return

        except Exception as sftp_e:
            error_msg_sftp_bad_zip_upload = f"Error al subir el ZIP defectuoso '{zip_filename}' a SFTP ({sftp_bad_zip_target_path}): {sftp_e}"
            # print(f"[{thread_name}] {error_msg_sftp_bad_zip_upload}")

            invoices_collection.update_one(
                {"_id": ObjectId(invoice_id)},
                {"$set": {
                    "status": "failed",
                    "bad_zip_file": True,
                    "error_message": error_msg_sftp_bad_zip_upload
                }}
            )
            notify_api_error(invoice_id, error_msg_sftp_bad_zip_upload, zip_filename, status="error")
            should_delete_processing_zip = True
            should_delete_extraction_folder = True
            return

    except ValueError as e:
        error_msg = f"Error de formato de nombre de archivo para '{zip_filename}': {e}. Moviendo a '{BAD_ZIP_FOLDER}'."
        # print(f"[{thread_name}] {error_msg}")

        if os.path.exists(processing_zip_path):
            destination_value_error_path = os.path.join(BAD_ZIP_FOLDER, zip_filename)
            try:
                shutil.move(processing_zip_path, destination_value_error_path)
            except Exception as move_e:
                print(f"[{thread_name}] ERROR al mover '{zip_filename}' a BadZips: {move_e}")

        invoices_collection.update_one(
            {"_id": ObjectId(invoice_id)},
            {"$set": {"status": "failed", "bad_file_name": True, "error_message": error_msg}}
        )
        notify_api_error(invoice_id, error_msg, zip_filename, status="error")
        should_delete_original_zip = True
        should_delete_extraction_folder = True
        return

    except Exception as e:
        error_msg = f"Error inesperado procesando '{zip_filename}' para ID {invoice_id}: {e}"
        # print(f"[{thread_name}] {error_msg}")
        invoices_collection.update_one(
            {"_id": ObjectId(invoice_id)},
            {"$set": {"status": "failed", "error_message": error_msg}}
        )
        notify_api_error(invoice_id, error_msg, zip_filename, status="error")
        should_delete_processing_zip = True
        should_delete_extraction_folder = True
        return

    finally:
        if sftp:
            try:
                sftp.close()
            except Exception as e:
                print(f"[{thread_name}] ERROR al cerrar SFTP client: {e}")
        if transport and transport.is_active():
            try:
                transport.close()
            except Exception as e:
                print(f"[{thread_name}] ERROR al cerrar Paramiko transport: {e}")

        if should_delete_original_zip and os.path.exists(original_zip_path):
            try:
                # os.remove(original_zip_path) # Comentado según tu código original
                destination_save_zip = os.path.join(SAVE_ZIP_FOLDER, zip_filename)
                shutil.move(original_zip_path, destination_save_zip)
                # print(f"[{thread_name}] Eliminado ZIP original de FilesPending: '{original_zip_path}'.")
            except Exception as e:
                print(f"[{thread_name}] ERROR al eliminar ZIP original: {e}")

        if should_delete_processing_zip and os.path.exists(processing_zip_path):
            try:
                os.remove(processing_zip_path)
                # print(f"[{thread_name}] Eliminado archivo ZIP de procesamiento: '{zip_filename}'.")
            except Exception as e:
                print(f"[{thread_name}] ERROR al eliminar ZIP de procesamiento: {e}")

        if should_delete_extraction_folder and os.path.exists(local_extraction_folder):
            try:
                shutil.rmtree(local_extraction_folder)
            except Exception as e:
                print(f"[{thread_name}] ERROR al eliminar carpeta de extracción: {e}")

        print(f"[{thread_name}] Procesamiento finalizado para factura: {factura_log_info}.")


# --- Bucle Principal del Worker ---
def worker_main():
    """
    Bucle principal del worker que consulta continuamente MongoDB para tareas pendientes
    y fallidas que califican para reintento, despachándolas a hilos separados.
    También ejecuta periódicamente la verificación de notificaciones pendientes.
    """
    print(f"Worker iniciado el {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z%z')}.")
    print(f"Consultando MongoDB cada {POLL_INTERVAL_SECONDS} segundos para tareas pendientes y fallidas...")
    print(f"Máximo de tareas concurrentes: {MAX_CONCURRENT_TASKS}")
    print(f"Máximo de reintentos: {MAX_RETRIES}, Intervalo de reintento: {RETRY_INTERVAL_MINUTES} minutos.")
    print(f"Verificación de notificaciones pendientes cada {NOTIFICATION_CHECK_INTERVAL_SECONDS} segundos.")

    active_threads = []
    last_notification_check_time = time.time()

    while True:
        active_threads = [t for t in active_threads if t.is_alive()]

        if time.time() - last_notification_check_time >= NOTIFICATION_CHECK_INTERVAL_SECONDS:
            # print("--- Ejecutando verificación de notificaciones pendientes para Oracle ---")
            check_pending_notifications()
            last_notification_check_time = time.time()
            # print("--- Verificación de notificaciones pendientes (Oracle) finalizada ---")

        if len(active_threads) < MAX_CONCURRENT_TASKS:
            retry_threshold_time = datetime.now() - timedelta(minutes=RETRY_INTERVAL_MINUTES)

            query = {
                "$or": [
                    {"status": "pending"},
                    {
                        "status": "failed",
                        "bad_zip_file": {"$ne": True},
                        "bad_file_name": {"$ne": True},
                        "retries": {"$lt": MAX_RETRIES},
                        "$or": [
                            {"last_retry_at": {"$lt": retry_threshold_time}},
                            {"last_retry_at": {"$exists": False}}
                        ]
                    }
                ]
            }

            tasks_to_process = invoices_collection.find(query).sort("create_at", 1).limit(MAX_CONCURRENT_TASKS - len(active_threads))

            for invoice_doc in tasks_to_process:
                thread = threading.Thread(
                    target=process_single_invoice,
                    args=(invoice_doc,),
                    name=f"InvoiceProcessor-{invoice_doc.get('nit','N/A')}_{invoice_doc.get('factura','N/A')}"
                )
                thread.start()
                active_threads.append(thread)
                status_msg = "pendiente" if invoice_doc["status"] == "pending" else "fallida (reintento)"
                print(f"Hilo despachado para procesar factura {status_msg}: {invoice_doc.get('nit','N/A')}/{invoice_doc.get('factura','N/A')}")

        time.sleep(POLL_INTERVAL_SECONDS)

# --- Punto de Entrada para el Script del Worker ---
if __name__ == "__main__":
    worker_main()