import xmlrpc.client
import os
import re
import sys
import uuid
import argparse
from dotenv import load_dotenv
import ssl
import socket
import time
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
import google.auth
import json
import pandas as pd

# Cargar variables de entorno desde ubicación específica
import logging

script_dir = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(script_dir, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/mail_crm.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Cargar .env desde el directorio del script (compatible Windows y Linux)
env_path = os.path.join(script_dir, ".env")
load_dotenv(dotenv_path=env_path)

# ==== CONFIGURACIÓN ODOO ====
url = os.getenv('ODOO_URL')
db = os.getenv('ODOO_DB')
username = os.getenv('ODOO_USERNAME')
password = os.getenv('ODOO_API_KEY') or os.getenv('ODOO_PASSWORD')

# ==== CONFIGURACIÓN GCP ====
project_id = os.getenv('GCP_PROJECT_ID', 'nebulan-476518')
dataset_id = os.getenv('GCP_DATASET_ID', 'nebulanodoo')

# Soporta override por env: GCP_TABLE_MAIL_MESSAGE / GCP_TABLE_MAIL_TRACKING
# o GCP_TABLE_NAME (aplica prefijo st_ a ambas si empieza con 'st_')
_default_msg = 'prod_mail_message'
_default_trk = 'prod_mail_tracking_value'

table_messages = os.getenv('GCP_TABLE_MAIL_MESSAGE', _default_msg)
table_messages_temp = f"{table_messages}_temp"
table_tracking = os.getenv('GCP_TABLE_MAIL_TRACKING', _default_trk)
table_tracking_temp = f"{table_tracking}_temp"
table_sync_control = os.getenv('GCP_TABLE_SYNC_CONTROL', 'etl_sync_control')
SYNC_SAFETY_MARGIN_MINUTES = 5
DEBUG_MODE = os.getenv('DEBUG_MODE', 'false').lower() == 'true'

# ==== CONFIGURACIÓN DE LOTES ====
BATCH_SIZE = 1000
SLEEP_AFTER_TABLE_OPS = 10
SLEEP_BETWEEN_BATCHES = 1
TABLE_READY_MAX_WAIT = 30
LOAD_MODE_MESSAGES = 'incremental'
LOAD_MODE_TRACKING = 'incremental'

# Contexto Odoo: español Chile + timezone Santiago.
# allowed_company_ids y active_test se inyectan dinamicamente (init_odoo_context).
ODOO_CONTEXT = {
    'lang': 'es_CL',
    'tz': 'America/Santiago',
}


def init_odoo_context(models, uid, model_name):
    """Detecta companias + active_test (solo si el modelo tiene campo 'active')."""
    try:
        info = models.execute_kw(
            db, uid, password, 'res.users', 'read', [[uid]],
            {'fields': ['company_ids']}
        )
        if info and info[0].get('company_ids'):
            ODOO_CONTEXT['allowed_company_ids'] = info[0]['company_ids']
            logger.info(f"   Empresas permitidas: {ODOO_CONTEXT['allowed_company_ids']}")
    except Exception as e:
        logger.info(f"   No se detectaron company_ids: {e}")
    try:
        fields = models.execute_kw(
            db, uid, password, model_name, 'fields_get', [],
            {'attributes': ['type']}
        )
        if 'active' in fields:
            ODOO_CONTEXT['active_test'] = False
            logger.info(f"   {model_name} tiene 'active' - incluyendo archivados")
        else:
            ODOO_CONTEXT.pop('active_test', None)
    except Exception as e:
        logger.info(f"   active_test check fallo: {e}")

def validate_environment_variables():
    """Valida que todas las variables de entorno necesarias estén configuradas"""
    required_vars = {
        'ODOO_URL': url,
        'ODOO_DB': db,
        'ODOO_USERNAME': username,
        'ODOO_PASSWORD': password
    }
    
    missing_vars = [var for var, value in required_vars.items() if not value]
    
    if missing_vars:
        logger.error("ERROR: Faltan las siguientes variables de entorno:")
        for var in missing_vars:
            logger.info(f"   - {var}")
        
        logger.info("Para solucionar este problema:")
        logger.info("1. Verifica que el archivo .env existe en la ruta configurada")
        logger.info("2. El archivo .env debe contener las siguientes líneas:")
        logger.info("")
        logger.info("ODOO_URL=https://tu-servidor-odoo.com")
        logger.info("ODOO_DB=nombre_de_tu_base_de_datos")
        logger.info("ODOO_USERNAME=tu_usuario")
        logger.info("ODOO_PASSWORD=tu_contraseña")
        logger.info("")
        logger.info(" Ubicación del archivo .env configurada:")
        logger.info(f"   {env_path}")
        
        # Verificar si el archivo existe
        if os.path.exists(env_path):
            logger.info("El archivo .env existe en la ubicación esperada")
            logger.warning("Verifica que las variables estén correctamente definidas en el archivo")
        else:
            logger.error("El archivo .env NO existe en la ubicación esperada")
            logger.info("Crea el archivo .env en esa ubicación con las variables necesarias")
        
        return False
    
    logger.info("Todas las variables de entorno están configuradas correctamente:")
    logger.info(f"   - Archivo .env encontrado en: {env_path}")
    logger.info(f"   - ODOO_URL: {url}")
    logger.info(f"   - ODOO_DB: {db}")
    logger.info(f"   - ODOO_USERNAME: {username}")
    logger.info(f"   - ODOO_PASSWORD: {'*' * len(password) if password else 'NO CONFIGURADA'}")
    
    return True

def convert_datetime_to_timestamp(datetime_value):
    """Convierte datetime de Odoo a formato compatible con BigQuery TIMESTAMP"""
    if not datetime_value or datetime_value in [False, None, '', 'False']:
        return None
    
    try:
        # Si es string, convertir a datetime primero
        if isinstance(datetime_value, str):
            datetime_value = datetime_value.strip()
            if not datetime_value or datetime_value.lower() in ['false', 'none', '']:
                return None
            
            # Formatos típicos de Odoo
            formats_to_try = [
                "%Y-%m-%d %H:%M:%S",      # Formato principal de Odoo
                "%Y-%m-%d %H:%M:%S.%f",   # Con microsegundos
                "%Y-%m-%dT%H:%M:%S",      # ISO format sin Z
                "%Y-%m-%dT%H:%M:%SZ",     # ISO format con Z
                "%Y-%m-%dT%H:%M:%S.%fZ",  # ISO format con microsegundos y Z
                "%Y-%m-%d",               # Solo fecha
            ]
            
            dt = None
            for fmt in formats_to_try:
                try:
                    dt = datetime.strptime(datetime_value, fmt)
                    break
                except ValueError:
                    continue
            
            if dt is None:
                logger.warning(f"No se pudo parsear fecha: {datetime_value}")
                return None
                
            # Convertir a UTC timestamp string para BigQuery
            return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        
        # Si ya es datetime object
        elif isinstance(datetime_value, datetime):
            return datetime_value.strftime("%Y-%m-%d %H:%M:%S UTC")
        
        # Si es otro tipo, intentar convertir a string y procesar
        else:
            return convert_datetime_to_timestamp(str(datetime_value))
            
    except Exception as e:
        logger.warning(f"Error convirtiendo datetime {datetime_value}: {e}")
        return None

def safe_convert_to_int(value):
    """Convierte un valor a entero de forma segura para BigQuery"""
    if value is None or value == '' or value == 'False' or value == False or value == 'None':
        return None
    try:
        if isinstance(value, str):
            value = value.strip()
            if value == '' or value.lower() in ('none', 'false', 'null'):
                return None
        # BigQuery INTEGER puede manejar hasta 64-bit signed integers
        result = int(float(value))
        # Verificar límites de BigQuery INTEGER
        if result < -9223372036854775808 or result > 9223372036854775807:
            logger.warning(f"Valor entero fuera de rango para BigQuery: {result}")
            return None
        return result
    except (ValueError, TypeError, OverflowError):
        return None

def safe_convert_to_float(value):
    """Convierte un valor a float de forma segura para BigQuery"""
    if value is None or value == '' or value == 'False' or value == False or value == 'None':
        return None
    try:
        if isinstance(value, str):
            value = value.strip()
            if value == '' or value.lower() in ('none', 'false', 'null'):
                return None
        result = float(value)
        # Verificar que no sea inf o -inf (BigQuery no los soporta)
        if result == float('inf') or result == float('-inf') or result != result:  # NaN check
            return None
        return result
    except (ValueError, TypeError, OverflowError):
        return None

def safe_convert_to_bool(value):
    """Convierte un valor a boolean de forma segura para BigQuery"""
    if value is None or value == '' or value == 'None':
        return None
    if isinstance(value, str):
        value = value.strip().lower()
        if value in ('', 'none', 'null'):
            return None
        return value in ('true', '1', 'yes', 'on', 't', 'y')
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return None

def extract_many2one_info(value):
    """Extrae información de un campo many2one"""
    if isinstance(value, list) and len(value) > 1:
        return {
            'id': safe_convert_to_int(value[0]),
            'name': str(value[1])
        }
    else:
        return {
            'id': None,
            'name': None
        }

def clean_text_for_bigquery(text):
    """Limpia texto para BigQuery con manejo robusto de encoding y caracteres especiales"""
    if not text or text in [False, None]:
        return None
    
    text = str(text)
    
    # Remover caracteres de control problemáticos y caracteres no UTF-8
    import re
    
    # Remover caracteres de control (0x00-0x1F excepto \t, \n, \r)
    text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', text)
    
    # Remover caracteres no válidos para UTF-8
    text = text.encode('utf-8', errors='ignore').decode('utf-8')
    
    # Escapar caracteres problemáticos para JSON
    text = text.replace('\x00', '')  # NULL bytes
    text = text.replace('\r\n', '\n')  # Normalizar saltos de línea
    text = text.replace('\r', '\n')    # Normalizar saltos de línea
    
    # Limitar longitud para BigQuery
    # STRING en BigQuery tiene límite de 1MB pero es recomendable mantenerlo más pequeño
    max_length = 65536  # 64KB límite práctico
    if len(text) > max_length:
        text = text[:max_length] + "...[TRUNCATED]"
    
    # Verificar que no esté vacío después de limpieza
    text = text.strip()
    if not text:
        return None
    
    return text

def create_batches(data, batch_size):
    """Divide una lista en lotes del tamaño especificado"""
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]


def odoo_connect():
    """Conecta a Odoo y retorna (models, uid). Init contexto multicompany."""
    if url.startswith('https'):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    socket.setdefaulttimeout(120)

    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', verbose=False)
    version = common.version()
    logger.info(f"Conexión establecida. Versión Odoo: {version.get('server_version', 'N/A')}")

    uid = common.authenticate(db, username, password, {})
    if not uid:
        logger.error("Error de autenticación. Verifique las credenciales.")
        return None, None

    logger.info(f"Autenticación exitosa. UID: {uid}")
    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', verbose=False)
    init_odoo_context(models, uid, 'mail.message')
    return models, uid


def get_crm_lead_message_ids(models, uid):
    """Devuelve TODOS los IDs de mail.message asociados a crm.lead (sin filtros temporales)."""
    return models.execute_kw(db, uid, password, 'mail.message', 'search',
                             [[('model', '=', 'crm.lead')]],
                             {'context': ODOO_CONTEXT})


def get_messages_data(models, uid, last_write_date=None):
    """Extrae mail.message filtrado por crm.lead con cursor incremental opcional."""
    logger.info("Obteniendo datos de mail.message para modelo crm.lead...")

    message_fields = [
        'id', 'subject', 'date', 'email_from', 'author_id', 'message_type',
        'subtype_id', 'is_internal', 'model', 'res_id', 'record_name',
        'parent_id', 'body', 'reply_to', 'reply_to_force_new', 'message_id',
        'mail_server_id', 'partner_ids', 'tracking_value_ids',
        'write_date', 'create_date',
    ]

    if last_write_date:
        domain = [
            '&',
            ('model', '=', 'crm.lead'),
            '|',
            ('write_date', '>', last_write_date),
            ('create_date', '>', last_write_date),
        ]
        logger.info(f"[INCREMENTAL messages] write_date OR create_date > {last_write_date}")
    else:
        domain = [('model', '=', 'crm.lead')]
        logger.info("[FULL messages] Todos los mensajes de crm.lead")

    message_ids = models.execute_kw(db, uid, password, 'mail.message', 'search',
                                    [domain], {'context': ODOO_CONTEXT})

    logger.info(f"Se encontraron {len(message_ids)} mensajes")

    if not message_ids:
        return []

    all_messages = []
    total_batches = len(list(create_batches(message_ids, BATCH_SIZE)))
    for i, batch in enumerate(create_batches(message_ids, BATCH_SIZE), 1):
        try:
            logger.info(f"Procesando lote {i}/{total_batches} de mensajes ({len(batch)} registros)...")
            batch_messages = models.execute_kw(db, uid, password, 'mail.message', 'read',
                                               [batch],
                                               {'fields': message_fields, 'context': ODOO_CONTEXT})
            all_messages.extend(batch_messages)
            if i < total_batches:
                time.sleep(SLEEP_BETWEEN_BATCHES)
        except Exception as e:
            logger.error(f"Error en lote {i} de mensajes: {e}")
            continue

    logger.info(f"Se obtuvieron {len(all_messages)} mensajes")
    return all_messages


def get_tracking_data(models, uid, crm_lead_message_ids, last_write_date=None):
    """Extrae mail.tracking.value para los message_ids dados, con cursor opcional."""
    if not crm_lead_message_ids:
        logger.warning("No hay message_ids de crm.lead para buscar tracking")
        return []

    logger.info(f"Obteniendo mail.tracking.value para {len(crm_lead_message_ids)} mensajes crm.lead...")

    tracking_fields = [
        'id', 'field_id', 'old_value_integer', 'old_value_float', 'old_value_char',
        'old_value_text', 'old_value_datetime', 'new_value_integer', 'new_value_float',
        'new_value_char', 'new_value_text', 'new_value_datetime', 'mail_message_id',
        'write_date', 'create_date',
    ]

    if last_write_date:
        domain = [
            '&',
            ('mail_message_id', 'in', crm_lead_message_ids),
            '|',
            ('write_date', '>', last_write_date),
            ('create_date', '>', last_write_date),
        ]
        logger.info(f"[INCREMENTAL tracking] write_date OR create_date > {last_write_date}")
    else:
        domain = [('mail_message_id', 'in', crm_lead_message_ids)]
        logger.info("[FULL tracking] Todos los tracking values de mensajes crm.lead")

    tracking_ids = models.execute_kw(db, uid, password, 'mail.tracking.value', 'search',
                                     [domain], {'context': ODOO_CONTEXT})

    logger.info(f"Se encontraron {len(tracking_ids)} valores de seguimiento")

    if not tracking_ids:
        return []

    all_tracking = []
    total_batches = len(list(create_batches(tracking_ids, BATCH_SIZE)))
    for i, batch in enumerate(create_batches(tracking_ids, BATCH_SIZE), 1):
        try:
            logger.info(f"Procesando lote {i}/{total_batches} de tracking values ({len(batch)} registros)...")
            batch_tracking = models.execute_kw(db, uid, password, 'mail.tracking.value', 'read',
                                               [batch],
                                               {'fields': tracking_fields, 'context': ODOO_CONTEXT})
            all_tracking.extend(batch_tracking)
            if i < total_batches:
                time.sleep(SLEEP_BETWEEN_BATCHES)
        except Exception as e:
            logger.error(f"Error en lote {i} de tracking values: {e}")
            continue

    logger.info(f"Se obtuvieron {len(all_tracking)} valores de seguimiento")
    return all_tracking


def get_odoo_crm_data():
    """Wrapper full-load (compatibilidad). Para sync_control usar las funciones dedicadas."""
    models, uid = odoo_connect()
    if not uid:
        return [], []
    messages = get_messages_data(models, uid, last_write_date=None)
    crm_msg_ids = [m['id'] for m in messages] if messages else []
    tracking = get_tracking_data(models, uid, crm_msg_ids, last_write_date=None)
    return messages, tracking

def transform_messages_for_bigquery(messages_data):
    """Transforma los datos de mail.message para BigQuery"""
    transformed_data = []
    extraction_time = datetime.utcnow().isoformat() + "Z"
    
    logger.info(f"Transformando {len(messages_data)} mensajes...")
    
    for i, message in enumerate(messages_data):
        try:
            # Procesar campos many2one
            author_info = extract_many2one_info(message.get('author_id'))
            subtype_info = extract_many2one_info(message.get('subtype_id'))
            parent_info = extract_many2one_info(message.get('parent_id'))
            mail_server_info = extract_many2one_info(message.get('mail_server_id'))
            
            # Partner IDs (many2many)
            partner_ids = message.get('partner_ids', [])
            partner_ids_str = ','.join(map(str, partner_ids)) if partner_ids and isinstance(partner_ids, list) else None
            
            # Tracking value IDs (many2many)
            tracking_value_ids = message.get('tracking_value_ids', [])
            tracking_value_ids_str = ','.join(map(str, tracking_value_ids)) if tracking_value_ids and isinstance(tracking_value_ids, list) else None
            
            row = {
                "message_id": safe_convert_to_int(message.get('id')),
                "subject": clean_text_for_bigquery(message.get('subject')),
                "message_date": convert_datetime_to_timestamp(message.get('date')),
                "email_from": clean_text_for_bigquery(message.get('email_from')),
                "author_id": author_info['id'],
                "author_name": clean_text_for_bigquery(author_info['name']),
                "message_type": clean_text_for_bigquery(message.get('message_type')),
                "subtype_id": subtype_info['id'],
                "subtype_name": clean_text_for_bigquery(subtype_info['name']),
                "is_internal": safe_convert_to_bool(message.get('is_internal')),
                "model": clean_text_for_bigquery(message.get('model')),
                "res_id": safe_convert_to_int(message.get('res_id')),
                "record_name": clean_text_for_bigquery(message.get('record_name')),
                "parent_id": parent_info['id'],
                "parent_name": clean_text_for_bigquery(parent_info['name']),
                "body": clean_text_for_bigquery(message.get('body')),
                "reply_to": clean_text_for_bigquery(message.get('reply_to')),
                "reply_to_force_new": safe_convert_to_bool(message.get('reply_to_force_new')),
                "odoo_message_id": clean_text_for_bigquery(message.get('message_id')),
                "mail_server_id": mail_server_info['id'],
                "mail_server_name": clean_text_for_bigquery(mail_server_info['name']),
                "partner_ids_count": len(partner_ids) if partner_ids and isinstance(partner_ids, list) else 0,
                "partner_ids_list": partner_ids_str,
                "tracking_value_ids_count": len(tracking_value_ids) if tracking_value_ids and isinstance(tracking_value_ids, list) else 0,
                "tracking_value_ids_list": tracking_value_ids_str,
                "extraction_date": extraction_time,
                "write_date": convert_datetime_to_timestamp(message.get('write_date')),
                "create_date": convert_datetime_to_timestamp(message.get('create_date')),
            }
            
            transformed_data.append(row)
            
            # Log de progreso cada 100 registros
            if (i + 1) % 100 == 0:
                logger.info(f"   Procesados {i + 1}/{len(messages_data)} mensajes...")
                
        except Exception as e:
            logger.warning(f"Error transformando mensaje {message.get('id', 'N/A')}: {e}")
            continue
    
    logger.info(f"Transformación de mensajes completada: {len(transformed_data)} registros listos")
    return transformed_data

def transform_tracking_for_bigquery(tracking_data):
    """Transforma los datos de mail.tracking.value para BigQuery"""
    transformed_data = []
    extraction_time = datetime.utcnow().isoformat() + "Z"
    
    logger.info(f"Transformando {len(tracking_data)} tracking values...")
    
    for i, tracking in enumerate(tracking_data):
        try:
            # Procesar campos many2one
            field_info = extract_many2one_info(tracking.get('field_id'))
            mail_message_info = extract_many2one_info(tracking.get('mail_message_id'))
            
            row = {
                "tracking_id": safe_convert_to_int(tracking.get('id')),
                "field_id": field_info['id'],
                "field_name": clean_text_for_bigquery(field_info['name']),
                "old_value_integer": safe_convert_to_int(tracking.get('old_value_integer')),
                "old_value_float": safe_convert_to_float(tracking.get('old_value_float')),
                "old_value_char": clean_text_for_bigquery(tracking.get('old_value_char')),
                "old_value_text": clean_text_for_bigquery(tracking.get('old_value_text')),
                "old_value_datetime": convert_datetime_to_timestamp(tracking.get('old_value_datetime')),
                "new_value_integer": safe_convert_to_int(tracking.get('new_value_integer')),
                "new_value_float": safe_convert_to_float(tracking.get('new_value_float')),
                "new_value_char": clean_text_for_bigquery(tracking.get('new_value_char')),
                "new_value_text": clean_text_for_bigquery(tracking.get('new_value_text')),
                "new_value_datetime": convert_datetime_to_timestamp(tracking.get('new_value_datetime')),
                "mail_message_id": mail_message_info['id'],
                "mail_message_name": clean_text_for_bigquery(mail_message_info['name']),
                "extraction_date": extraction_time,
                "write_date": convert_datetime_to_timestamp(tracking.get('write_date')),
                "create_date": convert_datetime_to_timestamp(tracking.get('create_date')),
            }
            
            transformed_data.append(row)
            
            # Log de progreso cada 100 registros
            if (i + 1) % 100 == 0:
                logger.info(f"   Procesados {i + 1}/{len(tracking_data)} tracking values...")
                
        except Exception as e:
            logger.warning(f"Error transformando tracking value {tracking.get('id', 'N/A')}: {e}")
            continue
    
    logger.info(f"Transformación de tracking values completada: {len(transformed_data)} registros listos")
    return transformed_data

def init_bigquery_client():
    """Inicializa el cliente de BigQuery"""
    try:
        credentials, _ = google.auth.default()
        client = bigquery.Client(credentials=credentials, project=project_id)
        logger.info(f"Cliente BigQuery inicializado para proyecto: {project_id}")
        return client
    except Exception as e:
        logger.error(f"Error inicializando BigQuery: {e}")
        return None

def get_messages_schema():
    """Retorna el esquema de BigQuery para mensajes"""
    return [
        bigquery.SchemaField("message_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("subject", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("message_date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("email_from", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("author_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("author_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("message_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("subtype_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("subtype_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("is_internal", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("model", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("res_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("record_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("parent_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("parent_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("body", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("reply_to", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("reply_to_force_new", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("odoo_message_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("mail_server_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mail_server_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("partner_ids_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("partner_ids_list", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("tracking_value_ids_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("tracking_value_ids_list", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("extraction_date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("write_date", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("create_date", "DATETIME", mode="NULLABLE"),
    ]


def get_tracking_schema():
    """Retorna el esquema de BigQuery para tracking values"""
    return [
        bigquery.SchemaField("tracking_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("field_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("field_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("old_value_integer", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("old_value_float", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("old_value_char", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("old_value_text", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("old_value_datetime", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("new_value_integer", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("new_value_float", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("new_value_char", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("new_value_text", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("new_value_datetime", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("mail_message_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mail_message_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("extraction_date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("write_date", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("create_date", "DATETIME", mode="NULLABLE"),
    ]


def load_and_merge(client, data, schema, table_final, table_tmp, label,
                   is_incremental=False, pk_field='id'):
    """Carga datos a tabla temporal y luego MERGE a tabla final.
    is_incremental=True omite el DELETE-not-in-temp (huerfanos via reconcile FULL).
    Devuelve (success, inserts, updates).
    """
    if not data:
        logger.warning(f"No hay datos de {label} para cargar")
        return True, 0, 0

    logger.info(f"[LOAD] Cargando {len(data):,} registros de {label} a tabla temporal...")

    df = pd.DataFrame(data)

    # Convertir columnas TIMESTAMP/DATETIME de string a datetime para pyarrow
    for field in schema:
        if field.name not in df.columns:
            continue
        if field.field_type == "TIMESTAMP":
            df[field.name] = pd.to_datetime(df[field.name], errors='coerce', utc=True)
        elif field.field_type == "DATETIME":
            df[field.name] = pd.to_datetime(df[field.name], errors='coerce')
            try:
                df[field.name] = df[field.name].dt.tz_localize(None)
            except (TypeError, AttributeError):
                pass

    temp_full = f"{project_id}.{dataset_id}.{table_tmp}"
    final_full = f"{project_id}.{dataset_id}.{table_final}"

    # Paso 1: Cargar a tabla temporal
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
        autodetect=False,
    )

    try:
        job = client.load_table_from_dataframe(df, temp_full, job_config=job_config)
        job.result()
        logger.info(f"[OK] {len(data):,} registros de {label} cargados en tabla temporal")
    except Exception as e:
        logger.error(f"[ERROR] Error cargando tabla temporal de {label}: {e}")
        return False, 0, 0

    inserts = len(data)
    updates = 0

    # Paso 2: MERGE temporal -> final (con condicional FULL/incremental)
    try:
        try:
            client.get_table(final_full)
            target_exists = True
        except Exception:
            target_exists = False

        if not target_exists:
            logger.info(f"[CREATE] Tabla final {label} no existe, creando desde temporal")
            create_query = f"CREATE TABLE `{final_full}` AS SELECT * FROM `{temp_full}`"
            client.query(create_query).result()
            final_table = client.get_table(final_full)
            logger.info(f"[OK] Tabla {label} creada: {final_table.num_rows:,} registros")
            return True, inserts, 0

        # Telemetry inserts/updates
        try:
            updates = count_existing_matches(client, temp_full, final_full, pk_field)
            inserts = max(0, len(data) - updates)
        except Exception:
            pass

        merge_start = time.time()
        final_table = client.get_table(final_full)
        temp_table = client.get_table(temp_full)
        final_cols = {f.name for f in final_table.schema}
        temp_cols = {f.name for f in temp_table.schema}
        common_cols = sorted(final_cols & temp_cols)
        update_cols = [c for c in common_cols if c != pk_field]

        update_str = ', '.join(f'T.`{c}` = S.`{c}`' for c in update_cols)
        insert_cols_str = ', '.join(f'`{c}`' for c in common_cols)
        insert_vals_str = ', '.join(f'S.`{c}`' for c in common_cols)

        if is_incremental:
            logger.info(f"[MERGE {label}] INCREMENTAL: solo upsert (huerfanos via reconcile FULL)")
        else:
            logger.info(f"[MERGE {label}] FULL: upsert + eliminar obsoletos")

        merge_query = f"""
        MERGE `{final_full}` T
        USING `{temp_full}` S
        ON T.`{pk_field}` = S.`{pk_field}`
        WHEN MATCHED THEN
            UPDATE SET {update_str}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols_str})
            VALUES ({insert_vals_str})
        """
        client.query(merge_query).result()
        logger.info(f"   [MERGE {label}] Upsert completado (inserts:{inserts:,} updates:{updates:,})")

        if not is_incremental:
            delete_query = f"""
            DELETE FROM `{final_full}` T
            WHERE T.`{pk_field}` NOT IN (SELECT `{pk_field}` FROM `{temp_full}`)
            """
            client.query(delete_query).result()
            logger.info(f"   [DELETE {label}] Registros obsoletos eliminados")

        merge_elapsed = time.time() - merge_start
        final_table = client.get_table(final_full)
        logger.info(f"[SUCCESS] {label}: {final_table.num_rows:,} filas ({merge_elapsed:.1f}s)")
        return True, inserts, updates

    except Exception as e:
        logger.error(f"[ERROR] Error en MERGE {label}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False, inserts, updates
    finally:
        client.delete_table(temp_full, not_found_ok=True)
        logger.info(f"[CLEANUP {label}] Tabla temporal eliminada")

# =========================================================
# SYNC CONTROL + RECONCILE
# =========================================================

def parse_sync_start(value):
    if not value:
        return None
    value = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(value, fmt)
            if fmt == "%Y-%m-%d":
                dt = dt.replace(hour=0, minute=0, second=0)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    raise ValueError(f"Formato invalido: '{value}'. Usa YYYY-MM-DD o YYYY-MM-DD HH:MM:SS")


def ensure_sync_control_table(client):
    table_ref = client.dataset(dataset_id).table(table_sync_control)
    schema = [
        bigquery.SchemaField("execution_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("start_time", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("end_time", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("records_processed", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("records_inserted", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("records_updated", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("last_odoo_write_date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
    ]
    try:
        client.get_table(table_ref)
    except Exception:
        client.create_table(bigquery.Table(table_ref, schema=schema))
        logger.info(f"   Tabla control {table_sync_control} creada")


def get_last_target_max_write_date(client, target_table):
    try:
        table_ref = f"{project_id}.{dataset_id}.{target_table}"
        for row in client.query(f"SELECT MAX(write_date) AS mwd FROM `{table_ref}`").result():
            wd = row.mwd
            if not wd:
                return None
            if hasattr(wd, 'strftime'):
                safe = wd - timedelta(minutes=SYNC_SAFETY_MARGIN_MINUTES)
                return safe.strftime('%Y-%m-%d %H:%M:%S')
            try:
                parsed = datetime.strptime(str(wd)[:19], '%Y-%m-%d %H:%M:%S')
                safe = parsed - timedelta(minutes=SYNC_SAFETY_MARGIN_MINUTES)
                return safe.strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                return str(wd)[:19]
        return None
    except Exception:
        return None


def get_last_successful_sync(client, target_table):
    q = f"""
    SELECT last_odoo_write_date
    FROM `{project_id}.{dataset_id}.{table_sync_control}`
    WHERE table_name = '{target_table}'
      AND status = 'SUCCESS'
      AND last_odoo_write_date IS NOT NULL
    ORDER BY end_time DESC LIMIT 1
    """
    try:
        for row in client.query(q).result():
            if row.last_odoo_write_date:
                safe = row.last_odoo_write_date - timedelta(minutes=SYNC_SAFETY_MARGIN_MINUTES)
                return safe.strftime("%Y-%m-%d %H:%M:%S"), "control_table"
    except Exception:
        pass
    fallback = get_last_target_max_write_date(client, target_table)
    if fallback:
        return fallback, "target_table_max"
    return None, "full_load"


def register_sync_execution(client, target_table, status, records_processed,
                             records_inserted, records_updated,
                             last_write_date, error_message, start_time_dt):
    row = {
        "execution_id": str(uuid.uuid4()),
        "table_name": target_table,
        "start_time": start_time_dt.isoformat(),
        "end_time": datetime.now().isoformat(),
        "records_processed": records_processed,
        "records_inserted": records_inserted,
        "records_updated": records_updated,
        "last_odoo_write_date": last_write_date,
        "status": status,
        "error_message": (error_message[:500] if error_message else None),
    }
    ctrl_full = f"{project_id}.{dataset_id}.{table_sync_control}"
    try:
        errors = client.insert_rows_json(ctrl_full, [row])
        if errors:
            logger.warning(f"   sync_control [{target_table}] errors: {errors}")
        else:
            logger.info(f"   sync_control [{target_table}]: status={status} cursor={last_write_date}")
    except Exception as e:
        logger.warning(f"   Excepcion sync_control [{target_table}]: {e}")


def count_existing_matches(client, temp_full, target_full, pk_field='id'):
    q = (f"SELECT COUNT(*) AS c FROM `{target_full}` T "
         f"WHERE T.`{pk_field}` IN (SELECT `{pk_field}` FROM `{temp_full}`)")
    try:
        for row in client.query(q).result():
            return row.c
    except Exception:
        pass
    return 0


def track_max_write_date(records, current_max):
    for r in records:
        wd = r.get('write_date')
        if wd is None or wd is False:
            continue
        if hasattr(wd, 'strftime'):
            wd = wd.strftime('%Y-%m-%d %H:%M:%S')
        elif not isinstance(wd, str):
            continue
        if current_max is None or wd > current_max:
            current_max = wd
    return current_max


def reconcile_messages_full(client, models, uid):
    """Reconciliacion FULL para mail.message: compara IDs vivos en Odoo (crm.lead)."""
    target_ref = f"{project_id}.{dataset_id}.{table_messages}"
    try:
        odoo_ids = models.execute_kw(db, uid, password, 'mail.message', 'search',
                                     [[('model', '=', 'crm.lead')]],
                                     {'context': ODOO_CONTEXT})
        odoo_set = set(odoo_ids)
        logger.info(f"[RECONCILE messages] Odoo: {len(odoo_set):,} IDs vivos")
    except Exception as e:
        logger.warning(f"[RECONCILE messages] No se pudo obtener IDs de Odoo: {e}")
        return 0
    try:
        bq_set = {row.message_id for row in client.query(f"SELECT message_id FROM `{target_ref}`").result()}
        logger.info(f"[RECONCILE messages] BQ: {len(bq_set):,} IDs en tabla")
    except Exception as e:
        logger.warning(f"[RECONCILE messages] No se pudo leer IDs de BQ: {e}")
        return 0
    obsolete = bq_set - odoo_set
    if not obsolete:
        return 0
    deleted = 0
    obsolete_list = list(obsolete)
    for i in range(0, len(obsolete_list), 5000):
        batch = obsolete_list[i:i + 5000]
        ids_str = ','.join(str(x) for x in batch)
        try:
            client.query(f"DELETE FROM `{target_ref}` WHERE message_id IN ({ids_str})").result()
            deleted += len(batch)
        except Exception as e:
            logger.warning(f"[RECONCILE messages] Error eliminando lote: {e}")
    logger.info(f"[RECONCILE messages] Eliminados {deleted:,} huerfanos")
    return deleted


def reconcile_tracking_full(client, models, uid, crm_lead_message_ids):
    """Reconciliacion FULL para mail.tracking.value: compara IDs vivos en Odoo
    (filtrados por mensajes de crm.lead)."""
    target_ref = f"{project_id}.{dataset_id}.{table_tracking}"
    if not crm_lead_message_ids:
        logger.info("[RECONCILE tracking] Sin mensajes crm.lead, skip")
        return 0
    try:
        odoo_ids = models.execute_kw(db, uid, password, 'mail.tracking.value', 'search',
                                     [[('mail_message_id', 'in', crm_lead_message_ids)]],
                                     {'context': ODOO_CONTEXT})
        odoo_set = set(odoo_ids)
        logger.info(f"[RECONCILE tracking] Odoo: {len(odoo_set):,} IDs vivos")
    except Exception as e:
        logger.warning(f"[RECONCILE tracking] No se pudo obtener IDs de Odoo: {e}")
        return 0
    try:
        bq_set = {row.tracking_id for row in client.query(f"SELECT tracking_id FROM `{target_ref}`").result()}
        logger.info(f"[RECONCILE tracking] BQ: {len(bq_set):,} IDs en tabla")
    except Exception as e:
        logger.warning(f"[RECONCILE tracking] No se pudo leer IDs de BQ: {e}")
        return 0
    obsolete = bq_set - odoo_set
    if not obsolete:
        return 0
    deleted = 0
    obsolete_list = list(obsolete)
    for i in range(0, len(obsolete_list), 5000):
        batch = obsolete_list[i:i + 5000]
        ids_str = ','.join(str(x) for x in batch)
        try:
            client.query(f"DELETE FROM `{target_ref}` WHERE tracking_id IN ({ids_str})").result()
            deleted += len(batch)
        except Exception as e:
            logger.warning(f"[RECONCILE tracking] Error eliminando lote: {e}")
    logger.info(f"[RECONCILE tracking] Eliminados {deleted:,} huerfanos")
    return deleted


# =========================================================
# RUN ETL POR PARTE
# =========================================================

def run_part_messages(client, models, uid, full_load=False, force_from=None):
    """Procesa mail.message con sync_control. Devuelve (success, processed, raw_records)."""
    start_time_dt = datetime.now()
    total_processed = 0
    inserts = 0
    updates = 0
    cursor_used = None
    max_write_date = None

    logger.info("\n" + "=" * 60)
    logger.info("PARTE 1: PROCESANDO MAIL.MESSAGE (crm.lead)")
    logger.info("=" * 60)

    try:
        target_full = f"{project_id}.{dataset_id}.{table_messages}"
        try:
            client.get_table(target_full)
            table_exists = True
        except Exception:
            table_exists = False

        if not table_exists:
            full_load = True
            mode_desc = "FULL (primera vez)"
            is_incremental = False
        elif full_load:
            mode_desc = "FULL"
            is_incremental = False
            force_from = None
        elif force_from:
            cursor_used = force_from
            mode_desc = f"INCREMENTAL FORZADO desde {force_from}"
            is_incremental = True
        elif LOAD_MODE_MESSAGES == 'incremental':
            cursor_used, source = get_last_successful_sync(client, table_messages)
            if cursor_used is None:
                full_load = True
                is_incremental = False
                mode_desc = "FULL (sin sync previa)"
            else:
                is_incremental = True
                mode_desc = f"INCREMENTAL desde {cursor_used} ({source})"
        else:
            mode_desc = "FULL"
            is_incremental = False

        logger.info(f"[MODE messages] {mode_desc}")

        messages_raw = get_messages_data(models, uid,
                                          last_write_date=cursor_used if is_incremental else None)

        if not messages_raw:
            logger.info("[OK messages] Sin registros para procesar")
            register_sync_execution(client, table_messages, "SUCCESS", 0, 0, 0,
                                    cursor_used, None, start_time_dt)
            return True, 0, []

        max_write_date = track_max_write_date(messages_raw, max_write_date)
        total_processed = len(messages_raw)

        messages_transformed = transform_messages_for_bigquery(messages_raw)
        success, inserts, updates = load_and_merge(
            client, messages_transformed, get_messages_schema(),
            table_messages, table_messages_temp, "MENSAJES",
            is_incremental=is_incremental, pk_field='message_id',
        )
        if not success:
            raise RuntimeError("MERGE de mail.message fallo")

        if is_incremental:
            try:
                reconcile_messages_full(client, models, uid)
            except Exception as e:
                logger.warning(f"[RECONCILE messages] Fallo no critico: {e}")

        new_cursor = max_write_date or cursor_used or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        register_sync_execution(client, table_messages, "SUCCESS",
                                total_processed, inserts, updates,
                                new_cursor, None, start_time_dt)
        logger.info(f"[OK messages] {total_processed:,} registros (inserts:{inserts:,} updates:{updates:,})")
        return True, total_processed, messages_raw

    except Exception as e:
        logger.error(f"[ERROR PARTE 1 messages]: {e}")
        import traceback; logger.error(traceback.format_exc())
        try:
            register_sync_execution(client, table_messages, "ERROR",
                                    total_processed, inserts, updates,
                                    None, str(e), start_time_dt)
        except Exception:
            pass
        return False, total_processed, []


def run_part_tracking(client, models, uid, crm_lead_message_ids,
                      full_load=False, force_from=None):
    """Procesa mail.tracking.value con sync_control. Devuelve (success, processed)."""
    start_time_dt = datetime.now()
    total_processed = 0
    inserts = 0
    updates = 0
    cursor_used = None
    max_write_date = None

    logger.info("\n" + "=" * 60)
    logger.info("PARTE 2: PROCESANDO MAIL.TRACKING.VALUE")
    logger.info("=" * 60)

    try:
        target_full = f"{project_id}.{dataset_id}.{table_tracking}"
        try:
            client.get_table(target_full)
            table_exists = True
        except Exception:
            table_exists = False

        if not table_exists:
            full_load = True
            mode_desc = "FULL (primera vez)"
            is_incremental = False
        elif full_load:
            mode_desc = "FULL"
            is_incremental = False
            force_from = None
        elif force_from:
            cursor_used = force_from
            mode_desc = f"INCREMENTAL FORZADO desde {force_from}"
            is_incremental = True
        elif LOAD_MODE_TRACKING == 'incremental':
            cursor_used, source = get_last_successful_sync(client, table_tracking)
            if cursor_used is None:
                full_load = True
                is_incremental = False
                mode_desc = "FULL (sin sync previa)"
            else:
                is_incremental = True
                mode_desc = f"INCREMENTAL desde {cursor_used} ({source})"
        else:
            mode_desc = "FULL"
            is_incremental = False

        logger.info(f"[MODE tracking] {mode_desc}")

        tracking_raw = get_tracking_data(models, uid, crm_lead_message_ids,
                                         last_write_date=cursor_used if is_incremental else None)

        if not tracking_raw:
            logger.info("[OK tracking] Sin registros para procesar")
            register_sync_execution(client, table_tracking, "SUCCESS", 0, 0, 0,
                                    cursor_used, None, start_time_dt)
            return True, 0

        max_write_date = track_max_write_date(tracking_raw, max_write_date)
        total_processed = len(tracking_raw)

        tracking_transformed = transform_tracking_for_bigquery(tracking_raw)
        success, inserts, updates = load_and_merge(
            client, tracking_transformed, get_tracking_schema(),
            table_tracking, table_tracking_temp, "TRACKING VALUES",
            is_incremental=is_incremental, pk_field='tracking_id',
        )
        if not success:
            raise RuntimeError("MERGE de mail.tracking.value fallo")

        if is_incremental:
            try:
                reconcile_tracking_full(client, models, uid, crm_lead_message_ids)
            except Exception as e:
                logger.warning(f"[RECONCILE tracking] Fallo no critico: {e}")

        new_cursor = max_write_date or cursor_used or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        register_sync_execution(client, table_tracking, "SUCCESS",
                                total_processed, inserts, updates,
                                new_cursor, None, start_time_dt)
        logger.info(f"[OK tracking] {total_processed:,} registros (inserts:{inserts:,} updates:{updates:,})")
        return True, total_processed

    except Exception as e:
        logger.error(f"[ERROR PARTE 2 tracking]: {e}")
        import traceback; logger.error(traceback.format_exc())
        try:
            register_sync_execution(client, table_tracking, "ERROR",
                                    total_processed, inserts, updates,
                                    None, str(e), start_time_dt)
        except Exception:
            pass
        return False, total_processed


def run_etl(full_load=False, force_from=None):
    """ETL fusionado: mail.message + mail.tracking.value (manejo independiente)."""
    logger.info("=" * 80)
    logger.info("[START] ETL FUSIONADO: MAIL.MESSAGE + MAIL.TRACKING.VALUE (filtrado crm.lead)")
    logger.info(f"[INFO] Tablas: {table_messages}, {table_tracking}")
    logger.info(f"[INFO] Control: {table_sync_control}  Margen: {SYNC_SAFETY_MARGIN_MINUTES}min")
    logger.info("=" * 80)

    if not validate_environment_variables():
        raise RuntimeError("Variables de entorno faltantes")

    start_time = time.time()

    client = init_bigquery_client()
    if not client:
        logger.error("No se pudo conectar a BigQuery")
        return False

    ensure_sync_control_table(client)

    models, uid = odoo_connect()
    if not uid:
        return False

    # PARTE 1: mensajes (independiente)
    success_msg, processed_msg, _ = (False, 0, [])
    try:
        success_msg, processed_msg, _ = run_part_messages(client, models, uid, full_load, force_from)
    except Exception as e:
        logger.error(f"[ERROR] Error general PARTE 1: {e}")
        import traceback; logger.error(traceback.format_exc())

    # Para tracking necesitamos los IDs de mensajes crm.lead (independiente del incremental)
    try:
        all_crm_lead_msg_ids = get_crm_lead_message_ids(models, uid)
    except Exception as e:
        logger.error(f"[ERROR] No se pudieron obtener message_ids de crm.lead: {e}")
        all_crm_lead_msg_ids = []

    # PARTE 2: tracking values (independiente, corre aunque PARTE 1 falle)
    success_trk, processed_trk = (False, 0)
    try:
        success_trk, processed_trk = run_part_tracking(client, models, uid,
                                                       all_crm_lead_msg_ids,
                                                       full_load, force_from)
    except Exception as e:
        logger.error(f"[ERROR] Error general PARTE 2: {e}")
        import traceback; logger.error(traceback.format_exc())

    elapsed = time.time() - start_time
    logger.info("\n" + "=" * 80)
    logger.info("[SUMMARY] RESUMEN FINAL")
    logger.info(f"[TIME] Tiempo total: {elapsed:.0f}s ({elapsed/60:.1f} min)")
    logger.info(f"[TABLE] {table_messages}: {processed_msg:,} registros - {'OK' if success_msg else 'ERROR'}")
    logger.info(f"[TABLE] {table_tracking}: {processed_trk:,} registros - {'OK' if success_trk else 'ERROR'}")
    logger.info("[COMPLETED] ETL COMPLETADO")
    return success_msg and success_trk


def main():
    """Entry point para el orquestador. Modo incremental por defecto."""
    success = run_etl(full_load=False, force_from=None)
    if not success:
        raise RuntimeError("ETL mail_message+tracking fallo en alguna tabla")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ETL mail.message + mail.tracking.value -> BigQuery (sync_control)')
    parser.add_argument('--full', action='store_true', help='Carga completa (TRUNCATE + reload) para AMBAS tablas')
    parser.add_argument('--from', dest='force_from', type=str,
                        help="Incremental forzado desde: YYYY-MM-DD o YYYY-MM-DD HH:MM:SS (aplica a AMBAS)")
    parser.add_argument('--debug', action='store_true')
    args = parser.parse_args()

    if args.debug:
        os.environ['DEBUG_MODE'] = 'true'

    force_from = None
    if args.force_from:
        try:
            force_from = parse_sync_start(args.force_from)
        except ValueError as e:
            logger.error(f"[ERROR] {e}")
            sys.exit(1)

    success = run_etl(full_load=args.full, force_from=force_from)
    sys.exit(0 if success else 1)
