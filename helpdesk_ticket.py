import xmlrpc.client
import os
import sys
import uuid
import argparse
from dotenv import load_dotenv
import ssl
import socket
from google.cloud import bigquery
from google.oauth2 import service_account
import google.auth
import json
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Cargar variables de entorno
import logging

script_dir = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(script_dir, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/helpdesk_ticket.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

# ==== CONFIGURACIÓN ODOO ====
url = os.getenv('ODOO_URL')
db = os.getenv('ODOO_DB')
username = os.getenv('ODOO_USERNAME')
password = os.getenv('ODOO_API_KEY') or os.getenv('ODOO_PASSWORD')

# ==== CONFIGURACIÓN GCP ====
project_id = os.getenv('GCP_PROJECT_ID', 'nebulan-476518')
dataset_id = os.getenv('GCP_DATASET_ID', 'nebulanodoo')
table_name = os.getenv('GCP_TABLE_NAME', 'prod_helpdesk_ticket')
table_temp = f"{table_name}_temp"
table_sync_control = os.getenv('GCP_TABLE_SYNC_CONTROL', 'etl_sync_control')
SYNC_SAFETY_MARGIN_MINUTES = 5
DEBUG_MODE = os.getenv('DEBUG_MODE', 'false').lower() == 'true'

# ==== MODELO ODOO ====
ODOO_MODEL = 'helpdesk.ticket'

# ==== CONFIGURACIÓN DE LOTES ====
BATCH_SIZE = 1000
SLEEP_AFTER_TABLE_OPS = 5
SLEEP_BETWEEN_BATCHES = 1
LOAD_MODE = 'incremental'

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

def create_batches(data, batch_size):
    """Divide una lista en lotes del tamaño especificado"""
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]

def safe_convert_to_int(value):
    """Convierte un valor a entero de forma segura"""
    if value is None or value == '' or value == 'False' or value == False:
        return None
    try:
        if isinstance(value, str):
            value = value.strip()
            if value == '' or value.lower() == 'none' or value.lower() == 'false':
                return None
        return int(float(value))
    except (ValueError, TypeError):
        return None

def safe_convert_to_float(value):
    """Convierte un valor a float de forma segura"""
    if value is None or value == '' or value == 'False' or value == False:
        return None
    try:
        if isinstance(value, str):
            value = value.strip()
            if value == '' or value.lower() == 'none' or value.lower() == 'false':
                return None
        return float(value)
    except (ValueError, TypeError):
        return None

def safe_convert_to_bool(value):
    """Convierte un valor a boolean de forma segura"""
    if value is None or value == '' or value == 'None':
        return None
    if isinstance(value, str):
        return value.lower() in ('true', '1', 'yes', 'on')
    return bool(value)

def safe_convert_to_datetime(value):
    """Convierte un valor a datetime de forma segura para BigQuery"""
    if value is None or value == '' or value == 'False' or value == False:
        return None

    try:
        if isinstance(value, str):
            value = value.strip()
            if value == '' or value.lower() == 'none' or value.lower() == 'false':
                return None

            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y %H:%M:%S', '%m/%d/%Y %H:%M:%S']:
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue

            parsed_datetime = pd.to_datetime(value, errors='coerce')
            if pd.notna(parsed_datetime):
                return parsed_datetime.to_pydatetime()

        return None
    except Exception:
        return None

def safe_convert_to_date(value):
    """Convierte un valor a date de forma segura para BigQuery"""
    if value is None or value == '' or value == 'False' or value == False:
        return None

    try:
        if isinstance(value, str):
            value = value.strip()
            if value == '' or value.lower() == 'none' or value.lower() == 'false':
                return None

            for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y']:
                try:
                    return datetime.strptime(value, fmt).date()
                except ValueError:
                    continue

            parsed_date = pd.to_datetime(value, errors='coerce')
            if pd.notna(parsed_date):
                return parsed_date.date()

        return None
    except Exception:
        return None

def sanitize_field_name(field_name):
    """Sanitiza nombre de campo para BigQuery"""
    return field_name.replace('.', '_').replace('-', '_')

def get_model_fields():
    """Obtiene TODOS los campos del modelo helpdesk.ticket sin exclusiones"""
    logger.info(f" Conectando a Odoo para obtener campos de {ODOO_MODEL}...")

    if url.startswith('https'):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

    socket.setdefaulttimeout(120)

    try:
        common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', verbose=False)
        uid = common.authenticate(db, username, password, {})

        if not uid:
            logger.error("Error de autenticación")
            return None, None, None

        logger.info("Conectado a Odoo")

    except Exception as e:
        logger.error(f"Error de conexión: {e}")
        return None, None, None

    try:
        models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', verbose=False)

        init_odoo_context(models, uid, ODOO_MODEL)

        # Obtener TODOS los campos sin exclusiones
        fields_info = models.execute_kw(db, uid, password,
                                      ODOO_MODEL, 'fields_get',
                                      [], {'context': ODOO_CONTEXT})

        field_names = list(fields_info.keys())

        # Contar campos many2one
        many2one_count = sum(1 for f in fields_info.values() if f.get('type') == 'many2one')
        logger.info(f"Total de campos detectados: {len(field_names)}")
        logger.info(f"Campos many2one (generarán columna adicional _name): {many2one_count}")

        return field_names, fields_info, (models, uid)

    except Exception as e:
        logger.error(f"Error accediendo a los campos: {e}")
        return None, None, None

def map_odoo_type_to_bigquery(odoo_type):
    """Mapea tipos de Odoo a tipos de BigQuery manteniendo equivalencia"""
    type_mapping = {
        'char': 'STRING',
        'text': 'STRING',
        'html': 'STRING',
        'integer': 'INTEGER',
        'float': 'FLOAT64',
        'monetary': 'FLOAT64',
        'boolean': 'BOOLEAN',
        'date': 'DATE',
        'datetime': 'DATETIME',
        'binary': 'STRING',
        'selection': 'STRING',
        'reference': 'STRING',
        'many2one': 'INTEGER',      # ID numérico
        'one2many': 'STRING',       # JSON array de IDs
        'many2many': 'STRING',      # JSON array de IDs
    }
    return type_mapping.get(odoo_type, 'STRING')

def create_bigquery_schema(fields_info):
    """Crea el esquema de BigQuery basado en los campos de Odoo"""
    schema = []
    used_names = set()  # Evitar duplicados

    for field_name, field_data in fields_info.items():
        odoo_type = field_data.get('type', 'char')
        bq_type = map_odoo_type_to_bigquery(odoo_type)
        
        safe_field_name = sanitize_field_name(field_name)
        
        # Campo principal (evitar duplicados)
        if safe_field_name not in used_names:
            schema.append(bigquery.SchemaField(safe_field_name, bq_type, mode="NULLABLE"))
            used_names.add(safe_field_name)
        
        # Para many2one: agregar campo adicional con el nombre (evitar duplicados)
        if odoo_type == 'many2one':
            name_field = f"{safe_field_name}_name"
            if name_field not in used_names:
                schema.append(bigquery.SchemaField(name_field, "STRING", mode="NULLABLE"))
                used_names.add(name_field)

    return schema

def process_many2one_value(value):
    """Procesa un campo many2one y retorna (id, name)"""
    if value is None or value == False:
        return None, None
    
    if isinstance(value, (list, tuple)) and len(value) >= 2:
        return safe_convert_to_int(value[0]), str(value[1])
    elif isinstance(value, (list, tuple)) and len(value) == 1:
        return safe_convert_to_int(value[0]), None
    elif isinstance(value, int):
        return value, None
    
    return None, None

def process_field_value(field_name, value, field_type):
    """Procesa un valor de campo según su tipo de Odoo"""
    if value is None or value == False:
        return None

    try:
        if field_type in ['char', 'text', 'html', 'selection', 'reference']:
            result = str(value) if value not in [False, None] else None
            # "" → NULL
            return None if result == "" else result

        elif field_type == 'integer':
            return safe_convert_to_int(value)

        elif field_type in ['float', 'monetary']:
            return safe_convert_to_float(value)

        elif field_type == 'boolean':
            return safe_convert_to_bool(value)

        elif field_type == 'date':
            return safe_convert_to_date(value)

        elif field_type == 'datetime':
            return safe_convert_to_datetime(value)

        elif field_type in ['one2many', 'many2many']:
            if isinstance(value, list) and len(value) > 0:
                return json.dumps(value)
            # [] → NULL
            return None

        elif field_type == 'binary':
            result = str(value) if value not in [False, None] else None
            return None if result == "" else result

        else:
            result = str(value) if value not in [False, None] else None
            return None if result == "" else result

    except Exception as e:
        logger.warning(f"Error procesando campo {field_name}: {e}")
        return None

def get_odoo_data(last_write_date=None):
    """Obtiene datos del modelo de Odoo. Si last_write_date, filtra incremental.
    Devuelve (records, field_names, fields_info, models, uid, raw_records)."""
    logger.info(f" Conectando a Odoo para extraer {ODOO_MODEL}...")

    field_names, fields_info, connection_data = get_model_fields()
    if not field_names:
        return [], None, None, None, None, []

    models, uid = connection_data

    try:
        if last_write_date:
            search_domain = [
                '|',
                ('write_date', '>', last_write_date),
                ('create_date', '>', last_write_date),
            ]
            logger.info(f"[INCREMENTAL] write_date OR create_date > {last_write_date}")
        else:
            search_domain = []
            logger.info("[FULL] Todos los registros")

        record_ids = models.execute_kw(db, uid, password,
                                    ODOO_MODEL, 'search',
                                    [search_domain],
                                    {'context': ODOO_CONTEXT})

        logger.info(f"Procesando {len(record_ids)} registros...")

    except Exception as e:
        logger.error(f"Error accediendo a los datos: {e}")
        return [], None, None, None, None, []

    all_records = []
    batch_size = 2000

    id_batches = list(create_batches(record_ids, batch_size))
    total_batches = len(id_batches)

    for i, ids_batch in enumerate(id_batches, 1):
        try:
            logger.info(f"  Lote {i}/{total_batches} ({len(ids_batch)} registros)...")
            batch_records = models.execute_kw(db, uid, password,
                                           ODOO_MODEL, 'read',
                                           [ids_batch],
                                           {'fields': field_names, 'context': ODOO_CONTEXT})
            all_records.extend(batch_records)

            if i < total_batches:
                time.sleep(SLEEP_BETWEEN_BATCHES)

        except Exception as e:
            logger.error(f"Error en lote {i}: {e}")
            continue

    logger.info(f"Datos extraídos: {len(all_records)} registros")

    # Procesar datos para BigQuery
    processed_records = []
    for record in all_records:
        row_dict = {}

        for field_name in field_names:
            value = record.get(field_name)
            field_info = fields_info[field_name]
            field_type = field_info.get('type', 'char')

            safe_field_name = sanitize_field_name(field_name)
            
            # Manejo especial para many2one: crear dos columnas
            if field_type == 'many2one':
                field_id, field_name_value = process_many2one_value(value)
                row_dict[safe_field_name] = field_id
                row_dict[f"{safe_field_name}_name"] = field_name_value
            else:
                processed_value = process_field_value(field_name, value, field_type)
                row_dict[safe_field_name] = processed_value

        processed_records.append(row_dict)

    # Crear fields_info sanitizado incluyendo campos _name
    sanitized_fields_info = {}
    for field_name, field_data in fields_info.items():
        safe_field_name = sanitize_field_name(field_name)
        sanitized_fields_info[safe_field_name] = field_data
        
        # Agregar entrada para campo _name de many2one
        if field_data.get('type') == 'many2one':
            sanitized_fields_info[f"{safe_field_name}_name"] = {'type': 'char', 'string': f"{field_data.get('string', '')} (Name)"}

    return processed_records, field_names, sanitized_fields_info, models, uid, all_records

def get_bigquery_client():
    """Obtiene cliente de BigQuery"""
    credentials, _ = google.auth.default()
    return bigquery.Client(credentials=credentials, project=project_id)

def clean_dataframe_for_bigquery(df, fields_info):
    """Limpia DataFrame para BigQuery"""
    df_clean = df.copy()

    for col in df_clean.columns:
        if col in fields_info:
            field_type = fields_info[col].get('type', 'char')

            if field_type == 'integer':
                df_clean[col] = df_clean[col].apply(safe_convert_to_int)
                df_clean[col] = df_clean[col].astype('Int64')
            
            elif field_type == 'many2one':
                df_clean[col] = df_clean[col].apply(safe_convert_to_int)
                df_clean[col] = df_clean[col].astype('Int64')
            
            elif field_type == 'date':
                df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce').dt.date
            
            elif field_type == 'datetime':
                df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
            
            elif field_type in ['float', 'monetary']:
                df_clean[col] = df_clean[col].apply(safe_convert_to_float)
            
            elif field_type == 'boolean':
                df_clean[col] = df_clean[col].apply(safe_convert_to_bool)
            
            else:
                # String fields (incluye los campos _name)
                df_clean[col] = df_clean[col].apply(lambda x: str(x) if x is not None and x != '' else None)
                df_clean[col] = df_clean[col].replace(['nan', 'None', 'False', '<NA>', 'NaT'], None)
        else:
            df_clean[col] = df_clean[col].apply(lambda x: str(x) if x is not None and x != '' else None)
            df_clean[col] = df_clean[col].replace(['nan', 'None', 'False', '<NA>', 'NaT'], None)

    return df_clean

def load_data_to_table(client, data, fields_info, is_incremental=False):
    """Carga datos a tabla temporal y luego MERGE a tabla final.
    is_incremental=False -> hace DELETE-not-in-temp (FULL). True omite ese DELETE.
    Devuelve (success, inserts, updates)."""
    if not data:
        logger.warning("No hay datos para cargar")
        return True, 0, 0

    logger.info(f"[LOAD] Cargando {len(data)} registros a tabla temporal...")

    df = pd.DataFrame(data)
    df = clean_dataframe_for_bigquery(df, fields_info)

    schema = create_bigquery_schema(fields_info)
    temp_full = f"{project_id}.{dataset_id}.{table_temp}"
    final_full = f"{project_id}.{dataset_id}.{table_name}"

    # Paso 1: Cargar a tabla temporal
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
        autodetect=False,
    )

    try:
        job = client.load_table_from_dataframe(df, temp_full, job_config=job_config)
        job.result()
        logger.info(f"[OK] {len(data)} registros cargados en tabla temporal")
    except Exception as e:
        logger.error(f"[ERROR] Error cargando tabla temporal: {e}")
        raise

    inserts = len(data)
    updates = 0
    # Paso 2: MERGE temporal -> final
    try:
        # Si la tabla final no existe, crearla desde la temporal
        try:
            client.get_table(final_full)
        except Exception:
            logger.info("[MERGE] Tabla final no existe, creándola desde temporal")
            create_query = f"""
            CREATE TABLE `{final_full}` AS
            SELECT * FROM `{temp_full}`
            """
            client.query(create_query).result()
            logger.info(f"[OK] Tabla {table_name} creada")
            client.delete_table(temp_full, not_found_ok=True)
            logger.info(f"[CLEANUP] Tabla temporal eliminada")
            return True, inserts, updates

        # Telemetry inserts/updates
        try:
            updates = count_existing_matches(client, temp_full, final_full)
            inserts = max(0, len(data) - updates)
        except Exception:
            pass

        # Obtener columnas comunes
        final_table = client.get_table(final_full)
        temp_table = client.get_table(temp_full)
        final_cols = {f.name for f in final_table.schema}
        temp_cols = {f.name for f in temp_table.schema}
        common_cols = sorted(final_cols & temp_cols)
        update_cols = [c for c in common_cols if c != 'id']

        update_str = ', '.join(f'T.`{c}` = S.`{c}`' for c in update_cols)
        insert_cols_str = ', '.join(f'`{c}`' for c in common_cols)
        insert_vals_str = ', '.join(f'S.`{c}`' for c in common_cols)

        if is_incremental:
            logger.info("[MERGE] Modo INCREMENTAL: solo upsert (huerfanos via reconcile FULL)")
        else:
            logger.info("[MERGE] Modo FULL: MERGE upsert + eliminar obsoletos")

        merge_query = f"""
        MERGE `{final_full}` T
        USING `{temp_full}` S
        ON T.id = S.id
        WHEN MATCHED THEN
            UPDATE SET {update_str}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols_str})
            VALUES ({insert_vals_str})
        """
        client.query(merge_query).result()
        logger.info(f"   [MERGE] Upsert completado (inserts:{inserts:,} updates:{updates:,})")

        # En modo FULL eliminar registros que ya no existen en Odoo
        if not is_incremental:
            delete_query = f"""
            DELETE FROM `{final_full}` T
            WHERE T.id NOT IN (SELECT id FROM `{temp_full}`)
            """
            client.query(delete_query).result()
            logger.info("   [DELETE] Registros obsoletos eliminados")

        table = client.get_table(final_full)
        logger.info(f"[SUCCESS] MERGE EXITOSO - {table.num_rows:,} filas en tabla")
        return True, inserts, updates

    except Exception as e:
        logger.error(f"[ERROR] Error en MERGE: {e}")
        raise
    finally:
        client.delete_table(temp_full, not_found_ok=True)
        logger.info(f"[CLEANUP] Tabla temporal eliminada")

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


def get_last_target_max_write_date(client):
    """Fallback: MAX(write_date) del target con margen de seguridad."""
    try:
        table_ref = f"{project_id}.{dataset_id}.{table_name}"
        query = f"SELECT MAX(write_date) AS mwd FROM `{table_ref}`"
        for row in client.query(query).result():
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


def get_last_successful_sync(client):
    q = f"""
    SELECT last_odoo_write_date
    FROM `{project_id}.{dataset_id}.{table_sync_control}`
    WHERE table_name = '{table_name}'
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
    fallback = get_last_target_max_write_date(client)
    if fallback:
        return fallback, "target_table_max"
    return None, "full_load"


def register_sync_execution(client, status, records_processed, records_inserted,
                             records_updated, last_write_date, error_message,
                             start_time_dt):
    row = {
        "execution_id": str(uuid.uuid4()),
        "table_name": table_name,
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
            logger.warning(f"   sync_control errors: {errors}")
        else:
            logger.info(f"   sync_control: status={status} cursor={last_write_date}")
    except Exception as e:
        logger.warning(f"   Excepcion sync_control: {e}")


def count_existing_matches(client, temp_full, target_full):
    q = f"SELECT COUNT(*) AS c FROM `{target_full}` T WHERE T.id IN (SELECT id FROM `{temp_full}`)"
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


def reconcile_full_with_odoo(client, models, uid):
    target_ref = f"{project_id}.{dataset_id}.{table_name}"
    try:
        odoo_ids = models.execute_kw(db, uid, password, ODOO_MODEL, 'search', [[]],
                                     {'context': ODOO_CONTEXT})
        odoo_set = set(odoo_ids)
        logger.info(f"[RECONCILE] Odoo: {len(odoo_set):,} IDs vivos")
    except Exception as e:
        logger.warning(f"[RECONCILE] No se pudo obtener IDs de Odoo: {e}")
        return 0
    try:
        bq_set = {row.id for row in client.query(f"SELECT id FROM `{target_ref}`").result()}
        logger.info(f"[RECONCILE] BQ: {len(bq_set):,} IDs en tabla")
    except Exception as e:
        logger.warning(f"[RECONCILE] No se pudo leer IDs de BQ: {e}")
        return 0
    obsolete = bq_set - odoo_set
    if not obsolete:
        logger.info("[RECONCILE] Sin huerfanos")
        return 0
    deleted = 0
    obsolete_list = list(obsolete)
    for i in range(0, len(obsolete_list), 5000):
        batch = obsolete_list[i:i + 5000]
        ids_str = ','.join(str(x) for x in batch)
        try:
            client.query(f"DELETE FROM `{target_ref}` WHERE id IN ({ids_str})").result()
            deleted += len(batch)
        except Exception as e:
            logger.warning(f"[RECONCILE] Error eliminando lote: {e}")
    logger.info(f"[RECONCILE] Eliminados {deleted:,} huerfanos")
    return deleted


def run_etl(full_load=False, force_from=None):
    """ETL principal con patron sync_control."""
    start_time_dt = datetime.now()
    start_time = time.time()
    total_processed = 0
    inserts = 0
    updates = 0
    cursor_used = None
    max_write_date = None
    client = None

    logger.info("="*80)
    logger.info(f"[START] ETL: {ODOO_MODEL}")
    logger.info(f"[INFO] Tabla destino: {project_id}.{dataset_id}.{table_name}")
    logger.info(f"[INFO] Control: {table_sync_control}  Margen: {SYNC_SAFETY_MARGIN_MINUTES}min")
    logger.info("="*80)

    required_vars = ['ODOO_URL', 'ODOO_DB', 'ODOO_USERNAME']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if not (os.getenv('ODOO_API_KEY') or os.getenv('ODOO_PASSWORD')):
        missing_vars.append('ODOO_API_KEY o ODOO_PASSWORD')
    if missing_vars:
        logger.error(f"[ERROR] Variables faltantes: {missing_vars}")
        raise RuntimeError(f"Variables faltantes: {missing_vars}")

    try:
        client = get_bigquery_client()
        ensure_sync_control_table(client)

        target_full = f"{project_id}.{dataset_id}.{table_name}"
        try:
            client.get_table(target_full)
            table_exists = True
        except Exception:
            table_exists = False

        if not table_exists:
            full_load = True
            logger.info("[MODE] Tabla destino no existe -> FULL LOAD forzado")
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
        elif LOAD_MODE == 'incremental':
            cursor_used, source = get_last_successful_sync(client)
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

        logger.info(f"[MODE] {mode_desc}")

        records_data, field_names, fields_info, models, uid, raw_records = get_odoo_data(
            last_write_date=cursor_used if is_incremental else None
        )

        if not records_data:
            logger.info("[OK] Sin registros para procesar")
            register_sync_execution(client, "SUCCESS", 0, 0, 0,
                                    cursor_used, None, start_time_dt)
            return True

        max_write_date = track_max_write_date(raw_records, max_write_date)
        total_processed = len(records_data)

        success, inserts, updates = load_data_to_table(
            client, records_data, fields_info, is_incremental=is_incremental
        )
        if not success:
            raise RuntimeError("MERGE fallo")

        # Reconcile FULL en incremental
        if is_incremental and models and uid:
            try:
                reconcile_full_with_odoo(client, models, uid)
            except Exception as e:
                logger.warning(f"[RECONCILE] Fallo no critico: {e}")

        new_cursor = max_write_date or cursor_used or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        register_sync_execution(client, "SUCCESS", total_processed, inserts, updates,
                                new_cursor, None, start_time_dt)

        elapsed = time.time() - start_time
        logger.info("="*80)
        logger.info(f"[OK] ETL {mode_desc} COMPLETADO en {elapsed:.0f}s ({elapsed/60:.1f} min)")
        logger.info(f"[STATS] Procesados: {total_processed:,}  (inserts: {inserts:,}, updates: {updates:,})")
        logger.info(f"[CURSOR] {new_cursor}")
        logger.info("="*80)
        return True

    except KeyboardInterrupt:
        logger.warning("[ABORT] Interrumpido")
        if client:
            register_sync_execution(client, "ERROR", total_processed, inserts, updates,
                                    None, "KeyboardInterrupt", start_time_dt)
        return False
    except Exception as e:
        logger.error(f"[ERROR] Error critico: {e}")
        import traceback; logger.error(traceback.format_exc())
        if client:
            register_sync_execution(client, "ERROR", total_processed, inserts, updates,
                                    None, str(e), start_time_dt)
        return False


def main():
    """Entry point para el orquestador. Modo incremental por defecto."""
    success = run_etl(full_load=False, force_from=None)
    if not success:
        raise RuntimeError(f"ETL {ODOO_MODEL} fallo")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=f'ETL {ODOO_MODEL} -> BigQuery (sync_control)')
    parser.add_argument('--full', action='store_true', help='Carga completa (TRUNCATE + reload)')
    parser.add_argument('--from', dest='force_from', type=str,
                        help="Incremental forzado desde: YYYY-MM-DD o YYYY-MM-DD HH:MM:SS")
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
