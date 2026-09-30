# -*- coding: utf-8 -*-
import xmlrpc.client
import os
from dotenv import load_dotenv
import ssl
import socket
from google.cloud import bigquery
from google.oauth2 import service_account
import google.auth
import json
import time
import pandas as pd
from datetime import datetime

import logging

script_dir = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(script_dir, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/project_task.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

# ==== CONFIGURACION ODOO ====
url = os.getenv('ODOO_URL')
db = os.getenv('ODOO_DB')
username = os.getenv('ODOO_USERNAME')
password = os.getenv('ODOO_API_KEY') or os.getenv('ODOO_PASSWORD')

# ==== CONFIGURACION GCP ====
project_id = os.getenv('GCP_PROJECT_ID', 'nebulan-476518')
dataset_id = os.getenv('GCP_DATASET_ID', 'nebulanodoo')
table_name = os.getenv('GCP_TABLE_NAME', 'prod_project_task')
table_temp = f"{table_name}_temp"

# ==== MODELO ODOO ====
ODOO_MODEL = 'project.task'

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

# ==== CONFIGURACION DE LOTES ====
BATCH_SIZE = 1000
SLEEP_AFTER_TABLE_OPS = 5
SLEEP_BETWEEN_BATCHES = 1

def create_batches(data, batch_size):
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]

def safe_convert_to_int(value):
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
    if value is None or value == '' or value == 'None':
        return None
    if isinstance(value, str):
        return value.lower() in ('true', '1', 'yes', 'on')
    return bool(value)

def safe_convert_to_datetime(value):
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
    return field_name.replace('.', '_').replace('-', '_')

def get_model_fields():
    logger.info(f"Conectando a Odoo para obtener campos de {ODOO_MODEL}...")

    if url.startswith('https'):
        ssl._create_default_https_context = ssl._create_unverified_context

    socket.setdefaulttimeout(120)

    try:
        common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', verbose=False)
        uid = common.authenticate(db, username, password, {})

        if not uid:
            logger.error("[ERROR] Error de autenticacion")
            return None, None, None

        logger.info("[OK] Conectado a Odoo")

    except Exception as e:
        logger.error(f"[ERROR] Error de conexion: {e}")
        return None, None, None

    try:
        models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', verbose=False)

        init_odoo_context(models, uid, ODOO_MODEL)

        fields_info = models.execute_kw(db, uid, password,
                                      ODOO_MODEL, 'fields_get',
                                      [], {'context': ODOO_CONTEXT})

        field_names = list(fields_info.keys())

        many2one_count = sum(1 for f in fields_info.values() if f.get('type') == 'many2one')
        logger.info(f"Total de campos detectados: {len(field_names)}")
        logger.info(f"Campos many2one (generaran columna adicional _name): {many2one_count}")

        return field_names, fields_info, (models, uid)

    except Exception as e:
        logger.error(f"[ERROR] Error accediendo a los campos: {e}")
        return None, None, None

def map_odoo_type_to_bigquery(odoo_type):
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
        'many2one': 'INTEGER',
        'one2many': 'STRING',
        'many2many': 'STRING',
    }
    return type_mapping.get(odoo_type, 'STRING')

def create_bigquery_schema(fields_info):
    schema = []
    used_names = set()

    for field_name, field_data in fields_info.items():
        odoo_type = field_data.get('type', 'char')
        bq_type = map_odoo_type_to_bigquery(odoo_type)
        
        safe_field_name = sanitize_field_name(field_name)
        
        if safe_field_name not in used_names:
            schema.append(bigquery.SchemaField(safe_field_name, bq_type, mode="NULLABLE"))
            used_names.add(safe_field_name)
        
        if odoo_type == 'many2one':
            name_field = f"{safe_field_name}_name"
            if name_field not in used_names:
                schema.append(bigquery.SchemaField(name_field, "STRING", mode="NULLABLE"))
                used_names.add(name_field)

    return schema

def process_many2one_value(value):
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
    if value is None or value == False:
        return None

    try:
        if field_type in ['char', 'text', 'html', 'selection', 'reference']:
            result = str(value) if value not in [False, None] else None
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
            return None

        elif field_type == 'binary':
            result = str(value) if value not in [False, None] else None
            return None if result == "" else result

        else:
            result = str(value) if value not in [False, None] else None
            return None if result == "" else result

    except Exception as e:
        logger.info(f"[!] Error procesando campo {field_name}: {e}")
        return None

def get_odoo_data():
    logger.info(f"Conectando a Odoo para extraer {ODOO_MODEL}...")

    field_names, fields_info, connection_data = get_model_fields()
    if not field_names:
        return [], None, None

    models, uid = connection_data

    try:
        search_domain = []
        record_ids = models.execute_kw(db, uid, password,
                                    ODOO_MODEL, 'search',
                                    [search_domain],
                                    {'context': ODOO_CONTEXT})

        logger.info(f"Procesando {len(record_ids)} registros...")

    except Exception as e:
        logger.error(f"[ERROR] Error accediendo a los datos: {e}")
        return [], None, None

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
            logger.error(f"[ERROR] Error en lote {i}: {e}")
            continue

    logger.info(f"[OK] Datos extraidos: {len(all_records)} registros")

    processed_records = []
    for record in all_records:
        row_dict = {}

        for field_name in field_names:
            value = record.get(field_name)
            field_info = fields_info[field_name]
            field_type = field_info.get('type', 'char')

            safe_field_name = sanitize_field_name(field_name)
            
            if field_type == 'many2one':
                field_id, field_name_value = process_many2one_value(value)
                row_dict[safe_field_name] = field_id
                row_dict[f"{safe_field_name}_name"] = field_name_value
            else:
                processed_value = process_field_value(field_name, value, field_type)
                row_dict[safe_field_name] = processed_value

        processed_records.append(row_dict)

    sanitized_fields_info = {}
    for field_name, field_data in fields_info.items():
        safe_field_name = sanitize_field_name(field_name)
        sanitized_fields_info[safe_field_name] = field_data
        
        if field_data.get('type') == 'many2one':
            sanitized_fields_info[f"{safe_field_name}_name"] = {'type': 'char', 'string': f"{field_data.get('string', '')} (Name)"}

    return processed_records, field_names, sanitized_fields_info

def get_bigquery_client():
    """Obtiene cliente de BigQuery"""
    credentials, _ = google.auth.default()
    return bigquery.Client(credentials=credentials, project=project_id)

def clean_dataframe_for_bigquery(df, fields_info):
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
                df_clean[col] = df_clean[col].apply(lambda x: str(x) if x is not None and x != '' else None)
                df_clean[col] = df_clean[col].replace(['nan', 'None', 'False', '<NA>', 'NaT'], None)
        else:
            df_clean[col] = df_clean[col].apply(lambda x: str(x) if x is not None and x != '' else None)
            df_clean[col] = df_clean[col].replace(['nan', 'None', 'False', '<NA>', 'NaT'], None)

    return df_clean

def load_data_to_table(client, data, fields_info):
    """Carga datos a tabla temporal y reemplaza tabla final"""
    if not data:
        logger.info("[!] No hay datos para cargar")
        return

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

    # Paso 2: Reemplazar tabla final (atómico en BigQuery)
    try:
        logger.info("[REPLACE] Reemplazando tabla final...")
        merge_start = time.time()

        replace_query = f"""
        CREATE OR REPLACE TABLE `{final_full}` AS
        SELECT * FROM `{temp_full}`
        """
        client.query(replace_query).result()

        merge_elapsed = time.time() - merge_start

        table = client.get_table(final_full)
        logger.info(f"[SUCCESS] Reemplazo EXITOSO")
        logger.info(f"   - Registros en tabla: {table.num_rows:,}")
        logger.info(f"   - Tiempo: {merge_elapsed:.1f}s")

    except Exception as e:
        logger.error(f"[ERROR] Error reemplazando tabla: {e}")
        raise
    finally:
        client.delete_table(temp_full, not_found_ok=True)
        logger.info(f"[CLEANUP] Tabla temporal eliminada")

def main():
    """Entry point para el orquestador."""
    logger.info(f"ETL {ODOO_MODEL} -> BIGQUERY")
    logger.info(f"Destino: {project_id}.{dataset_id}.{table_name}")

    required_vars = ['ODOO_URL', 'ODOO_DB', 'ODOO_USERNAME']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if not os.getenv('ODOO_API_KEY') and not os.getenv('ODOO_PASSWORD'):
        missing_vars.append('ODOO_API_KEY o ODOO_PASSWORD')

    if missing_vars:
        logger.error(f"[ERROR] Variables faltantes: {missing_vars}")
        raise RuntimeError(f"Variables faltantes: {missing_vars}")

    logger.info("=== EXTRACCION ===")
    records_data, field_names, fields_info = get_odoo_data()

    if not records_data:
        logger.error("[ERROR] Sin datos")
        return

    logger.info("=== CARGA ===")
    client = get_bigquery_client()
    load_data_to_table(client, records_data, fields_info)

    name_columns_count = len([f for f in fields_info.keys() if f.endswith('_name')])

    logger.info("=== COMPLETADO ===")
    logger.info(f"   Registros: {len(records_data)}")
    logger.info(f"   Campos Odoo: {len(field_names)}")
    logger.info(f"   Columnas _name agregadas: {name_columns_count}")
    logger.info(f"   Total columnas: {len(fields_info)}")
    logger.info(f"   Tabla: {project_id}.{dataset_id}.{table_name}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
