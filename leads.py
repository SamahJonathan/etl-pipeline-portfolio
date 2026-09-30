import xmlrpc.client
import os
import sys
import uuid
import argparse
from dotenv import load_dotenv
import ssl
import socket
import time
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
import google.auth
from google.api_core import exceptions as gcp_exceptions
from datetime import datetime, timedelta
import json

import logging

script_dir = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(script_dir, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/leads.log", encoding='utf-8'),
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

# ==== CONFIGURACIÓN GCP NEBULAN ====
project_id = os.getenv('GCP_PROJECT_ID', 'nebulan-476518')
dataset_id = os.getenv('GCP_DATASET_ID', 'nebulanodoo')
table_leads = os.getenv('GCP_TABLE_NAME', 'prod_crm_lead')
table_leads_temp = f"{table_leads}_temp"
table_sync_control = os.getenv('GCP_TABLE_SYNC_CONTROL', 'etl_sync_control')
SYNC_SAFETY_MARGIN_MINUTES = 5
DEBUG_MODE = os.getenv('DEBUG_MODE', 'false').lower() == 'true'

# ==== CONFIGURACIÓN DE LOTES ====
BATCH_SIZE = 2000
SLEEP_AFTER_TABLE_OPS = 5
SLEEP_BETWEEN_BATCHES = 1
ODOO_MODEL = 'crm.lead'
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
        return 0.0
    try:
        if isinstance(value, str):
            value = value.strip()
            if value == '' or value.lower() == 'none' or value.lower() == 'false':
                return 0.0
        return float(value)
    except (ValueError, TypeError):
        return 0.0

def safe_convert_to_bool(value):
    """Convierte un valor a boolean de forma segura"""
    if value is None or value == '' or value == 'None':
        return False
    if isinstance(value, str):
        return value.lower() in ('true', '1', 'yes', 'on')
    return bool(value)

def validate_configuration():
    """Valida que la configuración sea consistente para Nebulan"""
    logger.info("VALIDANDO CONFIGURACIÓN NEBULAN")
    logger.info("="*50)
    
    # Verificar .env
    if not all([url, db, username, password]):
        logger.error("ERROR: Variables de entorno faltantes en .env")
        return False
    
    logger.info(f"ODOO_URL: {url}")
    logger.info(f" ODOO_DB: {db}")
    logger.info(f"ODOO_USERNAME: {username}")
    
    # Verificar que sea la VM correcta
    if 'nebulan' not in url.lower():
        logger.warning(f"URL no parece ser de Nebulan: {url}")
    else:
        logger.info(f"URL Nebulan detectada")
    
    logger.info(f"Project configurado: {project_id}")
    logger.info(f"Configuración Nebulan validada")
    return True

def get_available_lead_fields(models, db, uid, password):
    """Obtiene los campos disponibles en crm.lead y selecciona solo los que existen"""
    logger.info("Verificando campos disponibles en crm.lead...")
    
    try:
        available_fields = models.execute_kw(db, uid, password,
                                           'crm.lead', 'fields_get', [],
                                           {'context': ODOO_CONTEXT})
        
        # Campos que queremos (validación dinámica)
        desired_fields = [
            'id', 'name', 'partner_id', 'partner_name', 'contact_name', 'email_from', 
            'phone', 'mobile', 'website', 'street', 'city', 'country_id',
            'company_id', 'user_id', 'team_id', 'stage_id', 'priority',
            'probability', 'expected_revenue', 'planned_revenue', 'recurring_revenue',
            'active', 'type', 'source_id', 'medium_id', 'campaign_id',
            'date_open', 'date_closed', 'date_deadline', 'date_conversion',
            'create_date', 'write_date', 'description', 'tag_ids'
        ]
        
        # Filtrar solo campos que existen
        valid_fields = []
        missing_fields = []
        
        for field in desired_fields:
            if field in available_fields:
                valid_fields.append(field)
            else:
                missing_fields.append(field)
        
        logger.info(f"Campos disponibles: {len(valid_fields)}")
        logger.warning(f"Campos no disponibles: {len(missing_fields)}")
        
        if missing_fields:
            logger.info(f"   Campos faltantes: {', '.join(missing_fields[:5])}{'...' if len(missing_fields) > 5 else ''}")
        
        return valid_fields, available_fields
        
    except Exception as e:
        logger.error(f"Error obteniendo campos: {e}")
        return [], {}

def create_dynamic_schema(valid_fields, available_fields):
    """Crea esquema BigQuery dinámico"""
    schema = [bigquery.SchemaField("id", "INTEGER", mode="REQUIRED")]
    
    field_type_mapping = {
        'char': 'STRING',
        'text': 'STRING', 
        'html': 'STRING',
        'integer': 'INTEGER',
        'float': 'FLOAT',
        'monetary': 'FLOAT',
        'boolean': 'BOOLEAN',
        'date': 'DATE',
        'datetime': 'TIMESTAMP',
        'selection': 'STRING',
        'many2one': 'BOTH',
        'many2many': 'STRING',
        'one2many': 'STRING'
    }
    
    many2one_fields = {}
    
    for field_name in valid_fields:
        if field_name == 'id':
            continue
            
        field_info = available_fields.get(field_name, {})
        field_type = field_info.get('type', 'char')
        
        if field_type == 'many2one':
            name_field = field_name.replace('_id', '') if field_name.endswith('_id') else f"{field_name}_name"
            id_field = f"{field_name}_value"
            
            schema.append(bigquery.SchemaField(name_field, "STRING", mode="NULLABLE"))
            schema.append(bigquery.SchemaField(id_field, "INTEGER", mode="NULLABLE"))
            many2one_fields[field_name] = (name_field, id_field)
            
        elif field_type in ['many2many', 'one2many']:
            schema.append(bigquery.SchemaField(field_name, "STRING", mode="NULLABLE"))
            
        else:
            # Manejo robusto de fechas
            if field_name in ['date_open', 'date_closed', 'date_deadline', 'date_conversion']:
                bq_type = 'DATE'
            elif field_name in ['create_date', 'write_date']:
                bq_type = 'TIMESTAMP'
            else:
                bq_type = field_type_mapping.get(field_type, 'STRING')
            
            schema.append(bigquery.SchemaField(field_name, bq_type, mode="NULLABLE"))
    
    return schema, many2one_fields

def get_odoo_leads_data(last_write_date=None):
    """Obtiene datos de CRM leads de Odoo. Si last_write_date, filtra incremental.
    Devuelve (all_leads, valid_fields, available_fields, models, uid)."""
    
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
        return [], {}, {}, None, None

    logger.info(f"Autenticación exitosa. UID: {uid}")
    
    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', verbose=False)

    init_odoo_context(models, uid, ODOO_MODEL)

    valid_fields, available_fields = get_available_lead_fields(models, db, uid, password)

    if not valid_fields:
        logger.error("No se pudieron obtener campos válidos")
        return [], {}, {}, models, uid

    logger.info(f"Obteniendo datos de crm.lead (TODOS: leads + oportunidades)...")

    if last_write_date:
        domain = [
            '|',
            ('write_date', '>', last_write_date),
            ('create_date', '>', last_write_date),
        ]
        logger.info(f"[INCREMENTAL] write_date OR create_date > {last_write_date}")
    else:
        domain = []
        logger.info("[FULL] Buscando todos los registros (incluyendo inactivos)")

    lead_ids = models.execute_kw(db, uid, password,
                                'crm.lead', 'search',
                                [domain], {'context': ODOO_CONTEXT})
    
    logger.info(f"Se encontraron {len(lead_ids)} leads TOTALES (incluyendo inactivos)")
    
    # ✅ DIAGNÓSTICO: Mostrar distribución por tipo
    try:
        type_count = models.execute_kw(db, uid, password,
                                      'crm.lead', 'read_group',
                                      [[], ['type'], ['type']], 
                                      {'context': ODOO_CONTEXT})
        
        logger.info(f"Distribución por tipo:")
        for group in type_count:
            logger.info(f"   {group['type']}: {group['type_count']} registros")
    except Exception as e:
        logger.warning(f"No se pudo obtener distribución por tipo: {e}")
    
    # ✅ DIAGNÓSTICO: Mostrar distribución por estado activo
    try:
        active_count = models.execute_kw(db, uid, password,
                                        'crm.lead', 'read_group',
                                        [[], ['active'], ['active']], 
                                        {'context': ODOO_CONTEXT})
        
        logger.info(f"Distribución por estado:")
        for group in active_count:
            state = "Activos" if group['active'] else "Inactivos" 
            logger.info(f"   {state}: {group['active_count']} registros")
    except Exception as e:
        logger.warning(f"No se pudo obtener distribución por estado: {e}")
    
    # Procesamiento por lotes
    all_leads = []
    batch_size = BATCH_SIZE
    total_batches = len(list(create_batches(lead_ids, batch_size)))
    
    logger.info(f"Procesando en {total_batches} lotes de {batch_size}...")
    
    for i, leads_batch in enumerate(create_batches(lead_ids, batch_size), 1):
        try:
            logger.info(f"Procesando lote {i}/{total_batches} ({len(leads_batch)} registros)...")
            batch_leads = models.execute_kw(db, uid, password,
                                           'crm.lead', 'read',
                                           [leads_batch],
                                           {'fields': valid_fields, 'context': ODOO_CONTEXT})
            all_leads.extend(batch_leads)
            
            if i < total_batches:
                time.sleep(SLEEP_BETWEEN_BATCHES)
                
        except Exception as e:
            logger.error(f"Error en lote {i} de leads: {e}")
            continue
    
    logger.info(f"Se obtuvieron {len(all_leads)} leads/oportunidades")

    return all_leads, valid_fields, available_fields, models, uid

def process_leads_for_bigquery(leads, valid_fields, available_fields):
    """Procesa leads para BigQuery - ETL limpio sin campos calculados"""
    
    logger.info("Procesando leads para BigQuery...")
    
    bigquery_leads = []
    many2one_fields = {}
    
    # Crear mapeo de many2one
    for field_name in valid_fields:
        field_info = available_fields.get(field_name, {})
        field_type = field_info.get('type', 'char')
        
        if field_type == 'many2one':
            name_field = field_name.replace('_id', '') if field_name.endswith('_id') else f"{field_name}_name"
            id_field = f"{field_name}_value"
            many2one_fields[field_name] = (name_field, id_field)
    
    for i, lead in enumerate(leads):
        row_dict = {}
        
        # Procesar campos originales
        for field_name in valid_fields:
            value = lead.get(field_name)
            field_info = available_fields.get(field_name, {})
            field_type = field_info.get('type', 'char')
            
            if field_name == 'id':
                row_dict[field_name] = safe_convert_to_int(value)
                
            elif field_type == 'many2one':
                if field_name in many2one_fields:
                    name_field, id_field = many2one_fields[field_name]
                    if isinstance(value, list) and len(value) >= 2:
                        row_dict[name_field] = str(value[1])
                        row_dict[id_field] = safe_convert_to_int(value[0])
                    else:
                        row_dict[name_field] = ""
                        row_dict[id_field] = None
                        
            elif field_type in ['many2many', 'one2many']:
                if value and isinstance(value, list):
                    row_dict[field_name] = ', '.join([str(v) for v in value])
                else:
                    row_dict[field_name] = ""
                    
            elif field_type == 'datetime' or field_name in ['create_date', 'write_date']:
                if value not in [False, None]:
                    row_dict[field_name] = str(value)
                else:
                    row_dict[field_name] = None
                
            elif field_type == 'date' or field_name in ['date_open', 'date_closed', 'date_deadline', 'date_conversion']:
                if value not in [False, None]:
                    row_dict[field_name] = str(value)
                else:
                    row_dict[field_name] = None
                
            elif field_type in ['float', 'monetary']:
                row_dict[field_name] = safe_convert_to_float(value)
                
            elif field_type == 'boolean':
                row_dict[field_name] = safe_convert_to_bool(value)
                
            elif field_type == 'integer':
                row_dict[field_name] = safe_convert_to_int(value)
                
            else:  # char, text, selection, etc.
                row_dict[field_name] = str(value) if value not in [False, None] else ""
        
        bigquery_leads.append(row_dict)
        
        if (i + 1) % 500 == 0:
            logger.info(f"   Procesados {i + 1}/{len(leads)}...")
    
    logger.info(f"{len(bigquery_leads)} leads procesados")
    return bigquery_leads

def get_bigquery_client():
    """Obtiene cliente de BigQuery"""
    credentials, _ = google.auth.default()
    return bigquery.Client(credentials=credentials, project=project_id)

def clean_dataframe_for_bigquery(df):
    """Limpia DataFrame para BigQuery"""
    df_clean = df.copy()
    
    for col in df_clean.columns:
        if col in ['date_open', 'date_closed', 'date_deadline', 'date_conversion']:
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce').dt.date
        elif col in ['create_date', 'write_date']:
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
        elif col.endswith('_value') or col == 'id':
            df_clean[col] = df_clean[col].apply(safe_convert_to_int)
            df_clean[col] = df_clean[col].astype('Int64')
        elif col in ['probability', 'expected_revenue', 'planned_revenue', 'recurring_revenue']:
            df_clean[col] = df_clean[col].apply(safe_convert_to_float)
        elif col == 'active':
            df_clean[col] = df_clean[col].apply(safe_convert_to_bool)
        else:
            df_clean[col] = df_clean[col].astype(str)
            df_clean[col] = df_clean[col].replace(['nan', 'None', 'False', '<NA>'], '')
    
    return df_clean

def load_leads_to_bigquery(client, leads_data, schema, is_incremental=False):
    """Carga leads a tabla temporal y luego MERGE a tabla final.
    is_incremental=True omite el DELETE-not-in-temp (huerfanos via reconcile FULL).
    Devuelve (success, inserts, updates)."""
    if not leads_data:
        logger.warning("No hay datos de leads para cargar")
        return True, 0, 0

    logger.info(f"[LOAD] Cargando {len(leads_data)} leads a tabla temporal...")

    df = pd.DataFrame(leads_data)
    df = clean_dataframe_for_bigquery(df)

    temp_full = f"{project_id}.{dataset_id}.{table_leads_temp}"
    final_full = f"{project_id}.{dataset_id}.{table_leads}"

    # Paso 1: Cargar a tabla temporal
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
        autodetect=False,
    )

    try:
        job = client.load_table_from_dataframe(df, temp_full, job_config=job_config)
        job.result()
        logger.info(f"[OK] {len(leads_data)} leads cargados en tabla temporal")
    except Exception as e:
        logger.error(f"[ERROR] Error cargando tabla temporal: {e}")
        raise

    inserts = len(leads_data)
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
            logger.info(f"[OK] Tabla {table_leads} creada")
            client.delete_table(temp_full, not_found_ok=True)
            logger.info(f"[CLEANUP] Tabla temporal eliminada")
            return True, inserts, updates

        # Telemetry inserts/updates
        try:
            updates = count_existing_matches(client, temp_full, final_full)
            inserts = max(0, len(leads_data) - updates)
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
        merge_start = time.time()

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

        # En modo FULL eliminar registros obsoletos
        if not is_incremental:
            delete_query = f"""
            DELETE FROM `{final_full}` T
            WHERE T.id NOT IN (SELECT id FROM `{temp_full}`)
            """
            client.query(delete_query).result()
            logger.info("   [DELETE] Registros obsoletos eliminados")

        merge_elapsed = time.time() - merge_start

        # Verificar resultado
        table = client.get_table(final_full)
        logger.info(f"[SUCCESS] MERGE EXITOSO")
        logger.info(f"   - Registros en tabla: {table.num_rows:,}")
        logger.info(f"   - Tiempo MERGE: {merge_elapsed:.1f}s")
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
    try:
        table_ref = f"{project_id}.{dataset_id}.{table_leads}"
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


def get_last_successful_sync(client):
    q = f"""
    SELECT last_odoo_write_date
    FROM `{project_id}.{dataset_id}.{table_sync_control}`
    WHERE table_name = '{table_leads}'
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
        "table_name": table_leads,
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
    target_ref = f"{project_id}.{dataset_id}.{table_leads}"
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
    logger.info(f"[INFO] Tabla destino: {project_id}.{dataset_id}.{table_leads}")
    logger.info(f"[INFO] Control: {table_sync_control}  Margen: {SYNC_SAFETY_MARGIN_MINUTES}min")
    logger.info("="*80)

    if not validate_configuration():
        logger.warning("Configuración con advertencias, pero continuando...")

    try:
        client = get_bigquery_client()
        ensure_sync_control_table(client)

        target_full = f"{project_id}.{dataset_id}.{table_leads}"
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

        leads_data, valid_fields, available_fields, models, uid = get_odoo_leads_data(
            last_write_date=cursor_used if is_incremental else None
        )

        if not leads_data:
            logger.info("[OK] Sin registros para procesar")
            register_sync_execution(client, "SUCCESS", 0, 0, 0,
                                    cursor_used, None, start_time_dt)
            return True

        max_write_date = track_max_write_date(leads_data, max_write_date)
        total_processed = len(leads_data)

        processed_leads = process_leads_for_bigquery(leads_data, valid_fields, available_fields)
        if not processed_leads:
            logger.error("No se pudieron procesar los leads")
            register_sync_execution(client, "ERROR", total_processed, 0, 0,
                                    None, "process_leads vacio", start_time_dt)
            return False

        schema, _ = create_dynamic_schema(valid_fields, available_fields)
        success, inserts, updates = load_leads_to_bigquery(
            client, processed_leads, schema, is_incremental=is_incremental
        )
        if not success:
            raise RuntimeError("MERGE fallo")

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
        logger.info(f"[STATS] Procesados: {total_processed:,} (inserts:{inserts:,} updates:{updates:,})")
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
