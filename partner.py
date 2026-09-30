"""
ETL para res.partner
Extrae todos los partners desde Odoo y los carga a BigQuery (carga FULL)
Compatible con Windows y Linux
Genera tabla: prod_res_partner

Pipeline: Odoo -> extraer lote 2000 -> transformar -> cargar a BQ (WRITE_TRUNCATE)

Reglas de transformación:
- many2one: [0] = _id (INTEGER), [1] = _name (STRING)
- fechas: DATETIME
- boolean: True/False (no tratar False como nulo)
- ids (one2many/many2many): lista sin corchetes como STRING
- vacíos (None, '', []) = NULL
- False booleano NO es nulo, es False
"""

import xmlrpc.client
import os
from dotenv import load_dotenv
import ssl
import socket
from google.cloud import bigquery
import google.auth
import time
import pandas as pd
import requests
import logging
import gc
import sys
import uuid
import argparse
from datetime import datetime, timedelta

# ============================================================================
# CONFIGURACIÓN DE LOGGING
# ============================================================================
script_dir = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(script_dir, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/partner.log", encoding='utf-8'),
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
table_name = os.getenv('GCP_TABLE_NAME', 'prod_res_partner')
table_temp = f"{table_name}_temp"
table_sync_control = os.getenv('GCP_TABLE_SYNC_CONTROL', 'etl_sync_control')
SYNC_SAFETY_MARGIN_MINUTES = 5
DEBUG_MODE = os.getenv('DEBUG_MODE', 'false').lower() == 'true'

# ==== CONFIGURACIÓN GENERAL ====
ODOO_MODEL = 'res.partner'
BATCH_SIZE_ODOO = 2000
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

# ============================================================================
# CAMPOS A EXTRAER
# ============================================================================
ODOO_FIELDS = [
    'id', 'name', 'create_date', 'vat', 'l10n_latam_identification_type_id',
    'l10n_cl_activity_description', 'category_id', 'tenant_code',
    'user_id', 'team_id', 'property_payment_term_id',
    'property_product_pricelist', 'buyer_id', 'property_supplier_payment_term_id',
    'property_purchase_currency_id', 'property_account_receivable_id',
    'property_account_payable_id', 'credit', 'day_sales_outstanding',
    'use_partner_credit_limit', 'ubl_cii_format', 'l10n_cl_sii_taxpayer_type',
    'company_id', 'company_name', 'parent_id',
]


# ============================================================================
# FUNCIONES UTILITARIAS
# ============================================================================
def get_xmlrpc_proxies():
    if url.startswith('https'):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    else:
        context = None

    socket.setdefaulttimeout(300)

    common = xmlrpc.client.ServerProxy(
        f'{url}/xmlrpc/2/common', verbose=False, context=context, allow_none=True
    )
    models = xmlrpc.client.ServerProxy(
        f'{url}/xmlrpc/2/object', verbose=False, context=context, allow_none=True
    )
    return common, models


def jsonrpc_call(service, method, args):
    """JSON-RPC para llamadas donde XML-RPC falla por None en Odoo 18"""
    endpoint = f"{url}/jsonrpc"
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {"service": service, "method": method, "args": args},
        "id": 1
    }
    response = requests.post(endpoint, json=payload,
                             headers={"Content-Type": "application/json"},
                             verify=False, timeout=300)
    result = response.json()
    if "error" in result:
        raise Exception(result["error"].get("data", {}).get("message", str(result["error"])))
    return result.get("result")


def get_bigquery_client():
    from google.oauth2 import service_account

    creds_path = os.getenv('GOOGLE_CREDENTIALS_PATH')

    if not creds_path or not os.path.exists(creds_path):
        local_creds = os.path.join(script_dir, 'credenciales.json')
        if os.path.exists(local_creds):
            creds_path = local_creds

    if not creds_path or not os.path.exists(creds_path):
        linux_creds = "/home/kpbchile/airflow/dags/credenciales.json"
        if os.path.exists(linux_creds):
            creds_path = linux_creds

    if creds_path and os.path.exists(creds_path):
        credentials = service_account.Credentials.from_service_account_file(
            creds_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        logger.info(f"[AUTH] Credenciales BQ desde: {creds_path}")
        return bigquery.Client(credentials=credentials, project=project_id)

    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    credentials, _ = google.auth.default(scopes=scopes)
    if not credentials.scopes and hasattr(credentials, 'with_scopes'):
        credentials = credentials.with_scopes(scopes)
    logger.info("[AUTH] Credenciales BQ desde ADC (Application Default Credentials)")
    return bigquery.Client(credentials=credentials, project=project_id)


def sanitize_field_name(field_name):
    return field_name.replace('.', '_').replace('-', '_')


# ============================================================================
# MAPEO DE TIPOS ODOO -> BIGQUERY
# ============================================================================
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


# ============================================================================
# OBTENER ESTRUCTURA DE CAMPOS DESDE ODOO
# ============================================================================
def get_model_fields():
    logger.info(f"[FIELDS] Obteniendo estructura de campos de {ODOO_MODEL}...")

    try:
        common, models = get_xmlrpc_proxies()

        version = common.version()
        logger.info(f"[OK] Conexion establecida. Version Odoo: {version.get('server_version', 'N/A')}")

        uid = common.authenticate(db, username, password, {})
        if not uid:
            logger.error("[ERROR] Error de autenticacion")
            return None, None, None, None
        logger.info(f"[OK] Autenticacion exitosa. UID: {uid}")

        init_odoo_context(models, uid, ODOO_MODEL)

    except Exception as e:
        logger.error(f"[ERROR] Error de conexion: {e}")
        return None, None, None, None

    try:
        fields_info = jsonrpc_call("object", "execute_kw",
                                   [db, uid, password, ODOO_MODEL, 'fields_get', [],
                                    {'context': ODOO_CONTEXT}])

        all_fields = fields_info
        fields_info = {k: v for k, v in all_fields.items() if k in ODOO_FIELDS}
        field_names = list(fields_info.keys())

        missing = set(ODOO_FIELDS) - set(all_fields.keys())
        if missing:
            logger.warning(f"[WARNING] Campos no encontrados en Odoo: {missing}")

        many2one_count = sum(1 for f in fields_info.values() if f.get('type') == 'many2one')
        expected_cols = len(field_names) + many2one_count

        logger.info(f"[OK] Campos a extraer: {len(field_names)} (de {len(all_fields)} disponibles)")
        logger.info(f"   many2one: {many2one_count} (generaran columna _name adicional)")
        logger.info(f"   Total columnas BQ esperadas: {expected_cols}")

        return field_names, fields_info, uid, models

    except Exception as e:
        logger.error(f"[ERROR] Error obteniendo campos: {e}")
        return None, None, None, None


# ============================================================================
# CREAR ESQUEMA BIGQUERY DINÁMICO
# ============================================================================
def create_bigquery_schema(fields_info):
    schema = []
    used_names = set()

    for field_name, field_data in fields_info.items():
        odoo_type = field_data.get('type', 'char')
        bq_type = map_odoo_type_to_bigquery(odoo_type)
        safe_name = sanitize_field_name(field_name)

        if safe_name not in used_names:
            schema.append(bigquery.SchemaField(safe_name, bq_type, mode="NULLABLE"))
            used_names.add(safe_name)

        if odoo_type == 'many2one':
            name_field = f"{safe_name}_name"
            if name_field not in used_names:
                schema.append(bigquery.SchemaField(name_field, "STRING", mode="NULLABLE"))
                used_names.add(name_field)

    return schema


# ============================================================================
# TRANSFORMACIÓN DE REGISTROS
# ============================================================================
def process_value(value, field_type):
    if field_type == 'boolean':
        if value is None or value == '' or value == 'None':
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes', 'on')
        return bool(value)

    if value is None or value == '':
        return None
    if isinstance(value, list) and len(value) == 0:
        return None

    if field_type == 'many2one':
        return value

    if field_type == 'integer':
        if value is False:
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    if field_type in ('float', 'monetary'):
        if value is False:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    if field_type == 'date':
        if value is False:
            return None
        if isinstance(value, str):
            value = value.strip()
            if value == '' or value.lower() in ('none', 'false'):
                return None
            from datetime import datetime
            for fmt in ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S']:
                try:
                    return datetime.strptime(value, fmt).date()
                except ValueError:
                    continue
            parsed = pd.to_datetime(value, errors='coerce')
            if pd.notna(parsed):
                return parsed.date()
        return None

    if field_type == 'datetime':
        if value is False:
            return None
        if isinstance(value, str):
            value = value.strip()
            if value == '' or value.lower() in ('none', 'false'):
                return None
            from datetime import datetime
            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d']:
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue
            parsed = pd.to_datetime(value, errors='coerce')
            if pd.notna(parsed):
                return parsed.to_pydatetime()
        return None

    if field_type in ('one2many', 'many2many'):
        if value is False:
            return None
        if isinstance(value, list) and len(value) > 0:
            return ', '.join(str(v) for v in value)
        return None

    if value is False:
        return None
    result = str(value) if value is not None else None
    if result in ('', 'None'):
        return None
    return result


def transform_record(record, fields_info):
    transformed = {}

    for field_name, field_data in fields_info.items():
        field_type = field_data.get('type', 'char')
        value = record.get(field_name)
        safe_name = sanitize_field_name(field_name)

        if field_type == 'many2one':
            if isinstance(value, (list, tuple)) and len(value) >= 2:
                try:
                    if isinstance(value[0], int):
                        transformed[safe_name] = value[0]
                    elif isinstance(value[0], float):
                        transformed[safe_name] = int(value[0])
                    elif isinstance(value[0], str):
                        transformed[safe_name] = int(float(value[0]))
                    else:
                        transformed[safe_name] = None
                except Exception:
                    transformed[safe_name] = None
                transformed[f"{safe_name}_name"] = str(value[1]) if value[1] is not None else None
            elif isinstance(value, (list, tuple)) and len(value) == 1:
                try:
                    transformed[safe_name] = int(float(value[0]))
                except Exception:
                    transformed[safe_name] = None
                transformed[f"{safe_name}_name"] = None
            elif isinstance(value, int) and value:
                transformed[safe_name] = value
                transformed[f"{safe_name}_name"] = None
            else:
                transformed[safe_name] = None
                transformed[f"{safe_name}_name"] = None
        else:
            transformed[safe_name] = process_value(value, field_type)

    return transformed


# ============================================================================
# LIMPIEZA DE DATAFRAME PARA BIGQUERY
# ============================================================================
def clean_dataframe(df, fields_info):
    df_clean = df.copy()

    for col in df_clean.columns:
        base_col = col[:-5] if col.endswith('_name') else col

        if col.endswith('_name'):
            df_clean[col] = df_clean[col].apply(lambda x: str(x) if x is not None and not (isinstance(x, float) and pd.isna(x)) else None)
            df_clean[col] = df_clean[col].replace(['nan', 'None', '<NA>', 'NaT'], None)
            continue

        field_data = fields_info.get(base_col) or fields_info.get(col)
        if not field_data:
            df_clean[col] = df_clean[col].apply(lambda x: str(x) if x is not None and not (isinstance(x, float) and pd.isna(x)) else None)
            df_clean[col] = df_clean[col].replace(['nan', 'None', '<NA>', 'NaT'], None)
            continue

        field_type = field_data.get('type', 'char')

        if field_type in ('integer', 'many2one'):
            SENTINEL = -999999999
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
            df_clean[col] = df_clean[col].fillna(SENTINEL)
            df_clean[col] = df_clean[col].astype('int64')
            df_clean[col] = df_clean[col].replace(SENTINEL, pd.NA)
            df_clean[col] = df_clean[col].astype('Int64')

        elif field_type in ('float', 'monetary'):
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')

        elif field_type == 'boolean':
            df_clean[col] = df_clean[col].apply(
                lambda x: None if x is None or (isinstance(x, float) and pd.isna(x)) else bool(x)
            )

        elif field_type == 'date':
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce').dt.date

        elif field_type == 'datetime':
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')

        elif field_type in ('one2many', 'many2many'):
            df_clean[col] = df_clean[col].apply(lambda x: str(x) if x is not None and not (isinstance(x, float) and pd.isna(x)) else None)
            df_clean[col] = df_clean[col].replace(['nan', 'None', '<NA>', 'NaT'], None)

        else:
            df_clean[col] = df_clean[col].apply(lambda x: str(x) if x is not None and not (isinstance(x, float) and pd.isna(x)) else None)
            df_clean[col] = df_clean[col].replace(['nan', 'None', 'False', '<NA>', 'NaT'], None)

    return df_clean


# ============================================================================
# PIPELINE PRINCIPAL: EXTRACT -> TRANSFORM -> LOAD (FULL)
# ============================================================================
def run_pipeline(uid, models, field_names, fields_info,
                 is_incremental=False, last_write_date=None):
    """Pipeline. Si is_incremental, filtra por write_date|create_date.
    Devuelve dict con stats: total_loaded, inserts, updates, max_write_date."""
    logger.info("=" * 80)
    logger.info(f"[PIPELINE] INICIO - {'INCREMENTAL' if is_incremental else 'FULL'}")
    logger.info(f"   Lote Odoo: {BATCH_SIZE_ODOO:,}")
    logger.info("=" * 80)

    # --- PASO 1: Buscar IDs ---
    try:
        if is_incremental and last_write_date:
            domain = [
                '|',
                ('write_date', '>', last_write_date),
                ('create_date', '>', last_write_date),
            ]
            logger.info(f"[INCREMENTAL] write_date OR create_date > {last_write_date}")
        else:
            domain = []
            logger.info("[FULL] Todos los registros")

        record_ids = models.execute_kw(db, uid, password,
                                       ODOO_MODEL, 'search',
                                       [domain], {'context': ODOO_CONTEXT})

        total_ids = len(record_ids)
        logger.info(f"[OK] Se encontraron {total_ids:,} partners")

        if total_ids == 0:
            logger.info("[INFO] No hay registros para procesar")
            return {'total_loaded': 0, 'inserts': 0, 'updates': 0, 'max_write_date': None}

    except Exception as e:
        logger.error(f"[ERROR] Error buscando registros: {e}")
        return {'total_loaded': -1, 'inserts': 0, 'updates': 0, 'max_write_date': None}

    # --- PASO 2: Extract + Transform ---
    all_transformed = []
    raw_records = []
    max_write_date = None
    total_batches = (total_ids + BATCH_SIZE_ODOO - 1) // BATCH_SIZE_ODOO

    for i in range(0, total_ids, BATCH_SIZE_ODOO):
        batch_ids = record_ids[i:i + BATCH_SIZE_ODOO]
        batch_num = (i // BATCH_SIZE_ODOO) + 1

        logger.info(f"[ODOO] Lote {batch_num}/{total_batches}: {len(batch_ids):,} registros...")

        # Asegurar que write_date/create_date estén en los campos pedidos para el cursor
        read_fields = list(field_names)
        for need in ('write_date', 'create_date'):
            if need not in read_fields:
                read_fields.append(need)

        try:
            records_data = models.execute_kw(db, uid, password,
                                             ODOO_MODEL, 'read',
                                             [batch_ids],
                                             {'fields': read_fields, 'context': ODOO_CONTEXT})

            max_write_date = track_max_write_date(records_data, max_write_date)
            raw_records.extend(records_data)

            for record in records_data:
                transformed = transform_record(record, fields_info)
                all_transformed.append(transformed)

        except Exception as e:
            logger.error(f"[ERROR] Error en lote {batch_num}: {e}")
            continue

        if batch_num < total_batches:
            time.sleep(SLEEP_BETWEEN_BATCHES)

    logger.info(f"[OK] Extraccion completada: {len(all_transformed):,} registros transformados")

    if not all_transformed:
        logger.error("[ERROR] No se obtuvieron registros")
        return {'total_loaded': -1, 'inserts': 0, 'updates': 0, 'max_write_date': max_write_date}

    # --- PASO 3: Load a tabla temporal + MERGE a tabla final ---
    logger.info("[LOAD] Cargando a tabla temporal...")

    client = get_bigquery_client()

    # Asegurar dataset
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset, timeout=30)
        logger.info(f"[OK] Dataset {dataset_id} creado")

    df = pd.DataFrame(all_transformed)
    df = clean_dataframe(df, fields_info)

    schema = create_bigquery_schema(fields_info)
    temp_full = f"{project_id}.{dataset_id}.{table_temp}"
    final_full = f"{project_id}.{dataset_id}.{table_name}"

    # Paso 3a: Cargar a tabla temporal
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
    )

    load_job = client.load_table_from_dataframe(df, temp_full, job_config=job_config)
    load_job.result()
    logger.info(f"[OK] {len(all_transformed):,} registros cargados en tabla temporal")

    del df
    gc.collect()

    inserts = len(all_transformed)
    updates = 0

    # Paso 3b: MERGE temporal -> final (con condicional FULL/incremental)
    try:
        # Si la tabla final no existe, crearla desde la temporal
        try:
            client.get_table(final_full)
            target_exists = True
        except Exception:
            target_exists = False

        if not target_exists:
            logger.info("[CREATE] Tabla final no existe, creandola desde temporal")
            create_query = f"CREATE TABLE `{final_full}` AS SELECT * FROM `{temp_full}`"
            client.query(create_query).result()
            final_table = client.get_table(final_full)
            logger.info(f"[OK] Tabla creada: {final_table.num_rows:,} registros")
            return {'total_loaded': inserts, 'inserts': inserts, 'updates': 0,
                    'max_write_date': max_write_date, 'raw_records': raw_records}

        # Telemetry inserts/updates
        try:
            updates = count_existing_matches(client, temp_full, final_full)
            inserts = max(0, len(all_transformed) - updates)
        except Exception:
            pass

        merge_start = time.time()
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

        if not is_incremental:
            delete_query = f"""
            DELETE FROM `{final_full}` T
            WHERE T.id NOT IN (SELECT id FROM `{temp_full}`)
            """
            client.query(delete_query).result()
            logger.info("   [DELETE] Registros obsoletos eliminados")

        merge_elapsed = time.time() - merge_start
        final_table = client.get_table(final_full)
        logger.info(f"[SUCCESS] MERGE EXITOSO - {final_table.num_rows:,} filas en tabla ({merge_elapsed:.1f}s)")

    except Exception as e:
        logger.error(f"[ERROR] Error en MERGE: {e}")
        import traceback; logger.error(traceback.format_exc())
        return {'total_loaded': -1, 'inserts': inserts, 'updates': updates,
                'max_write_date': max_write_date, 'raw_records': raw_records}
    finally:
        client.delete_table(temp_full, not_found_ok=True)
        logger.info(f"[CLEANUP] Tabla temporal eliminada")

    return {'total_loaded': len(all_transformed), 'inserts': inserts, 'updates': updates,
            'max_write_date': max_write_date, 'raw_records': raw_records}


# ============================================================================
# SYNC CONTROL + RECONCILE
# ============================================================================

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
        for row in client.query(f"SELECT MAX(write_date) AS mwd FROM `{project_id}.{dataset_id}.{table_name}`").result():
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


# ============================================================================
# MAIN
# ============================================================================
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

    logger.info("=" * 80)
    logger.info(f"[START] ETL: {ODOO_MODEL}")
    logger.info(f"[INFO] Tabla destino: {project_id}.{dataset_id}.{table_name}")
    logger.info(f"[INFO] Control: {table_sync_control}  Margen: {SYNC_SAFETY_MARGIN_MINUTES}min")
    logger.info("=" * 80)

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
            mode_desc = "FULL (primera vez)"
            is_incremental = False
            logger.info("[MODE] Tabla destino no existe -> FULL LOAD forzado")
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

        field_names, fields_info, uid, models = get_model_fields()

        if not field_names or not uid:
            raise RuntimeError("No se pudieron obtener campos del modelo")

        result = run_pipeline(uid, models, field_names, fields_info,
                              is_incremental=is_incremental,
                              last_write_date=cursor_used if is_incremental else None)

        total_processed = max(result.get('total_loaded', 0), 0)
        inserts = result.get('inserts', 0)
        updates = result.get('updates', 0)
        max_write_date = result.get('max_write_date')

        if total_processed == 0:
            logger.info("[OK] Sin registros para procesar")
            register_sync_execution(client, "SUCCESS", 0, 0, 0,
                                    cursor_used, None, start_time_dt)
            return True

        if result.get('total_loaded', 0) < 0:
            raise RuntimeError("run_pipeline reporto error")

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
        logger.info("=" * 80)
        logger.info(f"[OK] ETL {mode_desc} COMPLETADO en {elapsed:.0f}s ({elapsed/60:.1f} min)")
        logger.info(f"[STATS] Procesados: {total_processed:,} (inserts:{inserts:,} updates:{updates:,})")
        logger.info(f"[CURSOR] {new_cursor}")
        logger.info("=" * 80)
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
