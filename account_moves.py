"""
ETL fusionado para account.move y account.move.line
Extrae datos desde Odoo y los carga a BigQuery
Compatible con Windows y Linux
Genera dos tablas distintas: prod_account_move y prod_account_move_line
"""

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
import time
import pandas as pd
import json
from datetime import datetime, timedelta
import requests
import urllib3
import logging

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
        logging.FileHandler(f"{LOG_DIR}/account_moves.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Deshabilitar warnings de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Cargar variables de entorno
load_dotenv()

# ==== CONFIGURACIÓN ODOO ====
url = os.getenv('ODOO_URL')
db = os.getenv('ODOO_DB')
username = os.getenv('ODOO_USERNAME')
password = os.getenv('ODOO_API_KEY') or os.getenv('ODOO_PASSWORD')


# ==== CONFIGURACIÓN GCP ====
# Soporta override por env: GCP_TABLE_ACCOUNT_MOVE / GCP_TABLE_ACCOUNT_MOVE_LINE
# o GCP_TABLE_NAME (aplica prefijo st_ a ambas si empieza con 'st_')
project_id = os.getenv('GCP_PROJECT_ID', 'nebulan-476518')
dataset_id = os.getenv('GCP_DATASET_ID', 'nebulanodoo')

_default_move = 'prod_account_move'
_default_line = 'prod_account_move_line'

table_account_move = os.getenv('GCP_TABLE_ACCOUNT_MOVE', _default_move)
table_account_move_temp = f"{table_account_move}_temp"
table_account_move_line = os.getenv('GCP_TABLE_ACCOUNT_MOVE_LINE', _default_line)
table_account_move_line_temp = f"{table_account_move_line}_temp"
table_sync_control = os.getenv('GCP_TABLE_SYNC_CONTROL', 'etl_sync_control')
SYNC_SAFETY_MARGIN_MINUTES = 5
DEBUG_MODE = os.getenv('DEBUG_MODE', 'false').lower() == 'true'

# ==== CONFIGURACIÓN GENERAL ====
BATCH_SIZE_MOVE = 4000
BATCH_SIZE_LINE = 5000
SLEEP_AFTER_TABLE_OPS = 5
SLEEP_BETWEEN_BATCHES = 1

# Modo de carga: 'full' = reemplaza todo, 'incremental' = solo nuevos/modificados
LOAD_MODE_LINE = 'incremental'
LOAD_MODE_MOVE = 'incremental'

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
# CAMPOS PARA ACCOUNT.MOVE.LINE
# ============================================================================
SIMPLE_FIELDS = [
    'id',
    'invoice_date',
    'debit',
    'credit',
    'price_total',
    'ref',
    'name',
    'amount_currency',
    'write_date',
    'create_date',
    'x_studio_origen_n',
    'x_studio_folio_n',
]

MANY2ONE_FIELDS = {
    'move_id': ('move_id', 'move_id_value'),
    'subscription_id': ('subscription_id', 'subscription_id_value'),
    'partner_id': ('partner_id', 'partner_id_value'),
    'product_id': ('product_id', 'product_id_value'),
    'company_id': ('company_id', 'company_id_value'),
    'account_id': ('account_id', 'account_id_value'),
}

IDS_FIELDS = [
    'tax_ids',
    'sale_line_ids',
]

COLUMN_ORDER = [
    'id', 'move_id', 'move_id_value', 'invoice_date', 'x_studio_origen_n', 'x_studio_folio_n',
    'subscription_id', 'subscription_id_value', 'partner_id', 'partner_id_value',
    'tax_ids', 'debit', 'credit', 'price_total', 'ref', 'product_id', 'product_id_value',
    'name', 'amount_currency', 'company_id', 'company_id_value', 'account_id', 'account_id_value',
    'sale_line_ids', 'write_date', 'create_date'
]


# ============================================================================
# FUNCIONES UTILITARIAS
# ============================================================================
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


def get_bigquery_client():
    """Retorna cliente de BigQuery autenticado"""
    cred_path = os.path.join(script_dir, 'credenciales.json')
    if os.path.exists(cred_path):
        logger.info("[AUTH] Usando credenciales.json local")
        credentials = service_account.Credentials.from_service_account_file(cred_path)
    else:
        logger.info("[AUTH] Usando Application Default Credentials")
        credentials, _ = google.auth.default()
    return bigquery.Client(credentials=credentials, project=project_id)


def clean_dataframe_for_bigquery(df):
    """Limpia DataFrame para BigQuery (account.move)"""
    df_clean = df.copy()

    for col in df_clean.columns:
        if col in ['date', 'invoice_date', 'invoice_date_due']:
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce').dt.date
        elif col in ['write_date', 'create_date']:
            # Campos DATETIME (cursor incremental)
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
            try:
                df_clean[col] = df_clean[col].dt.tz_localize(None)
            except (TypeError, AttributeError):
                pass
        elif col.endswith('_value') or col in ['id']:
            df_clean[col] = df_clean[col].apply(safe_convert_to_int)
            df_clean[col] = df_clean[col].astype('Int64')
        elif col in ['amount_total', 'amount_untaxed', 'amount_tax', 'amount_residual']:
            df_clean[col] = df_clean[col].apply(safe_convert_to_float)
        else:
            df_clean[col] = df_clean[col].astype(str)
            df_clean[col] = df_clean[col].replace(['nan', 'None', 'False', '<NA>'], '')

    return df_clean


def jsonrpc_call(url, service, method, args):
    """Ejecuta llamada JSON-RPC a Odoo"""
    endpoint = f"{url}/jsonrpc"
    headers = {"Content-Type": "application/json"}
    
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "service": service,
            "method": method,
            "args": args
        },
        "id": 1
    }
    
    response = requests.post(endpoint, json=payload, headers=headers, verify=False, timeout=120)
    result = response.json()
    
    if "error" in result:
        raise Exception(result["error"].get("data", {}).get("message", str(result["error"])))
    
    return result.get("result")


# ============================================================================
# FUNCIONES PARA ACCOUNT.MOVE
# ============================================================================
def get_account_move_data(last_write_date=None):
    """Obtiene datos de account.move de Odoo. Si last_write_date, filtra incremental."""
    logger.info("="*80)
    logger.info("[ACCOUNT.MOVE] Procesando datos")
    logger.info("="*80)

    if url.startswith('https'):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

    socket.setdefaulttimeout(120)

    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', verbose=False)
    version = common.version()
    logger.info(f"[OK] Conexión establecida. Versión Odoo: {version.get('server_version', 'N/A')}")

    uid = common.authenticate(db, username, password, {})

    if not uid:
        logger.error("[ERROR] Error de autenticación. Verifique las credenciales.")
        return [], None, None

    logger.info(f"[OK] Autenticación exitosa. UID: {uid}")

    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', verbose=False)

    init_odoo_context(models, uid, 'account.move')

    logger.info("[EXTRACT] Obteniendo datos de account.move...")

    move_fields = [
        'id', 'name', 'move_type', 'state', 'date', 'invoice_date',
        'invoice_date_due', 'ref', 'partner_id', 'currency_id',
        'amount_total', 'amount_untaxed', 'amount_tax', 'amount_residual',
        'company_id', 'journal_id', 'invoice_origin', 'narration',
        'payment_state', 'invoice_payment_term_id', 'fiscal_position_id',
        'user_id', 'l10n_latam_document_type_id', 'write_date', 'create_date'
    ]

    base_filter = [('move_type', 'in', ['out_invoice', 'out_refund'])]
    if last_write_date:
        # AND base_filter, OR (write_date> | create_date>)
        domain = ['&'] + base_filter + [
            '|',
            ('write_date', '>', last_write_date),
            ('create_date', '>', last_write_date),
        ]
        logger.info(f"[INCREMENTAL] write_date OR create_date > {last_write_date}")
    else:
        domain = base_filter
        logger.info("[FULL] Solo move_type IN ('out_invoice','out_refund')")

    move_ids = models.execute_kw(db, uid, password,
                                'account.move', 'search',
                                [domain],
                                {'context': ODOO_CONTEXT})

    logger.info(f"[OK] Se encontraron {len(move_ids)} movimientos")

    all_moves = []
    move_batch_size = BATCH_SIZE_MOVE
    total_move_batches = len(list(create_batches(move_ids, move_batch_size)))

    for i, move_ids_batch in enumerate(create_batches(move_ids, move_batch_size), 1):
        try:
            logger.info(f"[BATCH] Procesando lote {i}/{total_move_batches} de movimientos ({len(move_ids_batch)} registros)...")
            batch_moves = models.execute_kw(db, uid, password,
                                           'account.move', 'read',
                                           [move_ids_batch],
                                           {'fields': move_fields, 'context': ODOO_CONTEXT})
            all_moves.extend(batch_moves)

            if i < total_move_batches:
                time.sleep(0.5)

        except Exception as e:
            logger.error(f"[ERROR] Error en lote {i} de movimientos: {e}")
            continue

    logger.info(f"[OK] Se obtuvieron {len(all_moves)} movimientos")

    def process_moves_for_bigquery(moves):
        bigquery_moves = []
        for move in moves:
            row_dict = {}

            for field in move_fields:
                value = move.get(field)

                if field in ['partner_id', 'currency_id', 'company_id', 'journal_id', 
                            'invoice_payment_term_id', 'fiscal_position_id', 'user_id',
                            'l10n_latam_document_type_id']:
                    if isinstance(value, list) and len(value) > 1:
                        row_dict[field.replace('_id', '')] = str(value[1])
                        row_dict[f"{field}_value"] = safe_convert_to_int(value[0])
                    else:
                        row_dict[field.replace('_id', '')] = ""
                        row_dict[f"{field}_value"] = None

                elif field in ['date', 'invoice_date', 'invoice_date_due']:
                    row_dict[field] = value if value not in [False, None] else None

                elif field in ['amount_total', 'amount_untaxed', 'amount_tax', 'amount_residual']:
                    row_dict[field] = safe_convert_to_float(value)

                elif field in ['payment_state']:
                    row_dict[field] = str(value) if value not in [False, None] else ""

                else:
                    row_dict[field] = str(value) if value not in [False, None] else ""

            bigquery_moves.append(row_dict)

        return bigquery_moves

    processed_moves = process_moves_for_bigquery(all_moves)
    # Devolvemos tambien models/uid para reconcile FULL posterior
    return processed_moves, models, uid, all_moves


def get_account_move_schema():
    """Retorna el esquema de BigQuery para account.move"""
    return [
        bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("move_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("state", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("invoice_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("invoice_date_due", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("ref", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("partner", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("partner_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("currency", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("currency_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("amount_total", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("amount_untaxed", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("amount_tax", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("amount_residual", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("company", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("company_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("journal", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("journal_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("invoice_origin", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("narration", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("payment_state", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("invoice_payment_term", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("invoice_payment_term_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("fiscal_position", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("fiscal_position_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("user", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("user_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("l10n_latam_document_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("l10n_latam_document_type_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("write_date", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("create_date", "DATETIME", mode="NULLABLE"),
    ]


def load_account_move_data_to_table(client, data, is_incremental=False):
    """Carga datos de account.move a tabla temporal y luego MERGE a tabla final.
    is_incremental=True omite el DELETE-not-in-temp (huerfanos via reconcile FULL)."""
    if not data:
        logger.warning(f"[WARNING] No hay datos para account.move")
        return

    logger.info(f"[LOAD] Cargando {len(data)} registros de account.move en tabla temporal...")

    df = pd.DataFrame(data)
    df = clean_dataframe_for_bigquery(df)

    move_schema = get_account_move_schema()
    temp_full = f"{project_id}.{dataset_id}.{table_account_move_temp}"
    final_full = f"{project_id}.{dataset_id}.{table_account_move}"

    # Paso 1: Cargar a tabla temporal (WRITE_TRUNCATE para empezar limpio)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=move_schema,
        autodetect=False,
    )

    try:
        job = client.load_table_from_dataframe(df, temp_full, job_config=job_config)
        job.result()
        logger.info(f"[OK] {len(data)} registros cargados en tabla temporal")
    except Exception as e:
        logger.error(f"[ERROR] Error cargando tabla temporal: {e}")
        return

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
            logger.info(f"[OK] Tabla {table_account_move} creada")
            client.delete_table(temp_full, not_found_ok=True)
            logger.info(f"[CLEANUP] Tabla temporal eliminada")
            return

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
            logger.info("[MERGE] Modo FULL: upsert + eliminar obsoletos")

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
        logger.info("   [MERGE] Upsert completado")

        # En modo FULL eliminar registros que ya no existen en Odoo
        if not is_incremental:
            delete_query = f"""
            DELETE FROM `{final_full}` T
            WHERE T.id NOT IN (SELECT id FROM `{temp_full}`)
            """
            client.query(delete_query).result()
            logger.info("   [DELETE] Registros obsoletos eliminados")

    except Exception as e:
        logger.error(f"[ERROR] Error en MERGE: {e}")
        import traceback
        logger.error(traceback.format_exc())

    # Paso 3: Limpiar tabla temporal
    client.delete_table(temp_full, not_found_ok=True)
    logger.info(f"[CLEANUP] Tabla temporal eliminada")


# ============================================================================
# FUNCIONES PARA ACCOUNT.MOVE.LINE
# ============================================================================
def get_last_write_date(table_id):
    """Obtiene el último write_date (con margen de seguridad) de la tabla en BigQuery."""
    try:
        client = get_bigquery_client()
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        # write_date puede ser STRING (account.move.line) o DATETIME (account.move)
        query = f"SELECT MAX(write_date) as max_write_date FROM `{table_ref}`"
        result = client.query(query).result()
        for row in result:
            if row.max_write_date:
                wd = row.max_write_date
                # Si viene como datetime/timestamp, restar margen
                if hasattr(wd, 'strftime'):
                    safe = wd - timedelta(minutes=SYNC_SAFETY_MARGIN_MINUTES)
                    return safe.strftime('%Y-%m-%d %H:%M:%S')
                # Si viene como string, intentar parsear y restar margen
                try:
                    parsed = datetime.strptime(str(wd)[:19], '%Y-%m-%d %H:%M:%S')
                    safe = parsed - timedelta(minutes=SYNC_SAFETY_MARGIN_MINUTES)
                    return safe.strftime('%Y-%m-%d %H:%M:%S')
                except Exception:
                    return str(wd)[:19]
        return None
    except Exception as e:
        logger.warning(f"[WARNING] No se pudo obtener último write_date (tabla nueva?): {e}")
        return None


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
    """Crea la tabla etl_sync_control si no existe."""
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


def get_last_successful_sync(client, target_table):
    """Devuelve cursor (con margen) desde sync_control para target_table.
    Fallback a MAX(write_date) del target. Retorna (cursor_str|None, source)."""
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
    fallback = get_last_write_date(target_table)
    if fallback:
        return fallback, "target_table_max"
    return None, "full_load"


def register_sync_execution(client, target_table, status, records_processed,
                             records_inserted, records_updated,
                             last_write_date, error_message, start_time_dt):
    """Registra ejecucion en etl_sync_control para target_table."""
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
            logger.warning(f"   sync_control insert_rows errors [{target_table}]: {errors}")
        else:
            logger.info(f"   sync_control [{target_table}]: status={status} cursor={last_write_date}")
    except Exception as e:
        logger.warning(f"   Excepcion en sync_control [{target_table}]: {e}")


def count_existing_matches(client, temp_full, target_full):
    """Cuenta cuantos IDs del temp ya existen en target (telemetry insert/update)."""
    q = f"SELECT COUNT(*) AS c FROM `{target_full}` T WHERE T.id IN (SELECT id FROM `{temp_full}`)"
    try:
        for row in client.query(q).result():
            return row.c
    except Exception:
        pass
    return 0


def track_max_write_date(records, current_max):
    """Devuelve el max write_date observado (string YYYY-MM-DD HH:MM:SS) durante el batch."""
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


def reconcile_full_with_odoo(client, models, uid, target_table, odoo_model, base_domain=None):
    """Reconciliacion FULL para incremental: lista todos los IDs vivos en Odoo y borra
    los huerfanos en BQ. base_domain permite filtrar (ej. move_type IN [...])."""
    target_ref = f"{project_id}.{dataset_id}.{target_table}"
    domain = base_domain if base_domain else []
    try:
        odoo_ids = models.execute_kw(db, uid, password, odoo_model, 'search', [domain],
                                     {'context': ODOO_CONTEXT})
        odoo_set = set(odoo_ids)
        logger.info(f"[RECONCILE {odoo_model}] Odoo: {len(odoo_set):,} IDs vivos")
    except Exception as e:
        logger.warning(f"[RECONCILE {odoo_model}] No se pudo obtener IDs de Odoo: {e}")
        return 0
    try:
        bq_set = {row.id for row in client.query(f"SELECT id FROM `{target_ref}`").result()}
        logger.info(f"[RECONCILE {odoo_model}] BQ: {len(bq_set):,} IDs en tabla")
    except Exception as e:
        logger.warning(f"[RECONCILE {odoo_model}] No se pudo leer IDs de BQ: {e}")
        return 0
    obsolete = bq_set - odoo_set
    if not obsolete:
        logger.info(f"[RECONCILE {odoo_model}] Sin huerfanos")
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
            logger.warning(f"[RECONCILE {odoo_model}] Error eliminando lote: {e}")
    logger.info(f"[RECONCILE {odoo_model}] Eliminados {deleted:,} huerfanos")
    return deleted




def transform_record(record):
    """Transforma un registro de account.move.line según la estructura deseada"""
    transformed = {}
    
    for field in SIMPLE_FIELDS:
        value = record.get(field)
        if value is False or value == '' or value == []:
            transformed[field] = None
        else:
            transformed[field] = value
    
    if transformed.get('invoice_date'):
        try:
            transformed['invoice_date'] = datetime.strptime(transformed['invoice_date'], '%Y-%m-%d').date()
        except (ValueError, TypeError):
            transformed['invoice_date'] = None
    

    
    for odoo_field, (id_col, value_col) in MANY2ONE_FIELDS.items():
        value = record.get(odoo_field)
        if isinstance(value, (list, tuple)) and len(value) == 2:
            # Asegura que el primer elemento sea int (acepta string tipo '123.0' o valores grandes)
            try:
                if isinstance(value[0], int):
                    transformed[id_col] = value[0]
                elif isinstance(value[0], float):
                    transformed[id_col] = int(value[0])
                elif isinstance(value[0], str):
                    # Puede venir como '123.0' o '123'
                    transformed[id_col] = int(float(value[0]))
                else:
                    transformed[id_col] = None
            except Exception:
                transformed[id_col] = None
            # El segundo elemento siempre como string
            transformed[value_col] = str(value[1]) if value[1] is not None else None
        else:
            transformed[id_col] = None
            transformed[value_col] = None
    
    for field in IDS_FIELDS:
        value = record.get(field)
        # Ambos campos se tratan igual: tomar primer elemento y convertir a INTEGER
        v = value
        result = None
        try:
            if isinstance(v, list) and v:
                # Si es lista, tomar el primer elemento
                v = v[0]
            if isinstance(v, int):
                result = v
            elif isinstance(v, float):
                # Acepta valores grandes
                result = int(v)
            elif isinstance(v, str):
                # Puede venir como '18.0', '18', o string largo
                v = v.strip()
                if v == '' or v.lower() == 'none':
                    result = None
                else:
                    # Intenta convertir a float y luego a int
                    result = int(float(v))
            else:
                result = None
        except Exception:
            result = None
        transformed[field] = result
    
    return transformed


def extract_account_move_line(last_write_date=None):
    """Extrae y procesa datos de account.move.line usando JSON-RPC.
    Si last_write_date, filtra incremental con dominio OR (write_date|create_date).
    Devuelve (processed_records, line_ids, models_xmlrpc, uid) para reconcile FULL posterior."""

    logger.info("="*80)
    logger.info("[ACCOUNT.MOVE.LINE] Procesando datos")
    logger.info("="*80)

    try:
        version = jsonrpc_call(url, "common", "version", [])
        logger.info(f"[OK] Conexión establecida. Versión Odoo: {version.get('server_version', 'N/A')}")

        uid = jsonrpc_call(url, "common", "authenticate", [db, username, password, {}])

        if not uid:
            logger.error("[ERROR] Error de autenticación")
            return None, [], None, None

        logger.info(f"[OK] Autenticación exitosa. UID: {uid}")

        # Inicializar contexto multicompany via XML-RPC (proxy reusable para reconcile FULL)
        models_xmlrpc = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', verbose=False)
        init_odoo_context(models_xmlrpc, uid, 'account.move.line')

    except Exception as e:
        logger.error(f"[ERROR] Error de conexión: {e}")
        return None, [], None, None

    fields_to_extract = SIMPLE_FIELDS + list(MANY2ONE_FIELDS.keys()) + IDS_FIELDS
    if 'create_date' not in fields_to_extract:
        fields_to_extract = fields_to_extract + ['create_date']

    logger.info("[SEARCH] Buscando registros de account.move.line...")
    logger.info(f"[INFO] Campos a extraer: {len(fields_to_extract)}")

    try:
        if last_write_date:
            domain = [
                '|',
                ('write_date', '>', last_write_date),
                ('create_date', '>', last_write_date),
            ]
            logger.info(f"[INCREMENTAL] write_date OR create_date > {last_write_date}")
        else:
            domain = []
            logger.info("[FULL] Buscando todos los registros")

        line_ids = jsonrpc_call(url, "object", "execute_kw",
                               [db, uid, password, 'account.move.line', 'search', [domain],
                                {'context': ODOO_CONTEXT}])

        logger.info(f"[OK] Se encontraron {len(line_ids):,} líneas de asiento")

        if len(line_ids) == 0:
            logger.info("[INFO] No hay registros nuevos/modificados para procesar")
            return None, [], models_xmlrpc, uid

    except Exception as e:
        logger.error(f"[ERROR] Error buscando registros: {e}")
        return None, [], models_xmlrpc, uid

    logger.info("[EXTRACT] Obteniendo y procesando datos...")

    try:
        total_ids = len(line_ids)
        logger.info(f"[BATCH] Procesando {total_ids:,} registros en lotes de {BATCH_SIZE_LINE}...")

        processed_records = []

        for i in range(0, total_ids, BATCH_SIZE_LINE):
            batch_ids = line_ids[i:i + BATCH_SIZE_LINE]
            batch_num = (i // BATCH_SIZE_LINE) + 1
            total_batches = (total_ids + BATCH_SIZE_LINE - 1) // BATCH_SIZE_LINE

            logger.info(f"   Lote {batch_num}/{total_batches}: {len(batch_ids):,} registros...")

            lines_data = jsonrpc_call(url, "object", "execute_kw",
                                     [db, uid, password, 'account.move.line', 'read',
                                      [batch_ids], {'fields': fields_to_extract, 'context': ODOO_CONTEXT}])

            for record in lines_data:
                transformed = transform_record(record)
                processed_records.append(transformed)

        logger.info(f"[OK] Obtenidos y procesados {len(processed_records):,} registros")

    except Exception as e:
        logger.error(f"[ERROR] Error obteniendo datos: {e}")
        return None, [], models_xmlrpc, uid

    return processed_records, line_ids, models_xmlrpc, uid


def load_account_move_line_to_bigquery(records, is_incremental=False):
    """Carga los registros de account.move.line a tabla temporal y luego MERGE a tabla final"""
    logger.info("="*80)
    logger.info("[LOAD] CARGANDO ACCOUNT.MOVE.LINE A BIGQUERY")
    logger.info("="*80)

    try:
        client = get_bigquery_client()
        logger.info(f"[OK] Conectado a BigQuery proyecto: {project_id}")
    except Exception as e:
        logger.error(f"[ERROR] Error conexión BigQuery: {e}")
        return False

    logger.info(f"[CONVERT] Convirtiendo {len(records):,} registros a DataFrame...")
    df = pd.DataFrame(records)

    existing_cols = [col for col in COLUMN_ORDER if col in df.columns]
    df = df[existing_cols]

    logger.info("[CLEAN] Limpiando tipos de datos...")

    # Columnas INTEGER (IDs y campos enteros) — Int64 nullable
    int_cols = ['id', 'move_id', 'subscription_id', 'partner_id',
                'product_id', 'company_id', 'account_id', 'x_studio_origen_n', 'x_studio_folio_n']
    SENTINEL_VALUE = -999999999
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].fillna(SENTINEL_VALUE)
            df[col] = df[col].astype('int64')
            df[col] = df[col].replace(SENTINEL_VALUE, pd.NA)
            df[col] = df[col].astype('Int64')

    # IDS_FIELDS (tax_ids, sale_line_ids): int64 puro con fillna(0)
    for col in IDS_FIELDS:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else (None if isinstance(x, list) else x))
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')

    # Columnas FLOAT (valores monetarios y decimales)
    float_cols = ['debit', 'credit', 'price_total', 'amount_currency']
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Columnas DATETIME (cursor incremental)
    for col in ['write_date', 'create_date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            try:
                df[col] = df[col].dt.tz_localize(None)
            except (TypeError, AttributeError):
                pass

    # Columnas STRING (asegurar que son strings, incluye valores de many2one)
    str_cols = ['move_id_value', 'subscription_id_value', 'partner_id_value',
                'product_id_value', 'company_id_value', 'account_id_value',
                'ref', 'name']
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: str(x) if (x is not None and not pd.isna(x)) else None)

    logger.info(f"   Columnas: {len(df.columns)}")
    logger.info(f"   Filas: {len(df):,}")

    temp_full = f"{project_id}.{dataset_id}.{table_account_move_line_temp}"
    final_full = f"{project_id}.{dataset_id}.{table_account_move_line}"

    try:
        # Schema explícito para account.move.line
        move_line_schema = [
            bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("move_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("move_id_value", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("invoice_date", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("x_studio_origen_n", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("x_studio_folio_n", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("subscription_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("subscription_id_value", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("partner_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("partner_id_value", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("tax_ids", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("debit", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("credit", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("price_total", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("ref", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("product_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("product_id_value", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("amount_currency", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("company_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("company_id_value", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("account_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("account_id_value", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("sale_line_ids", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("write_date", "DATETIME", mode="NULLABLE"),
            bigquery.SchemaField("create_date", "DATETIME", mode="NULLABLE"),
        ]

        # Paso 1: Cargar a tabla temporal
        logger.info(f"[TEMP] Cargando {len(df):,} registros a tabla temporal...")
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=move_line_schema,
            autodetect=False,
        )
        load_job = client.load_table_from_dataframe(df, temp_full, job_config=job_config)
        load_job.result()
        logger.info(f"[OK] {len(df):,} registros cargados en tabla temporal")

        # Paso 2: MERGE temporal -> final
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
            logger.info(f"[OK] Tabla {table_account_move_line} creada")
            client.delete_table(temp_full, not_found_ok=True)
            logger.info(f"[CLEANUP] Tabla temporal eliminada")
            return True

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
            logger.info("[MERGE] Modo INCREMENTAL: MERGE upsert")
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
        logger.info("   [MERGE] Upsert completado")

        # En modo FULL, eliminar registros que ya no existen en Odoo
        if not is_incremental:
            delete_query = f"""
            DELETE FROM `{final_full}` T
            WHERE T.id NOT IN (SELECT id FROM `{temp_full}`)
            """
            client.query(delete_query).result()
            logger.info("   [DELETE] Registros obsoletos eliminados")

        # Verificar resultado
        table = client.get_table(final_full)
        logger.info(f"[SUCCESS] MERGE EXITOSO")
        logger.info(f"   - Registros en tabla: {table.num_rows:,}")
        logger.info(f"   - Columnas: {len(table.schema)}")
        logger.info(f"   - Tamaño: {table.num_bytes / 1024 / 1024:.2f} MB")

        return True

    except Exception as e:
        logger.error(f"[ERROR] Error en MERGE: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    finally:
        # Paso 3: Limpiar tabla temporal siempre
        client.delete_table(temp_full, not_found_ok=True)
        logger.info(f"[CLEANUP] Tabla temporal eliminada")


# ============================================================================
# MAIN
# ============================================================================
def run_part_account_move(client, full_load=False, force_from=None):
    """Procesa account.move con sync_control. Devuelve (success, stats)."""
    start_time_dt = datetime.now()
    total_processed = 0
    inserts = 0
    updates = 0
    cursor_used = None
    max_write_date = None
    moves_data = None
    base_filter = [('move_type', 'in', ['out_invoice', 'out_refund'])]

    logger.info("\n" + "="*80)
    logger.info("PARTE 1: PROCESANDO ACCOUNT.MOVE")
    logger.info("="*80)

    try:
        target_full = f"{project_id}.{dataset_id}.{table_account_move}"
        try:
            client.get_table(target_full)
            table_exists = True
        except Exception:
            table_exists = False

        if not table_exists:
            full_load = True
            logger.info("[MODE move] Tabla destino no existe -> FULL LOAD forzado")
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
        elif LOAD_MODE_MOVE == 'incremental':
            cursor_used, source = get_last_successful_sync(client, table_account_move)
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

        logger.info(f"[MODE move] {mode_desc}")

        moves_data, models_move, uid_move, raw_moves = get_account_move_data(
            last_write_date=cursor_used if is_incremental else None
        )

        if not moves_data:
            logger.warning("[WARNING] No se obtuvieron datos de account.move")
            register_sync_execution(client, table_account_move, "SUCCESS",
                                    0, 0, 0, cursor_used, None, start_time_dt)
            return True, {'records': 0}

        max_write_date = track_max_write_date(raw_moves, max_write_date)
        total_processed = len(moves_data)

        # Telemetry inserts/updates (best effort)
        try:
            existing = {row.id for row in client.query(
                f"SELECT id FROM `{target_full}`"
            ).result()} if table_exists else set()
            new_ids = {m.get('id') for m in moves_data}
            updates = len(existing & new_ids)
            inserts = max(0, total_processed - updates)
        except Exception:
            inserts = total_processed
            updates = 0

        load_account_move_data_to_table(client, moves_data, is_incremental=is_incremental)

        # Reconcile FULL en incremental (account.move tiene base_filter)
        if is_incremental:
            try:
                reconcile_full_with_odoo(client, models_move, uid_move,
                                         table_account_move, 'account.move',
                                         base_domain=base_filter)
            except Exception as e:
                logger.warning(f"[RECONCILE move] Fallo no critico: {e}")

        new_cursor = max_write_date or cursor_used or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        register_sync_execution(client, table_account_move, "SUCCESS",
                                total_processed, inserts, updates,
                                new_cursor, None, start_time_dt)
        logger.info(f"[OK] Account.move completado: {total_processed:,} registros (inserts:{inserts:,} updates:{updates:,})")
        return True, {'records': total_processed, 'inserts': inserts, 'updates': updates}

    except Exception as e:
        logger.error(f"[ERROR] Error en PARTE 1 (account.move): {e}")
        import traceback; logger.error(traceback.format_exc())
        try:
            register_sync_execution(client, table_account_move, "ERROR",
                                    total_processed, inserts, updates,
                                    None, str(e), start_time_dt)
        except Exception:
            pass
        return False, {'records': total_processed}


def run_part_account_move_line(client, full_load=False, force_from=None):
    """Procesa account.move.line con sync_control. Devuelve (success, stats)."""
    start_time_dt = datetime.now()
    total_processed = 0
    inserts = 0
    updates = 0
    cursor_used = None
    max_write_date = None

    logger.info("\n" + "="*80)
    logger.info("PARTE 2: PROCESANDO ACCOUNT.MOVE.LINE")
    logger.info("="*80)

    try:
        target_full = f"{project_id}.{dataset_id}.{table_account_move_line}"
        try:
            client.get_table(target_full)
            table_exists = True
        except Exception:
            table_exists = False

        if not table_exists:
            full_load = True
            logger.info("[MODE line] Tabla destino no existe -> FULL LOAD forzado")
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
        elif LOAD_MODE_LINE == 'incremental':
            cursor_used, source = get_last_successful_sync(client, table_account_move_line)
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

        logger.info(f"[MODE line] {mode_desc}")

        records, _line_ids, models_xmlrpc, uid_line = extract_account_move_line(
            last_write_date=cursor_used if is_incremental else None
        )

        if not records:
            logger.info("[OK] Sin cambios - No hay registros nuevos/modificados")
            register_sync_execution(client, table_account_move_line, "SUCCESS",
                                    0, 0, 0, cursor_used, None, start_time_dt)
            return True, {'records': 0}

        max_write_date = track_max_write_date(records, max_write_date)
        total_processed = len(records)

        # Telemetry inserts/updates
        try:
            new_ids = {r.get('id') for r in records}
            existing = {row.id for row in client.query(
                f"SELECT id FROM `{target_full}` WHERE id IN UNNEST({list(new_ids)})"
            ).result()} if table_exists and new_ids else set()
            updates = len(existing & new_ids)
            inserts = max(0, total_processed - updates)
        except Exception:
            inserts = total_processed
            updates = 0

        success = load_account_move_line_to_bigquery(records, is_incremental)
        if not success:
            raise RuntimeError("MERGE de account.move.line fallo")

        # Reconcile FULL en incremental
        if is_incremental and models_xmlrpc and uid_line:
            try:
                reconcile_full_with_odoo(client, models_xmlrpc, uid_line,
                                         table_account_move_line, 'account.move.line')
            except Exception as e:
                logger.warning(f"[RECONCILE line] Fallo no critico: {e}")

        new_cursor = max_write_date or cursor_used or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        register_sync_execution(client, table_account_move_line, "SUCCESS",
                                total_processed, inserts, updates,
                                new_cursor, None, start_time_dt)
        logger.info(f"[OK] Account.move.line completado: {total_processed:,} registros (inserts:{inserts:,} updates:{updates:,})")
        return True, {'records': total_processed, 'inserts': inserts, 'updates': updates}

    except Exception as e:
        logger.error(f"[ERROR] Error en PARTE 2 (account.move.line): {e}")
        import traceback; logger.error(traceback.format_exc())
        try:
            register_sync_execution(client, table_account_move_line, "ERROR",
                                    total_processed, inserts, updates,
                                    None, str(e), start_time_dt)
        except Exception:
            pass
        return False, {'records': total_processed}


def run_etl(full_load=False, force_from=None):
    """ETL fusionado: account.move + account.move.line con manejo independiente.
    Si una tabla falla, la otra continua."""
    logger.info("="*80)
    logger.info("[START] ETL FUSIONADO: ACCOUNT.MOVE + ACCOUNT.MOVE.LINE")
    logger.info(f"[INFO] Tablas: {table_account_move}, {table_account_move_line}")
    logger.info(f"[INFO] Control: {table_sync_control}  Margen: {SYNC_SAFETY_MARGIN_MINUTES}min")
    logger.info("="*80)

    start_time = time.time()

    required_vars = ['ODOO_URL', 'ODOO_DB', 'ODOO_USERNAME']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if not (os.getenv('ODOO_API_KEY') or os.getenv('ODOO_PASSWORD')):
        missing_vars.append('ODOO_API_KEY o ODOO_PASSWORD')
    if missing_vars:
        logger.error(f"[ERROR] Variables faltantes: {missing_vars}")
        raise RuntimeError(f"Variables faltantes: {missing_vars}")

    client = get_bigquery_client()
    ensure_sync_control_table(client)

    # PARTE 1 (account.move) - independiente
    success_move = False
    stats_move = {'records': 0}
    try:
        success_move, stats_move = run_part_account_move(client, full_load, force_from)
    except Exception as e:
        logger.error(f"[ERROR] Error general PARTE 1: {e}")
        import traceback; logger.error(traceback.format_exc())

    # PARTE 2 (account.move.line) - independiente, corre aunque PARTE 1 falle
    success_line = False
    stats_line = {'records': 0}
    try:
        success_line, stats_line = run_part_account_move_line(client, full_load, force_from)
    except Exception as e:
        logger.error(f"[ERROR] Error general PARTE 2: {e}")
        import traceback; logger.error(traceback.format_exc())

    elapsed = time.time() - start_time
    logger.info("\n" + "="*80)
    logger.info("[SUMMARY] RESUMEN FINAL")
    logger.info("="*80)
    logger.info(f"[TIME] Tiempo total: {elapsed:.0f}s ({elapsed/60:.1f} min)")
    logger.info(f"[TABLE] {table_account_move}: {stats_move.get('records', 0):,} registros - {'OK' if success_move else 'ERROR'}")
    logger.info(f"[TABLE] {table_account_move_line}: {stats_line.get('records', 0):,} registros - {'OK' if success_line else 'ERROR'}")
    logger.info("[COMPLETED] ETL COMPLETADO")
    return success_move and success_line


def main():
    """Entry point para el orquestador. Modo incremental por defecto."""
    success = run_etl(full_load=False, force_from=None)
    if not success:
        raise RuntimeError("ETL account_moves fallo en alguna tabla")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ETL account.move + account.move.line -> BigQuery (sync_control)')
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