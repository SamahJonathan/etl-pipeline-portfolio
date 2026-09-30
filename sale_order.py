import xmlrpc.client
import os
import sys
import uuid
import argparse
from dotenv import load_dotenv
import ssl
import socket
from google.cloud import bigquery
import google.auth
import time
import pandas as pd
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
        logging.FileHandler(f"{LOG_DIR}/sale_order.log", encoding='utf-8'),
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
table_sale_order = os.getenv('GCP_TABLE_NAME', 'prod_sale_order')
table_sale_order_temp = f"{table_sale_order}_temp"
table_sync_control = os.getenv('GCP_TABLE_SYNC_CONTROL', 'etl_sync_control')
SYNC_SAFETY_MARGIN_MINUTES = 5
DEBUG_MODE = os.getenv('DEBUG_MODE', 'false').lower() == 'true'

# ==== CONFIGURACIÓN DE LOTES ====
BATCH_SIZE = 4000
SLEEP_BETWEEN_BATCHES = 1
ODOO_MODEL = 'sale.order'
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

def safe_convert_to_string(value):
    """Convierte un valor a string de forma segura"""
    if value is None or value == False:
        return ""
    if isinstance(value, list):
        # Para campos many2many u one2many, unir IDs con comas
        return ', '.join([str(id) for id in value if id])
    return str(value).strip()

def safe_convert_to_datetime(value):
    """Convierte un valor a datetime string de forma segura"""
    if value is None or value == False:
        return None
    return str(value).strip() if str(value).strip() else None

def get_sale_orders(last_write_date=None):
    """Obtiene sale.order. Devuelve (processed_orders, raw_orders, models, uid)."""
    # Configurar SSL
    if url.startswith('https'):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    
    socket.setdefaulttimeout(120)
    
    # Conexión a Odoo
    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', verbose=False)
    version = common.version()
    #print(f"✅ Conexión establecida. Versión Odoo: {version.get('server_version', 'N/A')}")
    
    # Autenticación
    uid = common.authenticate(db, username, password, {})
    if not uid:
        logger.error("Error de autenticación")
        return [], [], None, None

    # Proxy para modelos
    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', verbose=False)
    init_odoo_context(models, uid, ODOO_MODEL)
    
    # Campos de sale.order - AGREGADO team_id, invoice_status y opportunity_id
    so_fields = [
        'id', 'name', 'user_id', 'activity_ids', 'margin_percent',
        'amount_total', 'state', 'partner_id', 'partner_invoice_id',
        'partner_shipping_id', 'termino_condicion_id', 'validity_date',
        'date_order', 'start_date', 'end_date', 'plan_id', 'pricelist_id',
        'payment_term_id', 'categ_id', 'doc_cliente_ids', 'team_id',
        'invoice_status', 'company_id', 'amount_untaxed', 'amount_tax',
        'opportunity_id', 'write_date', 'create_date',
    ]
    
    #print("\n📋 Obteniendo sale.order...")
    
    if last_write_date:
        domain = [
            '|',
            ('write_date', '>', last_write_date),
            ('create_date', '>', last_write_date),
        ]
        logger.info(f"[INCREMENTAL] write_date OR create_date > {last_write_date}")
    else:
        domain = []
        logger.info("[FULL] Todas las ordenes")

    so_ids = models.execute_kw(db, uid, password,
                               'sale.order', 'search', [domain],
                               {'context': ODOO_CONTEXT})
    if not so_ids:
        return [], [], models, uid
    
    #print(f"✅ Se encontraron {len(so_ids)} órdenes de venta")
    
    # Procesar en lotes
    all_orders = []
    batch_size = 2000
    total_batches = len(list(create_batches(so_ids, batch_size)))
    
    for i, so_ids_batch in enumerate(create_batches(so_ids, batch_size), 1):
        try:
            logger.info(f"Procesando lote {i}/{total_batches} ({len(so_ids_batch)} registros)...")
            batch_orders = models.execute_kw(db, uid, password,
                                           'sale.order', 'read',
                                           [so_ids_batch],
                                           {'fields': so_fields, 'context': ODOO_CONTEXT})
            all_orders.extend(batch_orders)
            
            if i < total_batches:
                time.sleep(0.5)
                
        except Exception as e:
            logger.error(f"Error en lote {i}: {e}")
            continue
    
    logger.info(f"Se obtuvieron {len(all_orders)} órdenes de venta")
    
    # Procesar para BigQuery
    def process_orders_for_bigquery(orders):
        bigquery_orders = []
        
        for order in orders:
            row_dict = {}
            
            for field in so_fields:
                value = order.get(field)
                
                # Campo especial: opportunity_id (solo ID numérico)
                if field == 'opportunity_id':
                    if isinstance(value, list) and len(value) > 0:
                        row_dict['opportunity_id'] = safe_convert_to_int(value[0])
                    else:
                        row_dict['opportunity_id'] = None
                
                # Campos many2one (SIN opportunity_id)
                elif field in ['user_id', 'partner_id', 'partner_invoice_id', 
                            'partner_shipping_id', 'termino_condicion_id', 'plan_id',
                            'pricelist_id', 'payment_term_id', 'categ_id', 'doc_cliente_ids', 
                            'team_id', 'company_id']:
                    if isinstance(value, list) and len(value) > 1:
                        # Manejo especial para doc_cliente_ids
                        if field == 'doc_cliente_ids':
                            row_dict['doc_cliente'] = str(value[1])
                            row_dict[f"{field}_value"] = safe_convert_to_int(value[0])
                        else:
                            row_dict[field.replace('_id', '')] = str(value[1])
                            row_dict[f"{field}_value"] = safe_convert_to_int(value[0])
                    else:
                        if field == 'doc_cliente_ids':
                            row_dict['doc_cliente'] = ""
                            row_dict[f"{field}_value"] = None
                        else:
                            row_dict[field.replace('_id', '')] = ""
                            row_dict[f"{field}_value"] = None
                
                # Campos one2many
                elif field in ['activity_ids']:
                    row_dict[field] = safe_convert_to_string(value)
                
                # Campos float/monetarios
                elif field in ['margin_percent', 'amount_total', 'amount_untaxed', 'amount_tax']:
                    row_dict[field] = safe_convert_to_float(value)
                
                # Campos date/datetime
                elif field in ['validity_date', 'date_order', 'start_date', 'end_date']:
                    row_dict[field] = safe_convert_to_datetime(value)
                
                # Campos text/char/selection - AGREGADO invoice_status
                else:
                    row_dict[field] = str(value) if value not in [False, None] else ""
            
            bigquery_orders.append(row_dict)
        
        return bigquery_orders
    
    return process_orders_for_bigquery(all_orders), all_orders, models, uid
def get_bigquery_client():
    """Obtiene cliente de BigQuery"""
    credentials, _ = google.auth.default()
    return bigquery.Client(credentials=credentials, project=project_id)


def get_sale_order_schema():
    """Retorna el esquema de BigQuery para sale.order"""
    return [
        bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("user", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("user_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("activity_ids", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("margin_percent", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("amount_total", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("amount_untaxed", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("amount_tax", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("state", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("partner", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("partner_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("partner_invoice", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("partner_invoice_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("partner_shipping", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("partner_shipping_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("termino_condicion", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("termino_condicion_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("validity_date", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("date_order", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("start_date", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("end_date", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("plan", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("plan_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pricelist", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("pricelist_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("payment_term", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("payment_term_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("categ", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("categ_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("doc_cliente", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("doc_cliente_ids_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("team", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("team_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("company", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("company_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("invoice_status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("opportunity_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("write_date", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("create_date", "DATETIME", mode="NULLABLE"),
    ]

def clean_dataframe_for_bigquery(df):
    """Limpia DataFrame para BigQuery"""
    df_clean = df.copy()
    
    for col in df_clean.columns:
        if col.endswith('_value') or col in ['id', 'opportunity_id']:
            # Campos enteros (agregado opportunity_id)
            df_clean[col] = df_clean[col].apply(safe_convert_to_int)
            df_clean[col] = df_clean[col].astype('Int64')
        elif col in ['margin_percent', 'amount_total', 'amount_untaxed', 'amount_tax']:
            # Campos float
            df_clean[col] = df_clean[col].apply(safe_convert_to_float)
        elif col in ['validity_date', 'date_order', 'start_date', 'end_date', 'write_date', 'create_date']:
            # Campos datetime (incluye cursor incremental)
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
            try:
                df_clean[col] = df_clean[col].dt.tz_localize(None)
            except (TypeError, AttributeError):
                pass
        else:
            # Campos string - INCLUYE invoice_status
            df_clean[col] = df_clean[col].astype(str)
            df_clean[col] = df_clean[col].replace(['nan', 'None', 'False', '<NA>'], '')
            df_clean[col] = df_clean[col].fillna('')
    
    return df_clean

def load_sale_orders_to_bigquery(client, data, is_incremental=False):
    """Carga sale orders. is_incremental=True omite DELETE-not-in-temp.
    Devuelve (success, inserts, updates)."""
    if not data:
        logger.warning("No hay datos para cargar")
        return True, 0, 0

    logger.info(f"[LOAD] Cargando {len(data):,} registros a tabla temporal...")

    df = pd.DataFrame(data)
    df = clean_dataframe_for_bigquery(df)

    schema = get_sale_order_schema()
    temp_full = f"{project_id}.{dataset_id}.{table_sale_order_temp}"
    final_full = f"{project_id}.{dataset_id}.{table_sale_order}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
        autodetect=False,
    )

    try:
        job = client.load_table_from_dataframe(df, temp_full, job_config=job_config)
        job.result()
        logger.info(f"[OK] {len(data):,} cargados en tabla temporal")
    except Exception as e:
        logger.error(f"[ERROR] Error cargando tabla temporal: {e}")
        return False, 0, 0

    inserts = len(data)
    updates = 0
    try:
        try:
            client.get_table(final_full)
            target_exists = True
        except Exception:
            target_exists = False

        if not target_exists:
            client.query(f"CREATE TABLE `{final_full}` AS SELECT * FROM `{temp_full}`").result()
            final_table = client.get_table(final_full)
            logger.info(f"[OK] Tabla creada: {final_table.num_rows:,} registros")
            return True, inserts, 0

        try:
            updates = count_existing_matches(client, temp_full, final_full)
            inserts = max(0, len(data) - updates)
        except Exception:
            pass

        merge_start = time.time()
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
            logger.info("[MERGE] INCREMENTAL: solo upsert (huerfanos via reconcile FULL)")
        else:
            logger.info("[MERGE] FULL: upsert + eliminar obsoletos")

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
            client.query(f"""
            DELETE FROM `{final_full}` T
            WHERE T.id NOT IN (SELECT id FROM `{temp_full}`)
            """).result()
            logger.info("   [DELETE] Registros obsoletos eliminados")

        merge_elapsed = time.time() - merge_start
        final_table = client.get_table(final_full)
        logger.info(f"[SUCCESS] - {final_table.num_rows:,} filas ({merge_elapsed:.1f}s)")
        return True, inserts, updates

    except Exception as e:
        logger.error(f"[ERROR] Error en MERGE: {e}")
        import traceback; logger.error(traceback.format_exc())
        return False, inserts, updates
    finally:
        client.delete_table(temp_full, not_found_ok=True)
        logger.info("[CLEANUP] Tabla temporal eliminada")


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
    raise ValueError(f"Formato invalido: '{value}'")


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
        for row in client.query(f"SELECT MAX(write_date) AS mwd FROM `{project_id}.{dataset_id}.{table_sale_order}`").result():
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
    SELECT last_odoo_write_date FROM `{project_id}.{dataset_id}.{table_sync_control}`
    WHERE table_name = '{table_sale_order}' AND status = 'SUCCESS'
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
        "table_name": table_sale_order,
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
    target_ref = f"{project_id}.{dataset_id}.{table_sale_order}"
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

    logger.info("=" * 80)
    logger.info(f"[START] ETL: {ODOO_MODEL}")
    logger.info(f"[INFO] Tabla destino: {project_id}.{dataset_id}.{table_sale_order}")
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

        target_full = f"{project_id}.{dataset_id}.{table_sale_order}"
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

        orders_data, raw_orders, models, uid = get_sale_orders(
            last_write_date=cursor_used if is_incremental else None
        )

        if not orders_data:
            logger.info("[OK] Sin registros para procesar")
            register_sync_execution(client, "SUCCESS", 0, 0, 0,
                                    cursor_used, None, start_time_dt)
            return True

        max_write_date = track_max_write_date(raw_orders, max_write_date)
        total_processed = len(orders_data)

        success, inserts, updates = load_sale_orders_to_bigquery(
            client, orders_data, is_incremental=is_incremental
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
        logger.info("=" * 80)
        logger.info(f"[OK] ETL {mode_desc} COMPLETADO en {elapsed:.0f}s ({elapsed/60:.1f} min)")
        logger.info(f"[STATS] Procesados: {total_processed:,} (inserts:{inserts:,} updates:{updates:,})")
        logger.info(f"[CURSOR] {new_cursor}")
        logger.info("=" * 80)
        return True

    except KeyboardInterrupt:
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
