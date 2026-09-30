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
        logging.FileHandler(f"{LOG_DIR}/purchase_order.log", encoding='utf-8'),
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
table_purchase_order = os.getenv('GCP_TABLE_NAME', 'prod_purchase_order')
table_purchase_order_temp = f"{table_purchase_order}_temp"
table_sync_control = os.getenv('GCP_TABLE_SYNC_CONTROL', 'etl_sync_control')
SYNC_SAFETY_MARGIN_MINUTES = 5
DEBUG_MODE = os.getenv('DEBUG_MODE', 'false').lower() == 'true'

# ==== CONFIGURACIÓN DE LOTES ====
BATCH_SIZE = 4000
SLEEP_AFTER_TABLE_OPS = 5
SLEEP_BETWEEN_BATCHES = 1
ODOO_MODEL = 'purchase.order'
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
        return 0.0
    try:
        if isinstance(value, str):
            value = value.strip()
            if value == '' or value.lower() == 'none' or value.lower() == 'false':
                return 0.0
        return float(value)
    except (ValueError, TypeError):
        return 0.0

def safe_convert_to_boolean(value):
    if value is None or value == '' or value == 'False':
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ['true', '1', 'yes', 'on']
    return bool(value)

def get_purchase_orders(last_write_date=None):
    """Devuelve (processed_orders, raw_orders, models, uid)."""
    if url.startswith('https'):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

    socket.setdefaulttimeout(120)

    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', verbose=False)
    version = common.version()
    logger.info(f"Conexion establecida. Version Odoo: {version.get('server_version', 'N/A')}")

    uid = common.authenticate(db, username, password, {})
    if not uid:
        logger.info("Error de autenticacion")
        return [], [], None, None

    logger.info(f"Autenticacion exitosa. UID: {uid}")

    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', verbose=False)
    init_odoo_context(models, uid, ODOO_MODEL)

    purchase_fields = [
        'id', 'activity_calendar_event_id', 'activity_date_deadline', 'activity_exception_decoration',
        'activity_exception_icon', 'activity_ids', 'activity_state', 'activity_summary',
        'activity_type_icon', 'activity_type_id', 'activity_user_id', 'amount_tax',
        'amount_total', 'amount_untaxed', 'company_id', 'country_code', 'create_date',
        'create_uid', 'currency_id', 'currency_rate', 'date_approve', 'date_calendar_start',
        'date_order', 'date_planned', 'default_location_dest_id_usage', 'dest_address_id',
        'display_name', 'effective_date', 'fiscal_position_id', 'group_id', 'has_message',
        'incoming_picking_count', 'incoterm_id', 'incoterm_location', 'invoice_count',
        'invoice_ids', 'invoice_status', 'is_shipped', 'mail_reception_confirmed',
        'mail_reminder_confirmed', 'my_activity_date_deadline', 'name', 'notes',
        'on_time_rate', 'order_line', 'origin', 'partner_id', 'partner_ref',
        'payment_term_id', 'picking_ids', 'picking_type_id', 'priority', 'product_id',
        'rating_ids', 'receipt_reminder_email', 'receipt_status', 'reminder_date_before_receipt',
        'sale_order_count', 'state', 'tax_calculation_rounding_method', 'tax_country_id',
        'tax_totals', 'user_id', 'website_message_ids', 'write_date', 'write_uid'
    ]

    logger.info("Obteniendo purchase.order...")

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

    purchase_ids = models.execute_kw(db, uid, password,
                                   'purchase.order', 'search', [domain],
                                   {'context': ODOO_CONTEXT})

    logger.info(f"Se encontraron {len(purchase_ids)} ordenes de compra")
    if not purchase_ids:
        return [], [], models, uid

    all_orders = []
    batch_size = 2000
    total_batches = (len(purchase_ids) + batch_size - 1) // batch_size

    for i, purchase_ids_batch in enumerate(create_batches(purchase_ids, batch_size), 1):
        try:
            logger.info(f"Procesando lote {i}/{total_batches} ({len(purchase_ids_batch)} registros)...")
            batch_orders = models.execute_kw(db, uid, password,
                                           'purchase.order', 'read',
                                           [purchase_ids_batch],
                                           {'fields': purchase_fields, 'context': ODOO_CONTEXT})
            all_orders.extend(batch_orders)

            if i < total_batches:
                time.sleep(0.5)

        except Exception as e:
            logger.info(f"Error en lote {i}: {e}")
            continue

    logger.info(f"Se obtuvieron {len(all_orders)} ordenes de compra")

    many2one_fields = [
        'activity_calendar_event_id', 'activity_type_id', 'activity_user_id',
        'company_id', 'create_uid', 'currency_id', 'dest_address_id',
        'fiscal_position_id', 'group_id', 'incoterm_id', 'partner_id',
        'payment_term_id', 'picking_type_id', 'product_id', 'tax_country_id',
        'user_id', 'write_uid'
    ]

    multi_fields = [
        'activity_ids', 'invoice_ids', 'order_line', 'picking_ids',
        'rating_ids', 'website_message_ids'
    ]

    float_fields = [
        'amount_tax', 'amount_total', 'amount_untaxed', 'currency_rate', 'on_time_rate'
    ]

    int_fields = [
        'incoming_picking_count', 'invoice_count', 'reminder_date_before_receipt',
        'sale_order_count'
    ]

    boolean_fields = [
        'has_message', 'is_shipped', 'mail_reception_confirmed',
        'mail_reminder_confirmed', 'receipt_reminder_email'
    ]

    datetime_fields = ['create_date', 'date_approve', 'write_date']
    date_fields = [
        'activity_date_deadline', 'date_calendar_start', 'date_order',
        'date_planned', 'effective_date', 'my_activity_date_deadline'
    ]

    def process_orders_for_bigquery(orders):
        bigquery_orders = []

        for order in orders:
            row_dict = {}

            for field in purchase_fields:
                value = order.get(field)

                if field in many2one_fields:
                    if isinstance(value, list) and len(value) > 1:
                        field_name = field.replace('_id', '')
                        if field_name == 'create_uid':
                            field_name = 'create_u'
                        elif field_name == 'write_uid':
                            field_name = 'write_u'

                        row_dict[field_name] = str(value[1])
                        row_dict[f"{field}_value"] = safe_convert_to_int(value[0])
                    else:
                        field_name = field.replace('_id', '')
                        if field_name == 'create_uid':
                            field_name = 'create_u'
                        elif field_name == 'write_uid':
                            field_name = 'write_u'

                        row_dict[field_name] = ""
                        row_dict[f"{field}_value"] = None

                elif field in multi_fields:
                    if value and isinstance(value, list):
                        row_dict[field] = ', '.join([str(id) for id in value])
                    else:
                        row_dict[field] = ""

                elif field in float_fields:
                    row_dict[field] = safe_convert_to_float(value)

                elif field in int_fields or field == 'id':
                    row_dict[field] = safe_convert_to_int(value)

                elif field in boolean_fields:
                    row_dict[field] = safe_convert_to_boolean(value)

                elif field in datetime_fields:
                    # Guardar como string, se convierte en clean_dataframe
                    if value and value not in [False, None, '', 'False']:
                        row_dict[field] = str(value)
                    else:
                        row_dict[field] = None

                elif field in date_fields:
                    # Guardar como string, se convierte en clean_dataframe
                    if value and value not in [False, None, '', 'False']:
                        val_str = str(value)
                        row_dict[field] = val_str[:10] if len(val_str) >= 10 else val_str
                    else:
                        row_dict[field] = None

                elif field == 'tax_totals':
                    row_dict[field] = str(value) if value not in [False, None] else ""

                else:
                    row_dict[field] = str(value) if value not in [False, None] else ""

            bigquery_orders.append(row_dict)

        return bigquery_orders

    processed_orders = process_orders_for_bigquery(all_orders)
    return processed_orders, all_orders, models, uid

def get_bigquery_client():
    """Obtiene cliente de BigQuery"""
    credentials, _ = google.auth.default()
    return bigquery.Client(credentials=credentials, project=project_id)


def get_purchase_order_schema():
    """Retorna el esquema de BigQuery para purchase order"""
    return [
        bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("activity_calendar_event", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("activity_calendar_event_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("activity_date_deadline", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("activity_exception_decoration", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("activity_exception_icon", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("activity_ids", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("activity_state", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("activity_summary", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("activity_type_icon", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("activity_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("activity_type_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("activity_user", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("activity_user_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("amount_tax", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("amount_total", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("amount_untaxed", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("company", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("company_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("country_code", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("create_date", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("create_u", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("create_uid_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("currency", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("currency_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("currency_rate", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("date_approve", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("date_calendar_start", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("date_order", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("date_planned", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("default_location_dest_id_usage", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("dest_address", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("dest_address_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("display_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("effective_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("fiscal_position", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("fiscal_position_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("group", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("group_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("has_message", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("incoming_picking_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("incoterm", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("incoterm_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("incoterm_location", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("invoice_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("invoice_ids", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("invoice_status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("is_shipped", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("mail_reception_confirmed", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("mail_reminder_confirmed", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("my_activity_date_deadline", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("notes", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("on_time_rate", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("order_line", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("origin", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("partner", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("partner_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("partner_ref", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("payment_term", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("payment_term_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("picking_ids", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("picking_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("picking_type_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("priority", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("product", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("product_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("rating_ids", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("receipt_reminder_email", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("receipt_status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("reminder_date_before_receipt", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("sale_order_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("state", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("tax_calculation_rounding_method", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("tax_country", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("tax_country_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("tax_totals", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("user", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("user_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("website_message_ids", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("write_date", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("write_u", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("write_uid_value", "INTEGER", mode="NULLABLE"),
    ]

def clean_dataframe_for_bigquery(df):
    df_clean = df.copy()

    expected_fields = [
        'id', 'activity_calendar_event', 'activity_calendar_event_id_value', 'activity_date_deadline',
        'activity_exception_decoration', 'activity_exception_icon', 'activity_ids', 'activity_state',
        'activity_summary', 'activity_type_icon', 'activity_type', 'activity_type_id_value',
        'activity_user', 'activity_user_id_value', 'amount_tax', 'amount_total', 'amount_untaxed',
        'company', 'company_id_value', 'country_code', 'create_date', 'create_u', 'create_uid_value',
        'currency', 'currency_id_value', 'currency_rate', 'date_approve', 'date_calendar_start',
        'date_order', 'date_planned', 'default_location_dest_id_usage', 'dest_address',
        'dest_address_id_value', 'display_name', 'effective_date', 'fiscal_position',
        'fiscal_position_id_value', 'group', 'group_id_value', 'has_message', 'incoming_picking_count',
        'incoterm', 'incoterm_id_value', 'incoterm_location', 'invoice_count', 'invoice_ids',
        'invoice_status', 'is_shipped', 'mail_reception_confirmed', 'mail_reminder_confirmed',
        'my_activity_date_deadline', 'name', 'notes', 'on_time_rate', 'order_line', 'origin',
        'partner', 'partner_id_value', 'partner_ref', 'payment_term', 'payment_term_id_value',
        'picking_ids', 'picking_type', 'picking_type_id_value', 'priority', 'product',
        'product_id_value', 'rating_ids', 'receipt_reminder_email', 'receipt_status',
        'reminder_date_before_receipt', 'sale_order_count', 'state', 'tax_calculation_rounding_method',
        'tax_country', 'tax_country_id_value', 'tax_totals', 'user', 'user_id_value',
        'website_message_ids', 'write_date', 'write_u', 'write_uid_value'
    ]

    df_clean = df_clean[[col for col in df_clean.columns if col in expected_fields]]

    datetime_fields = ['create_date', 'date_approve', 'write_date']
    date_fields = ['activity_date_deadline', 'date_calendar_start', 'date_order',
                   'date_planned', 'effective_date', 'my_activity_date_deadline']

    int_id_fields = ['id', 'incoming_picking_count', 'invoice_count',
                     'reminder_date_before_receipt', 'sale_order_count']

    float_fields = ['amount_tax', 'amount_total', 'amount_untaxed', 'currency_rate', 'on_time_rate']

    boolean_fields = ['has_message', 'is_shipped', 'mail_reception_confirmed',
                      'mail_reminder_confirmed', 'receipt_reminder_email']

    for col in df_clean.columns:
        if col.endswith('_value') or col in int_id_fields:
            df_clean[col] = df_clean[col].apply(safe_convert_to_int)
            df_clean[col] = df_clean[col].astype('Int64')
        elif col in float_fields:
            df_clean[col] = df_clean[col].apply(safe_convert_to_float)
        elif col in boolean_fields:
            df_clean[col] = df_clean[col].apply(safe_convert_to_boolean)
        elif col in datetime_fields:
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
        elif col in date_fields:
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
            df_clean[col] = df_clean[col].dt.date
            df_clean[col] = df_clean[col].where(df_clean[col].notna(), None)
        else:
            df_clean[col] = df_clean[col].astype(str)
            df_clean[col] = df_clean[col].replace(['nan', 'None', 'False', '<NA>'], '')

    return df_clean

def load_data_to_table(client, data, is_incremental=False):
    """Carga datos a tabla temporal y MERGE a tabla final.
    is_incremental=True omite el DELETE-not-in-temp.
    Devuelve (success, inserts, updates)."""
    if not data:
        logger.info("No hay datos para cargar")
        return True, 0, 0

    logger.info(f"[LOAD] Cargando {len(data):,} registros a tabla temporal...")

    df = pd.DataFrame(data)
    df = clean_dataframe_for_bigquery(df)

    schema = get_purchase_order_schema()
    temp_full = f"{project_id}.{dataset_id}.{table_purchase_order_temp}"
    final_full = f"{project_id}.{dataset_id}.{table_purchase_order}"

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
            logger.info("[CREATE] Tabla final no existe, creandola desde temporal")
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
        for row in client.query(f"SELECT MAX(write_date) AS mwd FROM `{project_id}.{dataset_id}.{table_purchase_order}`").result():
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
    WHERE table_name = '{table_purchase_order}' AND status = 'SUCCESS'
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
        "table_name": table_purchase_order,
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
    target_ref = f"{project_id}.{dataset_id}.{table_purchase_order}"
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
    logger.info(f"[INFO] Tabla destino: {project_id}.{dataset_id}.{table_purchase_order}")
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

        target_full = f"{project_id}.{dataset_id}.{table_purchase_order}"
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

        orders_data, raw_orders, models, uid = get_purchase_orders(
            last_write_date=cursor_used if is_incremental else None
        )

        if not orders_data:
            logger.info("[OK] Sin registros para procesar")
            register_sync_execution(client, "SUCCESS", 0, 0, 0,
                                    cursor_used, None, start_time_dt)
            return True

        max_write_date = track_max_write_date(raw_orders, max_write_date)
        total_processed = len(orders_data)

        success, inserts, updates = load_data_to_table(client, orders_data, is_incremental=is_incremental)
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
