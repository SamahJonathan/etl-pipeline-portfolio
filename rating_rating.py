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
from google.cloud import bigquery
import google.auth
from datetime import datetime, timedelta

import logging

script_dir = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(script_dir, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/rating_rating.log", encoding='utf-8'),
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
table_rating = os.getenv('GCP_TABLE_NAME', 'prod_rating_rating')
table_rating_temp = f"{table_rating}_temp"
table_sync_control = os.getenv('GCP_TABLE_SYNC_CONTROL', 'etl_sync_control')
SYNC_SAFETY_MARGIN_MINUTES = 5
DEBUG_MODE = os.getenv('DEBUG_MODE', 'false').lower() == 'true'

# ==== CONFIGURACIÓN DE LOTES ====
BATCH_SIZE = 2000
SLEEP_BETWEEN_BATCHES = 1
ODOO_MODEL = 'rating.rating'
LOAD_MODE = 'incremental'

# Campos que nunca se extraen:
# - access_token: token de seguridad interno de Odoo (permite acceso publico sin login), no aporta valor analitico
EXCLUDED_FIELDS = {'access_token'}

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
    logger.info("=" * 50)

    if not all([url, db, username, password]):
        logger.error("ERROR: Variables de entorno faltantes en .env")
        return False

    logger.info(f"ODOO_URL: {url}")
    logger.info(f" ODOO_DB: {db}")
    logger.info(f"ODOO_USERNAME: {username}")

    if 'nebulan' not in url.lower():
        logger.warning(f"URL no parece ser de Nebulan: {url}")
    else:
        logger.info(f"URL Nebulan detectada")

    logger.info(f"Project configurado: {project_id}")
    logger.info(f"Configuración Nebulan validada")
    return True


def get_available_rating_fields(models, uid):
    """Obtiene TODOS los campos de rating.rating, excepto EXCLUDED_FIELDS y campos binary
    (imagenes/adjuntos no aptos para BigQuery). Devuelve (valid_fields, available_fields)."""
    logger.info("Verificando campos disponibles en rating.rating...")

    try:
        available_fields = models.execute_kw(db, uid, password,
                                               ODOO_MODEL, 'fields_get', [],
                                               {'attributes': ['type', 'relation'], 'context': ODOO_CONTEXT})
    except Exception as e:
        logger.error(f"Error obteniendo campos: {e}")
        return [], {}

    valid_fields = []
    skipped_binary = []

    for field_name, info in available_fields.items():
        if field_name in EXCLUDED_FIELDS:
            continue
        if info.get('type') == 'binary':
            skipped_binary.append(field_name)
            continue
        valid_fields.append(field_name)

    if 'id' in valid_fields:
        valid_fields.remove('id')
    valid_fields = ['id'] + sorted(valid_fields)

    logger.info(f"Campos a extraer: {len(valid_fields)}")
    logger.info(f"   Excluidos (config): {', '.join(EXCLUDED_FIELDS)}")
    if skipped_binary:
        logger.info(f"   Excluidos (binary): {', '.join(skipped_binary)}")

    return valid_fields, available_fields


def create_dynamic_schema(valid_fields, available_fields):
    """Crea esquema BigQuery dinámico. Devuelve (schema, many2one_fields)."""
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
        'reference': 'STRING',
        'many2many': 'STRING',
        'one2many': 'STRING',
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

        else:
            bq_type = field_type_mapping.get(field_type, 'STRING')
            schema.append(bigquery.SchemaField(field_name, bq_type, mode="NULLABLE"))

    return schema, many2one_fields


def build_column_type_map(valid_fields, available_fields, many2one_fields):
    """Mapea cada columna final (BigQuery) a un tipo lógico para limpieza del DataFrame."""
    logical_type_map = {
        'integer': 'int',
        'float': 'float',
        'monetary': 'float',
        'boolean': 'bool',
        'date': 'date',
        'datetime': 'datetime',
    }

    col_types = {'id': 'int'}

    for field_name in valid_fields:
        if field_name == 'id':
            continue

        field_type = available_fields.get(field_name, {}).get('type', 'char')

        if field_name in many2one_fields:
            name_field, id_field = many2one_fields[field_name]
            col_types[name_field] = 'string'
            col_types[id_field] = 'int'
        else:
            col_types[field_name] = logical_type_map.get(field_type, 'string')

    return col_types


def get_odoo_ratings_data(last_write_date=None):
    """Obtiene datos de rating.rating de Odoo. Si last_write_date, filtra incremental.
    Devuelve (all_ratings, valid_fields, available_fields, many2one_fields, models, uid)."""

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
        return [], [], {}, {}, None, None

    logger.info(f"Autenticación exitosa. UID: {uid}")

    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', verbose=False)

    init_odoo_context(models, uid, ODOO_MODEL)

    valid_fields, available_fields = get_available_rating_fields(models, uid)

    if not valid_fields:
        logger.error("No se pudieron obtener campos válidos")
        return [], [], {}, {}, models, uid

    _, many2one_fields = create_dynamic_schema(valid_fields, available_fields)

    logger.info("Obteniendo datos de rating.rating...")

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

    rating_ids = models.execute_kw(db, uid, password,
                                    ODOO_MODEL, 'search',
                                    [domain], {'context': ODOO_CONTEXT})

    logger.info(f"Se encontraron {len(rating_ids)} ratings")

    # Diagnóstico: distribución por modelo de documento relacionado (helpdesk.ticket, project.task, sale.order, etc.)
    try:
        model_count = models.execute_kw(db, uid, password,
                                         ODOO_MODEL, 'read_group',
                                         [[], ['res_model'], ['res_model']],
                                         {'context': ODOO_CONTEXT})
        logger.info("Distribución por documento relacionado (res_model):")
        for group in model_count:
            logger.info(f"   {group.get('res_model') or 'N/A'}: {group['res_model_count']} registros")
    except Exception as e:
        logger.warning(f"No se pudo obtener distribución por res_model: {e}")

    # Procesamiento por lotes
    all_ratings = []
    batch_size = BATCH_SIZE
    total_batches = len(list(create_batches(rating_ids, batch_size)))

    logger.info(f"Procesando en {total_batches} lotes de {batch_size}...")

    for i, ratings_batch in enumerate(create_batches(rating_ids, batch_size), 1):
        try:
            logger.info(f"Procesando lote {i}/{total_batches} ({len(ratings_batch)} registros)...")
            batch_ratings = models.execute_kw(db, uid, password,
                                               ODOO_MODEL, 'read',
                                               [ratings_batch],
                                               {'fields': valid_fields, 'context': ODOO_CONTEXT})
            all_ratings.extend(batch_ratings)

            if i < total_batches:
                time.sleep(SLEEP_BETWEEN_BATCHES)

        except Exception as e:
            logger.error(f"Error en lote {i} de ratings: {e}")
            continue

    logger.info(f"Se obtuvieron {len(all_ratings)} ratings")

    return all_ratings, valid_fields, available_fields, many2one_fields, models, uid


def process_ratings_for_bigquery(ratings, valid_fields, available_fields, many2one_fields):
    """Procesa ratings para BigQuery. Tipado dinámico por field_type de Odoo."""

    logger.info("Procesando ratings para BigQuery...")

    bigquery_ratings = []

    for i, rating in enumerate(ratings):
        row_dict = {}

        for field_name in valid_fields:
            if field_name == 'id':
                row_dict['id'] = safe_convert_to_int(rating.get('id'))
                continue

            value = rating.get(field_name)
            field_type = available_fields.get(field_name, {}).get('type', 'char')

            if field_name in many2one_fields:
                name_field, id_field = many2one_fields[field_name]
                if isinstance(value, list) and len(value) >= 2:
                    row_dict[name_field] = str(value[1])
                    row_dict[id_field] = safe_convert_to_int(value[0])
                else:
                    row_dict[name_field] = ""
                    row_dict[id_field] = None

            elif field_type in ('many2many', 'one2many'):
                if value and isinstance(value, list):
                    row_dict[field_name] = ', '.join(str(v) for v in value)
                else:
                    row_dict[field_name] = ""

            elif field_type in ('datetime', 'date'):
                row_dict[field_name] = str(value) if value not in (False, None) else None

            elif field_type in ('float', 'monetary'):
                row_dict[field_name] = safe_convert_to_float(value)

            elif field_type == 'boolean':
                row_dict[field_name] = safe_convert_to_bool(value)

            elif field_type == 'integer':
                row_dict[field_name] = safe_convert_to_int(value)

            else:  # char, text, html, selection, reference...
                row_dict[field_name] = str(value) if value not in (False, None) else ""

        bigquery_ratings.append(row_dict)

        if (i + 1) % 500 == 0:
            logger.info(f"   Procesados {i + 1}/{len(ratings)}...")

    logger.info(f"{len(bigquery_ratings)} ratings procesados")
    return bigquery_ratings


def get_bigquery_client():
    """Obtiene cliente de BigQuery"""
    credentials, _ = google.auth.default()
    return bigquery.Client(credentials=credentials, project=project_id)


def clean_dataframe_for_bigquery(df, col_types):
    """Limpia DataFrame para BigQuery usando el tipo lógico de cada columna."""
    df_clean = df.copy()

    for col in df_clean.columns:
        col_type = col_types.get(col, 'string')

        if col_type == 'int':
            df_clean[col] = df_clean[col].apply(safe_convert_to_int)
            df_clean[col] = df_clean[col].astype('Int64')
        elif col_type == 'float':
            df_clean[col] = df_clean[col].apply(safe_convert_to_float)
        elif col_type == 'bool':
            df_clean[col] = df_clean[col].apply(safe_convert_to_bool)
        elif col_type == 'date':
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce').dt.date
        elif col_type == 'datetime':
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
            try:
                df_clean[col] = df_clean[col].dt.tz_localize(None)
            except (TypeError, AttributeError):
                pass
        else:
            df_clean[col] = df_clean[col].astype(str)
            df_clean[col] = df_clean[col].replace(['nan', 'None', 'False', '<NA>'], '')

    return df_clean


def load_ratings_to_bigquery(client, ratings_data, schema, col_types, is_incremental=False):
    """Carga ratings a tabla temporal y luego MERGE a tabla final.
    is_incremental=True omite el DELETE-not-in-temp (huerfanos via reconcile FULL).
    Devuelve (success, inserts, updates)."""
    if not ratings_data:
        logger.warning("No hay datos de ratings para cargar")
        return True, 0, 0

    logger.info(f"[LOAD] Cargando {len(ratings_data)} ratings a tabla temporal...")

    df = pd.DataFrame(ratings_data)
    df = clean_dataframe_for_bigquery(df, col_types)

    temp_full = f"{project_id}.{dataset_id}.{table_rating_temp}"
    final_full = f"{project_id}.{dataset_id}.{table_rating}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
        autodetect=False,
    )

    try:
        job = client.load_table_from_dataframe(df, temp_full, job_config=job_config)
        job.result()
        logger.info(f"[OK] {len(ratings_data)} ratings cargados en tabla temporal")
    except Exception as e:
        logger.error(f"[ERROR] Error cargando tabla temporal: {e}")
        raise

    inserts = len(ratings_data)
    updates = 0
    try:
        try:
            client.get_table(final_full)
        except Exception:
            logger.info("[MERGE] Tabla final no existe, creándola desde temporal")
            create_query = f"""
            CREATE TABLE `{final_full}` AS
            SELECT * FROM `{temp_full}`
            """
            client.query(create_query).result()
            logger.info(f"[OK] Tabla {table_rating} creada")
            client.delete_table(temp_full, not_found_ok=True)
            logger.info(f"[CLEANUP] Tabla temporal eliminada")
            return True, inserts, updates

        try:
            updates = count_existing_matches(client, temp_full, final_full)
            inserts = max(0, len(ratings_data) - updates)
        except Exception:
            pass

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

        if not is_incremental:
            delete_query = f"""
            DELETE FROM `{final_full}` T
            WHERE T.id NOT IN (SELECT id FROM `{temp_full}`)
            """
            client.query(delete_query).result()
            logger.info("   [DELETE] Registros obsoletos eliminados")

        merge_elapsed = time.time() - merge_start

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
        table_ref = f"{project_id}.{dataset_id}.{table_rating}"
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
    WHERE table_name = '{table_rating}'
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
        "table_name": table_rating,
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
    target_ref = f"{project_id}.{dataset_id}.{table_rating}"
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

    logger.info("=" * 80)
    logger.info(f"[START] ETL: {ODOO_MODEL}")
    logger.info(f"[INFO] Tabla destino: {project_id}.{dataset_id}.{table_rating}")
    logger.info(f"[INFO] Control: {table_sync_control}  Margen: {SYNC_SAFETY_MARGIN_MINUTES}min")
    logger.info("=" * 80)

    if not validate_configuration():
        logger.warning("Configuración con advertencias, pero continuando...")

    try:
        client = get_bigquery_client()
        ensure_sync_control_table(client)

        target_full = f"{project_id}.{dataset_id}.{table_rating}"
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

        ratings_data, valid_fields, available_fields, many2one_fields, models, uid = get_odoo_ratings_data(
            last_write_date=cursor_used if is_incremental else None
        )

        if not ratings_data:
            logger.info("[OK] Sin registros para procesar")
            register_sync_execution(client, "SUCCESS", 0, 0, 0,
                                    cursor_used, None, start_time_dt)
            return True

        max_write_date = track_max_write_date(ratings_data, max_write_date)
        total_processed = len(ratings_data)

        processed_ratings = process_ratings_for_bigquery(ratings_data, valid_fields, available_fields, many2one_fields)
        if not processed_ratings:
            logger.error("No se pudieron procesar los ratings")
            register_sync_execution(client, "ERROR", total_processed, 0, 0,
                                    None, "process_ratings vacio", start_time_dt)
            return False

        schema, _ = create_dynamic_schema(valid_fields, available_fields)
        col_types = build_column_type_map(valid_fields, available_fields, many2one_fields)
        success, inserts, updates = load_ratings_to_bigquery(
            client, processed_ratings, schema, col_types, is_incremental=is_incremental
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
