import xmlrpc.client
import os
from dotenv import load_dotenv
import ssl
import socket
from google.cloud import bigquery
from google.oauth2 import service_account
import google.auth
import time
import pandas as pd

from datetime import datetime

# Cargar variables de entorno
import logging

script_dir = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(script_dir, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/currency.log", encoding='utf-8'),
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
table_currency_rate = os.getenv('GCP_TABLE_NAME', 'prod_currency_rate')
table_currency_rate_temp = f"{table_currency_rate}_temp"

ODOO_MODEL = 'res.currency.rate'

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

# ==== CONFIGURACIÓN DE LOTES ====
BATCH_SIZE = 4000
SLEEP_AFTER_TABLE_OPS = 5
SLEEP_BETWEEN_BATCHES = 1

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

def safe_convert_to_datetime(value):
    """Convierte un valor a datetime de forma segura"""
    if value is None or value == '' or value == 'False' or value == False:
        return None
    try:
        if isinstance(value, str):
            value = value.strip()
            if value == '' or value.lower() == 'none' or value.lower() == 'false':
                return None
            # Intentar parsear diferentes formatos de fecha
            try:
                return datetime.fromisoformat(value.replace('Z', '+00:00'))
            except:
                return datetime.strptime(value[:19], '%Y-%m-%d %H:%M:%S')
        return value
    except (ValueError, TypeError):
        return None

def safe_convert_to_date(value):
    """Convierte un valor a date de forma segura"""
    if value is None or value == '' or value == 'False' or value == False:
        return None
    try:
        if isinstance(value, str):
            value = value.strip()
            if value == '' or value.lower() == 'none' or value.lower() == 'false':
                return None
            # Extraer solo la fecha
            if ' ' in value:
                value = value.split(' ')[0]
            return datetime.strptime(value, '%Y-%m-%d').date()
        return value
    except (ValueError, TypeError):
        return None

def get_currency_rates():
    """Obtiene datos de res.currency.rate de Odoo"""
    # Configurar SSL
    if url.startswith('https'):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    
    socket.setdefaulttimeout(120)
    
    # Conexión a Odoo
    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', verbose=False)
    version = common.version()
    logger.info(f"Conexión establecida. Versión Odoo: {version.get('server_version', 'N/A')}")
    
    # Autenticación
    uid = common.authenticate(db, username, password, {})
    if not uid:
        logger.error("Error de autenticación")
        return []
    
    logger.info(f"Autenticación exitosa. UID: {uid}")
    
    # Proxy para modelos
    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', verbose=False)

    init_odoo_context(models, uid, ODOO_MODEL)

    # Campos de res.currency.rate
    currency_rate_fields = [
        'id', 'company_id', 'company_rate', 'create_date', 'create_uid',
        'currency_id', 'display_name', 'inverse_company_rate', 'name', 
        'rate', 'write_date', 'write_uid'
    ]
    
    logger.info("Obteniendo res.currency.rate...")
    
    # Obtener todos los IDs
    rate_ids = models.execute_kw(db, uid, password,
                                'res.currency.rate', 'search', [[]],
                                {'context': ODOO_CONTEXT})
    
    logger.info(f"Se encontraron {len(rate_ids)} tasas de moneda")
    
    # Procesar en lotes
    all_rates = []
    batch_size = 2000
    total_batches = len(list(create_batches(rate_ids, batch_size)))
    
    for i, rate_ids_batch in enumerate(create_batches(rate_ids, batch_size), 1):
        try:
            logger.info(f"Procesando lote {i}/{total_batches} ({len(rate_ids_batch)} registros)...")
            batch_rates = models.execute_kw(db, uid, password,
                                           'res.currency.rate', 'read',
                                           [rate_ids_batch],
                                           {'fields': currency_rate_fields, 'context': ODOO_CONTEXT})
            all_rates.extend(batch_rates)
            
            if i < total_batches:
                time.sleep(0.5)
                
        except Exception as e:
            logger.error(f"Error en lote {i}: {e}")
            continue
    
    logger.info(f"Se obtuvieron {len(all_rates)} tasas de moneda")
    
    # Procesar para BigQuery
    def process_rates_for_bigquery(rates):
        bigquery_rates = []
        
        for rate in rates:
            row_dict = {}
            
            for field in currency_rate_fields:
                value = rate.get(field)
                
                # Campos many2one
                if field in ['company_id', 'create_uid', 'currency_id', 'write_uid']:
                    if isinstance(value, list) and len(value) > 1:
                        row_dict[field.replace('_id', '').replace('_uid', '')] = str(value[1])
                        row_dict[f"{field}_value"] = safe_convert_to_int(value[0])
                    else:
                        row_dict[field.replace('_id', '').replace('_uid', '')] = ""
                        row_dict[f"{field}_value"] = None
                
                # Campos float
                elif field in ['company_rate', 'inverse_company_rate', 'rate']:
                    row_dict[field] = safe_convert_to_float(value)
                
                # Campos datetime
                elif field in ['create_date', 'write_date']:
                    row_dict[field] = safe_convert_to_datetime(value)
                
                # Campos date
                elif field in ['display_name', 'name']:
                    row_dict[field] = safe_convert_to_date(value)
                
                # Campos text/char
                else:
                    row_dict[field] = str(value) if value not in [False, None] else ""
            
            bigquery_rates.append(row_dict)
        
        return bigquery_rates
    
    processed_rates = process_rates_for_bigquery(all_rates)
    return processed_rates

def get_bigquery_client():
    """Obtiene cliente de BigQuery"""
    credentials, _ = google.auth.default()
    return bigquery.Client(credentials=credentials, project=project_id)


def get_currency_rate_schema():
    """Retorna el esquema de BigQuery para currency rate"""
    return [
        bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("company", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("company_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("company_rate", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("create_date", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("create", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("create_uid_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("currency", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("currency_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("display_name", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("inverse_company_rate", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("name", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("rate", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("write_date", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("write", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("write_uid_value", "INTEGER", mode="NULLABLE"),
    ]

def clean_dataframe_for_bigquery(df):
    """Limpia DataFrame para BigQuery"""
    df_clean = df.copy()
    
    for col in df_clean.columns:
        if col.endswith('_value') or col == 'id':
            df_clean[col] = df_clean[col].apply(safe_convert_to_int)
            df_clean[col] = df_clean[col].astype('Int64')
        elif col in ['company_rate', 'inverse_company_rate', 'rate']:
            df_clean[col] = df_clean[col].apply(safe_convert_to_float)
        elif col in ['create_date', 'write_date']:
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
        elif col in ['display_name', 'name']:
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce').dt.date
        else:
            df_clean[col] = df_clean[col].astype(str)
            df_clean[col] = df_clean[col].replace(['nan', 'None', 'False', '<NA>'], '')
    
    return df_clean

def load_data_to_table(client, data):
    """Carga datos a tabla temporal y luego MERGE a tabla final"""
    if not data:
        logger.warning(f"No hay datos para cargar en {table_currency_rate}")
        return

    logger.info(f"[LOAD] Cargando {len(data)} registros a tabla temporal...")

    df = pd.DataFrame(data)
    df = clean_dataframe_for_bigquery(df)

    schema = get_currency_rate_schema()
    temp_full = f"{project_id}.{dataset_id}.{table_currency_rate_temp}"
    final_full = f"{project_id}.{dataset_id}.{table_currency_rate}"

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
            logger.info(f"[OK] Tabla {table_currency_rate} creada")
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

        logger.info("[MERGE] Ejecutando MERGE: upsert + eliminar obsoletos")
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
        logger.info("   [MERGE] Upsert completado")

        # Eliminar registros que ya no existen (carga FULL)
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
        logger.info(f"   - Columnas: {len(table.schema)}")
        logger.info(f"   - Tiempo MERGE: {merge_elapsed:.1f}s")

    except Exception as e:
        logger.error(f"[ERROR] Error en MERGE: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        client.delete_table(temp_full, not_found_ok=True)
        logger.info(f"[CLEANUP] Tabla temporal eliminada")


def main():
    """Entry point para el orquestador."""
    logger.info("Iniciando ETL de Currency Rate...")

    rates_data = get_currency_rates()

    if not rates_data:
        logger.warning("Sin datos para cargar")
        return

    client = get_bigquery_client()
    load_data_to_table(client, rates_data)

    logger.info(f"RESUMEN: {len(rates_data)} tasas de moneda")
    logger.info("ETL completado")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
