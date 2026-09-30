import xmlrpc.client
import os
from dotenv import load_dotenv
import ssl
import socket
from google.cloud import bigquery
import google.auth
import time
import pandas as pd

# Cargar variables de entorno
import logging

script_dir = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(script_dir, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/team.log", encoding='utf-8'),
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
table_crm_team = os.getenv('GCP_TABLE_NAME', 'prod_crm_team')
table_crm_team_temp = f"{table_crm_team}_temp"

ODOO_MODEL = 'crm.team'

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

def safe_convert_to_boolean(value):
    """Convierte un valor a boolean de forma segura"""
    if value is None or value == '':
        return False
    if isinstance(value, str):
        return value.lower() in ['true', '1', 'yes']
    return bool(value)

def get_crm_teams():
    """Obtiene datos de crm.team de Odoo"""
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

    # Campos de crm.team - 15 campos más importantes
    crm_team_fields = [
        'id', 'name', 'active', 'user_id', 'company_id', 'currency_id',
        'member_ids', 'sequence', 'use_leads', 'use_opportunities',
        'invoiced_target', 'create_date', 'write_date', 'color',
        'sale_order_count'
    ]
    
    logger.info("Obteniendo crm.team...")
    
    # Obtener todos los IDs
    team_ids = models.execute_kw(db, uid, password,
                                'crm.team', 'search', [[]],
                                {'context': ODOO_CONTEXT})
    
    logger.info(f"Se encontraron {len(team_ids)} equipos de ventas")
    
    # Procesar en lotes
    all_teams = []
    batch_size = 2000
    total_batches = len(list(create_batches(team_ids, batch_size)))
    
    for i, team_ids_batch in enumerate(create_batches(team_ids, batch_size), 1):
        try:
            logger.info(f"Procesando lote {i}/{total_batches} ({len(team_ids_batch)} registros)...")
            batch_teams = models.execute_kw(db, uid, password,
                                          'crm.team', 'read',
                                          [team_ids_batch],
                                          {'fields': crm_team_fields, 'context': ODOO_CONTEXT})
            all_teams.extend(batch_teams)
            
            if i < total_batches:
                time.sleep(0.5)
                
        except Exception as e:
            logger.error(f"Error en lote {i}: {e}")
            continue
    
    logger.info(f"Se obtuvieron {len(all_teams)} equipos de ventas")
    
    # Procesar para BigQuery
    def process_teams_for_bigquery(teams):
        bigquery_teams = []
        
        for team in teams:
            row_dict = {}
            
            for field in crm_team_fields:
                value = team.get(field)
                
                # Campos many2one
                if field in ['user_id', 'company_id', 'currency_id']:
                    if isinstance(value, list) and len(value) > 1:
                        row_dict[field.replace('_id', '')] = str(value[1])
                        row_dict[f"{field}_value"] = safe_convert_to_int(value[0])
                    else:
                        row_dict[field.replace('_id', '')] = ""
                        row_dict[f"{field}_value"] = None
                
                # Campos many2many
                elif field in ['member_ids']:
                    row_dict[field] = safe_convert_to_string(value)
                
                # Campos enteros
                elif field in ['id', 'sequence', 'color', 'sale_order_count']:
                    row_dict[field] = safe_convert_to_int(value)
                
                # Campos float
                elif field in ['invoiced_target']:
                    row_dict[field] = safe_convert_to_float(value)
                
                # Campos boolean
                elif field in ['active', 'use_leads', 'use_opportunities']:
                    row_dict[field] = safe_convert_to_boolean(value)
                
                # Campos date/datetime
                elif field in ['create_date', 'write_date']:
                    row_dict[field] = safe_convert_to_datetime(value)
                
                # Campos text/char
                else:
                    row_dict[field] = str(value) if value not in [False, None] else ""
            
            bigquery_teams.append(row_dict)
        
        return bigquery_teams
    
    processed_teams = process_teams_for_bigquery(all_teams)
    return processed_teams


def get_bigquery_client():
    """Obtiene cliente de BigQuery"""
    credentials, _ = google.auth.default()
    return bigquery.Client(credentials=credentials, project=project_id)


def get_crm_team_schema():
    """Retorna el esquema de BigQuery para crm.team"""
    return [
        bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("active", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("user", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("user_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("company", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("company_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("currency", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("currency_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("member_ids", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("sequence", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("use_leads", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("use_opportunities", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("invoiced_target", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("create_date", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("write_date", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("color", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("sale_order_count", "INTEGER", mode="NULLABLE"),
    ]

def clean_dataframe_for_bigquery(df):
    """Limpia DataFrame para BigQuery"""
    df_clean = df.copy()
    
    for col in df_clean.columns:
        if col.endswith('_value') or col in ['id', 'sequence', 'color', 'sale_order_count']:
            # Campos enteros
            df_clean[col] = df_clean[col].apply(safe_convert_to_int)
            df_clean[col] = df_clean[col].astype('Int64')
        elif col in ['invoiced_target']:
            # Campos float
            df_clean[col] = df_clean[col].apply(safe_convert_to_float)
        elif col in ['active', 'use_leads', 'use_opportunities']:
            # Campos boolean
            df_clean[col] = df_clean[col].apply(safe_convert_to_boolean)
        elif col in ['create_date', 'write_date']:
            # Campos datetime
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
        else:
            # Campos string
            df_clean[col] = df_clean[col].astype(str)
            df_clean[col] = df_clean[col].replace(['nan', 'None', 'False', '<NA>'], '')
            df_clean[col] = df_clean[col].fillna('')
    
    return df_clean

def load_crm_teams_to_bigquery(client, data):
    """Carga crm teams a tabla temporal y reemplaza tabla final"""
    if not data:
        logger.warning("No hay datos para cargar")
        return

    logger.info(f"[LOAD] Cargando {len(data)} registros a tabla temporal...")

    df = pd.DataFrame(data)
    df = clean_dataframe_for_bigquery(df)

    schema = get_crm_team_schema()
    temp_full = f"{project_id}.{dataset_id}.{table_crm_team_temp}"
    final_full = f"{project_id}.{dataset_id}.{table_crm_team}"

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

    # Paso 2: Reemplazar tabla final (atómico)
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
        import traceback
        logger.error(traceback.format_exc())
    finally:
        client.delete_table(temp_full, not_found_ok=True)
        logger.info(f"[CLEANUP] Tabla temporal eliminada")


def main():
    """Entry point para el orquestador."""
    teams_data = get_crm_teams()

    if not teams_data:
        logger.error("No se obtuvieron datos para procesar")
        return

    client = get_bigquery_client()
    load_crm_teams_to_bigquery(client, teams_data)

    logger.info(f"RESUMEN: {len(teams_data)} equipos de ventas")
    logger.info("ETL completado")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
