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
        logging.FileHandler(f"{LOG_DIR}/product_product.log", encoding='utf-8'),
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
table_productos = os.getenv('GCP_TABLE_NAME', 'prod_product_product')
table_productos_temp = f"{table_productos}_temp"
table_sync_control = os.getenv('GCP_TABLE_SYNC_CONTROL', 'etl_sync_control')
SYNC_SAFETY_MARGIN_MINUTES = 5
DEBUG_MODE = os.getenv('DEBUG_MODE', 'false').lower() == 'true'

# ==== CONFIGURACIÓN DE LOTES ====
BATCH_SIZE = 4000
SLEEP_AFTER_TABLE_OPS = 5
SLEEP_BETWEEN_BATCHES = 1
ODOO_MODEL = 'product.product'
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

# ==== MAPPINGS PARA CAMPOS SELECTION ====
SERVICE_TRACKING_MAPPING = {
    'no': 'Nada',
    'task_global_project': 'Tarea',
    'task_in_project': 'Proyecto y tarea',
    'project_only': 'Proyecto'
}

SERVICE_POLICY_MAPPING = {
    'ordered_prepaid': 'Precio fijo o de prepago',
    'delivered_timesheet': 'Según las hojas de horas',
    'delivered_milestones': 'Con base en los objetivos',
    'delivered_manual': 'Basado en cantidad entregada (manual)'
}

DETAILED_TYPE_MAPPING = {
    'consu': 'Consumible',
    'service': 'Servicio',
    'product': 'Producto almacenable'
}

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
        return int(float(value))  # Convertir a float primero para manejar strings como "1.0"
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
    if value is None or value == '' or value == 'False':
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ['true', '1', 'yes', 't', 'y']
    return bool(value)

def get_selection_display_value(value, mapping):
    """Obtiene el valor de display para campos selection"""
    if value and value in mapping:
        return mapping[value]
    return str(value) if value else ""

def get_product_counts(models, db, uid, password):
    """Obtiene conteos de productos por estado activo/inactivo"""
    logger.info("CONTEO DE PRODUCTOS POR ESTADO:")
    logger.info("=" * 50)
    
    try:
        active_count = models.execute_kw(db, uid, password,
                                       'product.product', 'search_count',
                                       [[['active', '=', True]]],
                                       {'context': ODOO_CONTEXT})

        inactive_count = models.execute_kw(db, uid, password,
                                         'product.product', 'search_count',
                                         [[['active', '=', False]]],
                                         {'context': ODOO_CONTEXT})

        total_count = models.execute_kw(db, uid, password,
                              'product.product', 'search_count',
                              [['|', ['active', '=', True], ['active', '=', False]]],
                              {'context': ODOO_CONTEXT})
        
        logger.info(f"Productos ACTIVOS:   {active_count:,}")
        logger.error(f"Productos INACTIVOS: {inactive_count:,}")
        logger.info(f"TOTAL:              {total_count:,}")
        logger.info("=" * 50)
        
        # Verificar que los números cuadren
        if active_count + inactive_count != total_count:
            logger.warning("Advertencia: Los conteos no cuadran exactamente")
        
        return {
            'active': active_count,
            'inactive': inactive_count,
            'total': total_count
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo conteos: {e}")
        return None

def get_odoo_products(last_write_date=None):
    """Obtiene productos del modelo product.product. Si last_write_date, filtra incremental.
    Devuelve (bigquery_products, raw_products, models, uid)."""
    # Crear contexto SSL si es HTTPS
    if url.startswith('https'):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    
    # Aumentar timeout para consultas grandes
    socket.setdefaulttimeout(120)
    
    # Crear conexión con ODOO
    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', verbose=False)
    
    # Verificar versión
    version = common.version()
    logger.info(f"Conexión establecida. Versión Odoo: {version.get('server_version', 'N/A')}")
    
    # Autenticación
    uid = common.authenticate(db, username, password, {})
    
    if not uid:
        logger.error("Error de autenticación. Verifique las credenciales.")
        return False, None, None, None

    logger.info(f"Autenticación exitosa. UID: {uid}")

    # Crear proxy para el modelo
    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', verbose=False)

    init_odoo_context(models, uid, ODOO_MODEL)

    # OBTENER CONTEOS ANTES DEL PROCESAMIENTO
    counts = get_product_counts(models, db, uid, password)
    
    # Campos para product.product - incluye campos específicos de variantes
    product_fields = [
        # Campos básicos de product.product
        'id', 'name', 'display_name', 'default_code', 'barcode', 'active',
        'product_tmpl_id', 'company_id',

        # Campos heredados del template (disponibles en product.product)
        'detailed_type', 'sale_ok', 'purchase_ok', 'recurring_invoice',
        'can_be_expensed', 'product_tooltip', 'planning_enabled',
        'uom_id', 'uom_po_id', 'list_price', 'taxes_id', 'categ_id',
        'service_tracking', 'service_policy',

        # Campos específicos de variantes
        'combination_indices', 'product_variant_ids', 'weight', 'volume',
        'product_variant_count',

        # Campos para cursor incremental
        'write_date', 'create_date',
    ]

    logger.info("Procesando variantes de productos del modelo product.product...")

    # Dominio: TODOS (activos+inactivos) o filtrado incremental
    base_active_or_inactive = ['|', ('active', '=', True), ('active', '=', False)]
    if last_write_date:
        domain = ['&'] + base_active_or_inactive + [
            '|',
            ('write_date', '>', last_write_date),
            ('create_date', '>', last_write_date),
        ]
        logger.info(f"[INCREMENTAL] write_date OR create_date > {last_write_date}")
    else:
        domain = base_active_or_inactive
        logger.info("[FULL] Todos los productos (activos+inactivos)")

    product_ids = models.execute_kw(db, uid, password,
                               'product.product', 'search',
                               [domain],
                               {'context': ODOO_CONTEXT})

    logger.info(f"  Se encontraron {len(product_ids)} variantes")

    if not product_ids:
        logger.info("[INFO] No hay productos para procesar")
        return [], [], models, uid
    
    # Procesar en lotes para evitar timeouts
    all_products = []
    products_batch_size = 2000
    
    total_product_batches = len(list(create_batches(product_ids, products_batch_size)))
    
    for i, product_ids_batch in enumerate(create_batches(product_ids, products_batch_size), 1):
        try:
            logger.info(f"  Procesando lote {i}/{total_product_batches} ({len(product_ids_batch)} productos)...")
            batch_products = models.execute_kw(db, uid, password,
                                              'product.product', 'read',
                                              [product_ids_batch],
                                              {'fields': product_fields, 'context': ODOO_CONTEXT})
            all_products.extend(batch_products)
            
            if i < total_product_batches:
                time.sleep(0.5)
                
        except Exception as e:
            logger.error(f"  Error en lote {i}: {e}")
            continue
    
    logger.info(f"Se obtuvieron {len(all_products)} variantes de productos")
    
    # Optimización: Obtener todos los impuestos de una vez
    logger.info("Obteniendo información de impuestos...")
    all_tax_ids = set()
    for product in all_products:
        if product.get('taxes_id') and isinstance(product['taxes_id'], list):
            all_tax_ids.update(product['taxes_id'])
    
    tax_names_cache = {}
    if all_tax_ids:
        try:
            taxes_data = models.execute_kw(db, uid, password,
                                         'account.tax', 'read',
                                         [list(all_tax_ids)],
                                         {'fields': ['name'], 'context': ODOO_CONTEXT})
            tax_names_cache = {tax['id']: tax['name'] for tax in taxes_data}
            logger.info(f"Cache de {len(tax_names_cache)} impuestos creado")
        except Exception as e:
            logger.warning(f"Error obteniendo impuestos: {e}")
    
    # Optimización: Obtener información de templates
    logger.info("Obteniendo información de templates...")
    all_template_ids = set()
    for product in all_products:
        if product.get('product_tmpl_id') and isinstance(product['product_tmpl_id'], list):
            all_template_ids.add(product['product_tmpl_id'][0])
    
    template_names_cache = {}
    if all_template_ids:
        try:
            templates_data = models.execute_kw(db, uid, password,
                                             'product.template', 'read',
                                             [list(all_template_ids)],
                                             {'fields': ['name'], 'context': ODOO_CONTEXT})
            template_names_cache = {tmpl['id']: tmpl['name'] for tmpl in templates_data}
            logger.info(f"Cache de {len(template_names_cache)} templates creado")
        except Exception as e:
            logger.warning(f"Error obteniendo templates: {e}")
    
    # Optimización: Obtener información completa de categorías
    logger.info("Obteniendo información de categorías...")
    all_category_ids = set()
    for product in all_products:
        if product.get('categ_id') and isinstance(product['categ_id'], list):
            all_category_ids.add(product['categ_id'][0])
    
    category_cache = {}
    if all_category_ids:
        try:
            # Obtener información completa de categorías incluyendo jerarquía
            categories_fields = ['name', 'complete_name', 'parent_id', 'parent_path']
            categories_data = models.execute_kw(db, uid, password,
                                              'product.category', 'read',
                                              [list(all_category_ids)],
                                              {'fields': categories_fields, 'context': ODOO_CONTEXT})
            
            for cat in categories_data:
                category_cache[cat['id']] = {
                    'name': cat.get('name', ''),
                    'complete_name': cat.get('complete_name', ''),
                    'parent_id': cat.get('parent_id')[0] if cat.get('parent_id') and isinstance(cat.get('parent_id'), list) else None,
                    'parent_name': cat.get('parent_id')[1] if cat.get('parent_id') and isinstance(cat.get('parent_id'), list) else '',
                    'parent_path': cat.get('parent_path', '')
                }
            
            logger.info(f"Cache de {len(category_cache)} categorías creado")
        except Exception as e:
            logger.warning(f"Error obteniendo categorías: {e}")
    
    logger.info("Procesando variantes para BigQuery...")
    
    # Procesar productos para BigQuery
    bigquery_products = []
    
    for product in all_products:
        row_dict = {}
        
        for field in product_fields:
            value = product.get(field)
            
            # Campos many2one
            if field in ['uom_id', 'uom_po_id', 'categ_id', 'product_tmpl_id', 'company_id']:
                if isinstance(value, list) and len(value) > 1:
                    row_dict[field.replace('_id', '')] = str(value[1])
                    row_dict[f"{field}_value"] = safe_convert_to_int(value[0])
                else:
                    row_dict[field.replace('_id', '')] = ""
                    row_dict[f"{field}_value"] = None
            
            # Campo many2many - taxes_id (usando cache)
            elif field == 'taxes_id':
                if value and isinstance(value, list):
                    tax_names = []
                    for tax_id in value:
                        if tax_id in tax_names_cache:
                            tax_names.append(tax_names_cache[tax_id])
                        else:
                            tax_names.append(str(tax_id))
                    row_dict[field] = ', '.join(tax_names)
                    row_dict['taxes_id_count'] = len(value)
                else:
                    row_dict[field] = ""
                    row_dict['taxes_id_count'] = 0
            
            # Campo many2many - product_variant_ids
            elif field == 'product_variant_ids':
                if value and isinstance(value, list):
                    row_dict[field] = ', '.join([str(x) for x in value])
                    row_dict['product_variant_ids_count'] = len(value)
                else:
                    row_dict[field] = ""
                    row_dict['product_variant_ids_count'] = 0
            
            # Campos selection con mapping
            elif field == 'detailed_type':
                row_dict[field] = str(value) if value not in [False, None] else ""
                row_dict[f"{field}_display"] = get_selection_display_value(value, DETAILED_TYPE_MAPPING)
            
            elif field == 'service_tracking':
                row_dict[field] = str(value) if value not in [False, None] else ""
                row_dict[f"{field}_display"] = get_selection_display_value(value, SERVICE_TRACKING_MAPPING)
            
            elif field == 'service_policy':
                row_dict[field] = str(value) if value not in [False, None] else ""
                row_dict[f"{field}_display"] = get_selection_display_value(value, SERVICE_POLICY_MAPPING)
            
            # Campos boolean
            elif field in ['sale_ok', 'purchase_ok', 'recurring_invoice', 'can_be_expensed', 
                          'planning_enabled', 'active']:
                row_dict[field] = safe_convert_to_bool(value)
            
            # Campos float/monetarios y medidas
            elif field in ['list_price', 'weight', 'volume']:
                row_dict[field] = safe_convert_to_float(value)
            
            # Campos integer
            elif field in ['id', 'product_variant_count']:
                row_dict[field] = safe_convert_to_int(value)
            
            # Campos char/text
            else:
                row_dict[field] = str(value) if value not in [False, None] else ""
        
        # Agregar nombre del template si está disponible
        template_id = row_dict.get('product_tmpl_id_value')
        if template_id and template_id in template_names_cache:
            row_dict['template_name'] = template_names_cache[template_id]
        else:
            row_dict['template_name'] = ""
        
        # Agregar información completa de la categoría
        category_id = row_dict.get('categ_id_value')
        if category_id and category_id in category_cache:
            cat_info = category_cache[category_id]
            row_dict['category_name'] = cat_info['name']
            row_dict['category_complete_name'] = cat_info['complete_name']
            row_dict['category_parent_id'] = cat_info['parent_id']
            row_dict['category_parent_name'] = cat_info['parent_name']
            row_dict['category_parent_path'] = cat_info['parent_path']
        else:
            row_dict['category_name'] = ""
            row_dict['category_complete_name'] = ""
            row_dict['category_parent_id'] = None
            row_dict['category_parent_name'] = ""
            row_dict['category_parent_path'] = ""
        
        bigquery_products.append(row_dict)

    logger.info(f"Se procesaron {len(bigquery_products)} variantes de productos para BigQuery.")
    return bigquery_products, all_products, models, uid
def get_bigquery_client():
    """Obtiene cliente de BigQuery"""
    credentials, _ = google.auth.default()
    return bigquery.Client(credentials=credentials, project=project_id)


def get_product_schema():
    """Retorna el esquema de BigQuery para variantes de productos"""
    return [
        bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("display_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("default_code", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("barcode", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("active", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("product_tmpl", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("product_tmpl_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("template_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("detailed_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("detailed_type_display", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("sale_ok", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("purchase_ok", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("recurring_invoice", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("can_be_expensed", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("product_tooltip", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("planning_enabled", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("uom", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("uom_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("uom_po", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("uom_po_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("list_price", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("weight", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("volume", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("taxes_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("taxes_id_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("categ", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("categ_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("category_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("category_complete_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("category_parent_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("category_parent_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("category_parent_path", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("service_tracking", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("service_tracking_display", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("service_policy", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("service_policy_display", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("combination_indices", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("product_variant_ids", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("product_variant_ids_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("product_variant_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("company", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("company_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("write_date", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("create_date", "DATETIME", mode="NULLABLE"),
    ]

def clean_dataframe_for_bigquery(df):
    """Limpia y prepara el DataFrame para BigQuery con manejo robusto de tipos"""
    df_clean = df.copy()
    
    for col in df_clean.columns:
        # Campos que deben ser enteros nullable
        if col.endswith('_value') or col.endswith('_count') or col in ['id', 'product_variant_count', 'category_parent_id', 'category_parent_id']:
            df_clean[col] = df_clean[col].apply(safe_convert_to_int)
            df_clean[col] = df_clean[col].astype('Int64')  # Pandas nullable integer

        # Campos float/monetarios y medidas
        elif col in ['list_price', 'weight', 'volume']:
            df_clean[col] = df_clean[col].apply(safe_convert_to_float)

        # Campos boolean
        elif col in ['sale_ok', 'purchase_ok', 'recurring_invoice', 'can_be_expensed',
                    'planning_enabled', 'active']:
            df_clean[col] = df_clean[col].apply(safe_convert_to_bool)

        # Campos DATETIME (cursor incremental)
        elif col in ['write_date', 'create_date']:
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
            # tz-naive para compatibilidad con BigQuery DATETIME
            try:
                df_clean[col] = df_clean[col].dt.tz_localize(None)
            except (TypeError, AttributeError):
                pass

        # Campos de texto - limpiar valores None/nan
        else:
            df_clean[col] = df_clean[col].astype(str)
            df_clean[col] = df_clean[col].replace(['nan', 'None', 'False', '<NA>'], '')

    return df_clean

def load_products_to_bigquery(client, product_data, is_incremental=False):
    """Carga productos a tabla temporal y MERGE a tabla final.
    is_incremental=True omite el DELETE-not-in-temp (huerfanos via reconcile FULL).
    Devuelve (success, inserts, updates)."""
    if not product_data:
        logger.warning("No hay datos de productos para cargar")
        return True, 0, 0

    logger.info(f"[LOAD] Cargando {len(product_data):,} registros a tabla temporal...")

    df = pd.DataFrame(product_data)
    df = clean_dataframe_for_bigquery(df)

    schema = get_product_schema()
    temp_full = f"{project_id}.{dataset_id}.{table_productos_temp}"
    final_full = f"{project_id}.{dataset_id}.{table_productos}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
        autodetect=False,
    )

    try:
        job = client.load_table_from_dataframe(df, temp_full, job_config=job_config)
        job.result()
        logger.info(f"[OK] {len(product_data):,} cargados en tabla temporal")
    except Exception as e:
        logger.error(f"[ERROR] Error cargando tabla temporal: {e}")
        return False, 0, 0

    inserts = len(product_data)
    updates = 0

    try:
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
            return True, inserts, 0

        try:
            updates = count_existing_matches(client, temp_full, final_full)
            inserts = max(0, len(product_data) - updates)
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
            delete_query = f"""
            DELETE FROM `{final_full}` T
            WHERE T.id NOT IN (SELECT id FROM `{temp_full}`)
            """
            client.query(delete_query).result()
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
        table_ref = f"{project_id}.{dataset_id}.{table_productos}"
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
    WHERE table_name = '{table_productos}'
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
        "table_name": table_productos,
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
    target_ref = f"{project_id}.{dataset_id}.{table_productos}"
    try:
        # Buscar TODOS (activos+inactivos) - no aplicamos active_test=False filter
        # porque el ETL tambien incluye archivados explicitamente
        domain = ['|', ('active', '=', True), ('active', '=', False)]
        odoo_ids = models.execute_kw(db, uid, password, ODOO_MODEL, 'search', [domain],
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
    logger.info(f"[INFO] Tabla destino: {project_id}.{dataset_id}.{table_productos}")
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

        target_full = f"{project_id}.{dataset_id}.{table_productos}"
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

        result = get_odoo_products(last_write_date=cursor_used if is_incremental else None)

        if result is False or result is None:
            logger.error("[ERROR] get_odoo_products fallo")
            register_sync_execution(client, "ERROR", 0, 0, 0, None,
                                    "get_odoo_products fallo", start_time_dt)
            return False

        bigquery_products, raw_products, models, uid = result

        if not bigquery_products:
            logger.info("[OK] Sin productos para procesar")
            register_sync_execution(client, "SUCCESS", 0, 0, 0,
                                    cursor_used, None, start_time_dt)
            return True

        max_write_date = track_max_write_date(raw_products, max_write_date)
        total_processed = len(bigquery_products)

        success, inserts, updates = load_products_to_bigquery(
            client, bigquery_products, is_incremental=is_incremental
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
