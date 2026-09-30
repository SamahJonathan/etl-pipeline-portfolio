import xmlrpc.client
import os
from dotenv import load_dotenv
import ssl
import socket
from google.cloud import bigquery
import google.auth
import time
import pandas as pd
import base64

# Cargar variables de entorno
import logging

script_dir = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(script_dir, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/users.log", encoding='utf-8'),
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
# Defaults staging (st_*). Override individual via env: GCP_TABLE_EMPLOYEE_PUBLIC, GCP_TABLE_RES_USERS
table_empleados = os.getenv('GCP_TABLE_EMPLOYEE_PUBLIC', 'prod_employee_public')
table_empleados_temp = f"{table_empleados}_temp"
table_usuarios = os.getenv('GCP_TABLE_RES_USERS', 'prod_res_users')
table_usuarios_temp = f"{table_usuarios}_temp"

# Contexto Odoo: español Chile + timezone Santiago.
# allowed_company_ids y active_test se inyectan dinamicamente (init_odoo_context).
ODOO_CONTEXT = {
    'lang': 'es_CL',
    'tz': 'America/Santiago',
}


def init_odoo_context(models, uid, model_name='res.users'):
    """Detecta companias + active_test (solo si el modelo tiene campo 'active')."""
    try:
        info = models.execute_kw(
            db, uid, password, 'res.users', 'read', [[uid]],
            {'fields': ['company_ids']}
        )
        if info and info[0].get('company_ids'):
            ODOO_CONTEXT['allowed_company_ids'] = info[0]['company_ids']
            print(f"   Empresas permitidas: {ODOO_CONTEXT['allowed_company_ids']}")
    except Exception as e:
        print(f"   No se detectaron company_ids: {e}")
    try:
        fields = models.execute_kw(
            db, uid, password, model_name, 'fields_get', [],
            {'attributes': ['type']}
        )
        if 'active' in fields:
            ODOO_CONTEXT['active_test'] = False
            print(f"   {model_name} tiene 'active' - incluyendo archivados")
        else:
            ODOO_CONTEXT.pop('active_test', None)
    except Exception as e:
        print(f"   active_test check fallo: {e}")

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

def safe_convert_to_bool(value):
    """Convierte un valor a boolean de forma segura"""
    if value is None or value == '' or value == 'False':
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ['true', '1', 'yes', 'on']
    return bool(value)

def process_avatar_field(value):
    """Procesa el campo avatar_128 como string base64"""
    if value and value != False:
        try:
            # Si ya es string, asumir que es base64 válido
            if isinstance(value, str):
                # Validar que sea base64 válido
                try:
                    base64.b64decode(value)
                    return value, len(value)
                except:
                    return value, len(value)  # Retornar como está si no es base64 válido
            
            # Si es bytes, convertir a base64 string
            elif isinstance(value, bytes):
                avatar_base64 = base64.b64encode(value).decode('utf-8')
                return avatar_base64, len(avatar_base64)
            
            # Para otros tipos, convertir a string
            else:
                avatar_str = str(value)
                return avatar_str, len(avatar_str)
                
        except Exception as e:
            logger.info(f"Error procesando avatar: {e}")
            return "avatar_processing_error", 0
    else:
        return "", 0

def get_employee_mapping_from_users(models, db, uid, password):
    """
    Estrategia INVERSA: Obtiene el mapeo de usuarios a empleados
    desde la tabla res.users que es más accesible
    ACTUALIZADO: Incluye usuarios inactivos
    """
    logger.info("Obteniendo mapeo usuario->empleado desde res.users (INCLUYENDO INACTIVOS)...")
    
    user_to_employee_mapping = {}
    employee_details = {}
    
    try:
        # PASO 1: Verificar qué campos de empleado están disponibles en res.users
        logger.info("  Verificando campos de empleado en res.users...")
        init_odoo_context(models, uid, 'res.users')
        user_fields_info = models.execute_kw(db, uid, password,
                                            'res.users', 'fields_get',
                                            [['employee_id', 'employee_ids']],
                                            {'context': ODOO_CONTEXT})
        
        available_employee_fields = []
        if 'employee_id' in user_fields_info:
            available_employee_fields.append('employee_id')
            logger.info("  Campo 'employee_id' disponible en res.users")
        
        if 'employee_ids' in user_fields_info:
            available_employee_fields.append('employee_ids')
            logger.info("  Campo 'employee_ids' disponible en res.users")
        
        if not available_employee_fields:
            logger.warning("  No se encontraron campos de empleado en res.users")
            return user_to_employee_mapping, employee_details
        
        # PASO 2: Obtener todos los usuarios con información de empleados (INCLUYENDO INACTIVOS)
        logger.info("  Obteniendo usuarios con información de empleados (ACTIVOS + INACTIVOS)...")
        
        # ✅ CORRECCIÓN CRÍTICA: Agregar active_test: False para incluir usuarios inactivos
        user_ids = models.execute_kw(db, uid, password,
                                   'res.users', 'search', 
                                   [[]], {'context': ODOO_CONTEXT})
        
        logger.info(f"  Procesando {len(user_ids)} usuarios (incluyendo inactivos)...")
        
        # ✅ DIAGNÓSTICO: Mostrar distribución por estado activo
        try:
            active_count = models.execute_kw(db, uid, password,
                                           'res.users', 'read_group',
                                           [[], ['active'], ['active']], 
                                           {'context': ODOO_CONTEXT})
            
            logger.info(f"  Distribución de usuarios por estado:")
            for group in active_count:
                state = "Activos" if group['active'] else "Inactivos" 
                logger.info(f"     {state}: {group['active_count']} usuarios")
        except Exception as e:
            logger.warning(f"    No se pudo obtener distribución por estado: {e}")
        
        user_fields_to_read = ['id', 'name', 'login', 'email', 'active'] + available_employee_fields
        
        # Procesar usuarios en lotes
        for i, batch_ids in enumerate(create_batches(user_ids, 500), 1):
            try:
                users_data = models.execute_kw(db, uid, password,
                                             'res.users', 'read',
                                             [batch_ids],
                                             {'fields': user_fields_to_read, 'context': ODOO_CONTEXT})
                
                for user in users_data:
                    user_id = user['id']
                    user_name = user.get('name', f"User {user_id}")
                    user_active = user.get('active', True)
                    
                    # Mapeo por employee_id (many2one)
                    if user.get('employee_id'):
                        if isinstance(user['employee_id'], list):
                            emp_id = user['employee_id'][0]
                            emp_name = user['employee_id'][1]
                        else:
                            emp_id = user['employee_id']
                            emp_name = f"Employee {emp_id}"
                        
                        user_to_employee_mapping[user_id] = emp_id
                        employee_details[emp_id] = {
                            'id': emp_id,
                            'name': emp_name,
                            'user_id': user_id,
                            'user_name': user_name,
                            'user_login': user.get('login', ''),
                            'user_email': user.get('email', ''),
                            'user_active': user_active
                        }
                    
                    # Mapeo por employee_ids (many2many)
                    if user.get('employee_ids'):
                        emp_ids = user['employee_ids'] if isinstance(user['employee_ids'], list) else [user['employee_ids']]
                        for emp_id in emp_ids:
                            if emp_id not in employee_details:  # No sobrescribir si ya existe
                                user_to_employee_mapping[user_id] = emp_id
                                employee_details[emp_id] = {
                                    'id': emp_id,
                                    'name': f"Employee {emp_id}",
                                    'user_id': user_id,
                                    'user_name': user_name,
                                    'user_login': user.get('login', ''),
                                    'user_email': user.get('email', ''),
                                    'user_active': user_active
                                }
                
                logger.info(f"  Lote usuarios {i} procesado")
                time.sleep(0.3)
                
            except Exception as e:
                logger.warning(f"  Error en lote usuarios {i}: {e}")
                continue
        
        logger.info(f"  Mapeo usuario->empleado: {len(user_to_employee_mapping)} relaciones")
        logger.info(f"  Detalles de empleados: {len(employee_details)} empleados")
        
        # PASO 3: Intentar obtener más detalles de empleados desde hr.employee.public
        logger.info("  Enriqueciendo datos desde hr.employee.public...")
        try:
            emp_ids_found = list(employee_details.keys())
            if emp_ids_found:
                # ✅ CORRECCIÓN: Buscar empleados incluyendo inactivos
                existing_public_emp_ids = models.execute_kw(db, uid, password,
                                                          'hr.employee.public', 'search',
                                                          [[['id', 'in', emp_ids_found]]],
                                                          {'context': ODOO_CONTEXT})
                
                if existing_public_emp_ids:
                    logger.info(f"    {len(existing_public_emp_ids)} empleados encontrados en hr.employee.public")
                    
                    # Obtener nombres reales de empleados
                    public_emp_data = models.execute_kw(db, uid, password,
                                                      'hr.employee.public', 'read',
                                                      [existing_public_emp_ids],
                                                      {'fields': ['id', 'name', 'work_email', 'active'], 'context': ODOO_CONTEXT})
                    
                    # Actualizar detalles con información real
                    for emp in public_emp_data:
                        if emp['id'] in employee_details:
                            employee_details[emp['id']]['name'] = emp.get('name', employee_details[emp['id']]['name'])
                            employee_details[emp['id']]['work_email'] = emp.get('work_email', '')
                    
                    logger.info(f"    Datos enriquecidos para {len(public_emp_data)} empleados")
                else:
                    logger.warning("    No se encontraron empleados coincidentes en hr.employee.public")
        except Exception as e:
            logger.warning(f"    Error enriqueciendo datos: {e}")
        
    except Exception as e:
        logger.error(f"  Error general obteniendo mapeo: {e}")
    
    return user_to_employee_mapping, employee_details

def get_odoo_employees():
    """Obtiene empleados públicos de Odoo ACTUALIZADO: Incluye empleados inactivos"""
    # Crear contexto SSL si es HTTPS
    if url.startswith('https'):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    
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
        return False
    
    logger.info(f"Autenticación exitosa. UID: {uid}")
    
    # Crear proxy para el modelo
    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', verbose=False)
    
    # Verificar campos disponibles en hr.employee.public
    logger.info("Verificando campos disponibles en hr.employee.public...")
    try:
        fields_info = models.execute_kw(db, uid, password,
                                      'hr.employee.public', 'fields_get', [[]],
                                      {'context': ODOO_CONTEXT})
        available_fields = list(fields_info.keys())
        logger.info(f"  Campos disponibles: {len(available_fields)}")
        
        # Campos base que queremos extraer
        desired_fields = ['id', 'name', 'work_phone', 'work_email', 'department_id',
                         'job_id', 'parent_id', 'coach_id', 'leave_manager_id',
                         'employee_id', 'active', 'company_id', 'resource_id']
        
        # Filtrar solo campos que existen
        employee_fields = []
        
        for field in desired_fields:
            if field in available_fields:
                employee_fields.append(field)
                logger.info(f"  Campo '{field}' disponible")
            else:
                logger.warning(f"  Campo '{field}' no disponible")
        
        # Verificar avatar
        avatar_field = None
        avatar_alternatives = ['avatar_128']
        for avatar_candidate in avatar_alternatives:
            if avatar_candidate in available_fields:
                employee_fields.append(avatar_candidate)
                avatar_field = avatar_candidate
                logger.info(f"  Campo de avatar encontrado: '{avatar_candidate}'")
                break
        
        if not avatar_field:
            logger.error("  No se encontró campo de avatar")
        
    except Exception as e:
        logger.warning(f"  Error verificando campos: {e}")
        employee_fields = ['id', 'name', 'work_phone', 'work_email', 'department_id',
                          'job_id', 'parent_id', 'coach_id', 'leave_manager_id',
                          'employee_id', 'active', 'company_id', 'resource_id']
        avatar_field = None
    
    logger.info(f"  Campos a extraer: {employee_fields}")
    
    # Obtener empleados públicos INCLUYENDO INACTIVOS
    logger.info("Procesando empleados públicos (ACTIVOS + INACTIVOS)...")
    
    # ✅ CORRECCIÓN CRÍTICA: Agregar active_test: False para incluir empleados inactivos
    employee_ids = models.execute_kw(db, uid, password,
                                   'hr.employee.public', 'search', 
                                   [[]], {'context': ODOO_CONTEXT})
    
    logger.info(f"  Se encontraron {len(employee_ids)} empleados públicos TOTALES (incluyendo inactivos)")
    
    # ✅ DIAGNÓSTICO: Mostrar distribución por estado activo
    try:
        active_count = models.execute_kw(db, uid, password,
                                       'hr.employee.public', 'read_group',
                                       [[], ['active'], ['active']], 
                                       {'context': ODOO_CONTEXT})
        
        logger.info(f"  Distribución de empleados por estado:")
        for group in active_count:
            state = "Activos" if group['active'] else "Inactivos" 
            logger.info(f"     {state}: {group['active_count']} empleados")
    except Exception as e:
        logger.warning(f"    No se pudo obtener distribución por estado: {e}")
    
    if not employee_ids:
        logger.error("No se encontraron empleados públicos.")
        return False
    
    # Procesar en lotes
    all_employees = []
    employees_batch_size = 2000
    
    total_employee_batches = len(list(create_batches(employee_ids, employees_batch_size)))
    
    for i, employee_ids_batch in enumerate(create_batches(employee_ids, employees_batch_size), 1):
        try:
            logger.info(f"  Procesando lote {i}/{total_employee_batches} ({len(employee_ids_batch)} empleados)...")
            batch_employees = models.execute_kw(db, uid, password,
                                              'hr.employee.public', 'read',
                                              [employee_ids_batch],
                                              {'fields': employee_fields, 'context': ODOO_CONTEXT})
            all_employees.extend(batch_employees)
            
            if i < total_employee_batches:
                time.sleep(0.5)
                
        except Exception as e:
            logger.error(f"  Error en lote {i}: {e}")
            continue
    
    logger.info(f"Se obtuvieron {len(all_employees)} empleados")
    
    # Procesar para BigQuery
    logger.info("Procesando empleados para BigQuery...")
    
    bigquery_employees = []
    
    for employee in all_employees:
        row_dict = {}
        
        for field in employee_fields:
            value = employee.get(field)
            
            # Campos many2one
            if field in ['department_id', 'job_id', 'parent_id', 'coach_id', 
                        'leave_manager_id', 'company_id', 'resource_id']:
                if isinstance(value, list) and len(value) > 1:
                    row_dict[field.replace('_id', '')] = str(value[1])
                    row_dict[f"{field}_value"] = safe_convert_to_int(value[0])
                else:
                    row_dict[field.replace('_id', '')] = ""
                    row_dict[f"{field}_value"] = None
            
            # Campo boolean
            elif field == 'active':
                row_dict[field] = safe_convert_to_bool(value)
            
            # Campo ID principal
            elif field == 'id':
                row_dict[field] = safe_convert_to_int(value)
            
            # Campos de avatar - NUEVO: procesar como string base64
            elif field in ['avatar_128']:
                avatar_base64, avatar_size = process_avatar_field(value)
                row_dict['avatar'] = avatar_base64
                row_dict['avatar_size'] = avatar_size
            
            # Campos char/text
            else:
                row_dict[field] = str(value) if value not in [False, None] else ""
        
        # Asegurar campos de avatar
        if 'avatar' not in row_dict:
            row_dict['avatar'] = ""
        if 'avatar_size' not in row_dict:
            row_dict['avatar_size'] = 0
        
        bigquery_employees.append(row_dict)
    
    logger.info(f"Se procesaron {len(bigquery_employees)} empleados para BigQuery.")
    return bigquery_employees

def get_odoo_users():
    """Obtiene usuarios de Odoo CON información de empleados para BigQuery ACTUALIZADO: Incluye usuarios inactivos"""
    if url.startswith('https'):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    
    socket.setdefaulttimeout(120)
    
    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', verbose=False)
    uid = common.authenticate(db, username, password, {})
    
    if not uid:
        logger.error("Error de autenticación para usuarios.")
        return False
    
    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', verbose=False)
    
    # PASO 1: Obtener mapeo usuario->empleado (ya incluye inactivos)
    user_to_employee_mapping, employee_details = get_employee_mapping_from_users(models, db, uid, password)
    
    # PASO 2: Verificar campos disponibles en res.users
    logger.info("Verificando campos disponibles en res.users...")
    try:
        fields_info = models.execute_kw(db, uid, password,
                                      'res.users', 'fields_get', [[]],
                                      {'context': ODOO_CONTEXT})
        available_fields = list(fields_info.keys())
        logger.info(f"  Campos disponibles: {len(available_fields)}")
        
        # Campos básicos + campos de empleado
        desired_fields = ['id', 'name', 'login', 'state', 'company_id', 'active', 'email', 'partner_id']
        optional_fields = ['out_provider_id', 'oauth_provider_id', 'oauth_uid', 'create_date', 'write_date']
        employee_fields = ['employee_id', 'employee_ids']  # Campos de empleado
        
        user_fields = []
        for field in desired_fields:
            if field in available_fields:
                user_fields.append(field)
            else:
                logger.warning(f"  Campo '{field}' no disponible")
        
        for field in optional_fields:
            if field in available_fields:
                user_fields.append(field)
                logger.info(f"  Campo opcional '{field}' encontrado")
        
        for field in employee_fields:
            if field in available_fields:
                user_fields.append(field)
                logger.info(f"  Campo de empleado '{field}' encontrado")
        
    except Exception as e:
        logger.warning(f"  Error verificando campos: {e}")
        user_fields = ['id', 'name', 'login', 'state', 'company_id']
    
    logger.info(f"  Campos a extraer: {user_fields}")
    
    # PASO 3: Obtener usuarios INCLUYENDO INACTIVOS
    logger.info("Procesando usuarios (ACTIVOS + INACTIVOS)...")
    
    # ✅ CORRECCIÓN CRÍTICA: Agregar active_test: False para incluir usuarios inactivos
    user_ids = models.execute_kw(db, uid, password,
                               'res.users', 'search', 
                               [[]], {'context': ODOO_CONTEXT})
    
    logger.info(f"  Se encontraron {len(user_ids)} usuarios TOTALES (incluyendo inactivos)")
    
    # ✅ DIAGNÓSTICO: Mostrar distribución por estado activo
    try:
        active_count = models.execute_kw(db, uid, password,
                                       'res.users', 'read_group',
                                       [[], ['active'], ['active']], 
                                       {'context': ODOO_CONTEXT})
        
        logger.info(f"  Distribución de usuarios por estado:")
        for group in active_count:
            state = "Activos" if group['active'] else "Inactivos" 
            logger.info(f"     {state}: {group['active_count']} usuarios")
    except Exception as e:
        logger.warning(f"    No se pudo obtener distribución por estado: {e}")
    
    if not user_ids:
        logger.error("No se encontraron usuarios.")
        return False
    
    all_users = []
    users_batch_size = 500
    
    total_user_batches = len(list(create_batches(user_ids, users_batch_size)))
    
    for i, user_ids_batch in enumerate(create_batches(user_ids, users_batch_size), 1):
        try:
            logger.info(f"  Procesando lote {i}/{total_user_batches} ({len(user_ids_batch)} usuarios)...")
            batch_users = models.execute_kw(db, uid, password,
                                          'res.users', 'read',
                                          [user_ids_batch],
                                          {'fields': user_fields, 'context': ODOO_CONTEXT})
            all_users.extend(batch_users)
            
            if i < total_user_batches:
                time.sleep(0.5)
                
        except Exception as e:
            logger.error(f"  Error en lote {i}: {e}")
            continue
    
    logger.info(f"Se obtuvieron {len(all_users)} usuarios")
    
    # PASO 4: Procesar usuarios para BigQuery CON información de empleados
    logger.info("Procesando usuarios para BigQuery...")
    
    bigquery_users = []
    users_with_employees = 0
    active_users = 0
    inactive_users = 0
    
    for user in all_users:
        row_dict = {}
        user_id = user['id']
        user_active = user.get('active', True)
        
        # Contadores para diagnóstico
        if user_active:
            active_users += 1
        else:
            inactive_users += 1
        
        # Procesar campos básicos de usuario
        for field in user_fields:
            value = user.get(field)
            
            # Campos many2one (excepto employee_id y employee_ids)
            if field in ['company_id', 'partner_id', 'oauth_provider_id', 'out_provider_id']:
                if isinstance(value, list) and len(value) > 1:
                    row_dict[field.replace('_id', '')] = str(value[1])
                    row_dict[f"{field}_value"] = safe_convert_to_int(value[0])
                else:
                    row_dict[field.replace('_id', '')] = ""
                    row_dict[f"{field}_value"] = None
            
            # Campos de empleado (NUEVOS)
            elif field == 'employee_id':
                if isinstance(value, list) and len(value) > 1:
                    row_dict['employee'] = str(value[1])
                    row_dict['employee_id_value'] = safe_convert_to_int(value[0])
                    users_with_employees += 1
                else:
                    row_dict['employee'] = ""
                    row_dict['employee_id_value'] = None
            
            elif field == 'employee_ids':
                # Para many2many, tomar el primer empleado si existe
                if isinstance(value, list) and len(value) > 0:
                    first_emp_id = value[0]
                    if first_emp_id in employee_details:
                        row_dict['employee_ids_first'] = employee_details[first_emp_id]['name']
                        row_dict['employee_ids_first_value'] = first_emp_id
                    else:
                        row_dict['employee_ids_first'] = f"Employee {first_emp_id}"
                        row_dict['employee_ids_first_value'] = first_emp_id
                    
                    # Contar total de empleados vinculados
                    row_dict['employee_ids_count'] = len(value)
                    if not row_dict.get('employee_id_value'):  # Solo contar si no tenemos employee_id
                        users_with_employees += 1
                else:
                    row_dict['employee_ids_first'] = ""
                    row_dict['employee_ids_first_value'] = None
                    row_dict['employee_ids_count'] = 0
            
            # Campo boolean
            elif field == 'active':
                row_dict[field] = safe_convert_to_bool(value)
            
            # Campo ID principal
            elif field == 'id':
                row_dict[field] = safe_convert_to_int(value)
            
            # Campos char/text y selection
            else:
                row_dict[field] = str(value) if value not in [False, None] else ""
        
        # Asegurar que los campos de empleado estén presentes aunque no se hayan extraído
        if 'employee' not in row_dict:
            row_dict['employee'] = ""
        if 'employee_id_value' not in row_dict:
            row_dict['employee_id_value'] = None
        if 'employee_ids_first' not in row_dict:
            row_dict['employee_ids_first'] = ""
        if 'employee_ids_first_value' not in row_dict:
            row_dict['employee_ids_first_value'] = None
        if 'employee_ids_count' not in row_dict:
            row_dict['employee_ids_count'] = 0
        if 'employee_work_email' not in row_dict:
            row_dict['employee_work_email'] = ''
        
        # Agregar información adicional desde el mapeo obtenido
        if user_id in user_to_employee_mapping:
            mapped_emp_id = user_to_employee_mapping[user_id]
            if mapped_emp_id in employee_details:
                emp_detail = employee_details[mapped_emp_id]
                # Si no tenemos employee_id directo, usar el mapeo
                if not row_dict['employee_id_value']:
                    row_dict['employee'] = emp_detail['name']
                    row_dict['employee_id_value'] = mapped_emp_id
                
                # Agregar campos adicionales de empleado
                row_dict['employee_work_email'] = emp_detail.get('work_email', '')
        
        bigquery_users.append(row_dict)
    
    logger.info(f"Se procesaron {len(bigquery_users)} usuarios para BigQuery.")
    logger.info(f"Usuarios activos: {active_users}")
    logger.info(f"Usuarios inactivos: {inactive_users}")
    logger.info(f"Usuarios con empleados vinculados: {users_with_employees}")
    return bigquery_users

def get_bigquery_client():
    """Obtiene cliente de BigQuery"""
    credentials, _ = google.auth.default()
    return bigquery.Client(credentials=credentials, project=project_id)


def get_employee_schema():
    """Retorna el esquema de BigQuery para empleados"""
    return [
        bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("work_phone", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("work_email", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("department", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("department_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("job", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("job_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("parent", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("parent_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("coach", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("coach_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("leave_manager", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("leave_manager_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("employee_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("active", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("company", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("company_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("resource", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("resource_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("avatar", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("avatar_size", "INTEGER", mode="NULLABLE"),
    ]


def get_users_schema():
    """Retorna el esquema de BigQuery para usuarios"""
    return [
        bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("login", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("state", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("company", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("company_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("active", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("partner", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("partner_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("oauth_provider", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("oauth_provider_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("oauth_uid", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("create_date", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("write_date", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("employee", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("employee_id_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("employee_ids_first", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("employee_ids_first_value", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("employee_ids_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("employee_work_email", "STRING", mode="NULLABLE"),
    ]

def clean_dataframe_for_bigquery(df):
    """Limpia y prepara el DataFrame para BigQuery con manejo robusto de tipos"""
    df_clean = df.copy()
    
    for col in df_clean.columns:
        if col.endswith('_value') or col in ['id', 'avatar_size', 'employee_ids_count']:
            df_clean[col] = df_clean[col].apply(safe_convert_to_int)
            df_clean[col] = df_clean[col].astype('Int64')
            
        elif col in ['active']:
            df_clean[col] = df_clean[col].apply(safe_convert_to_bool)
            
        else:
            df_clean[col] = df_clean[col].astype(str)
            df_clean[col] = df_clean[col].replace(['nan', 'None', 'False', '<NA>'], '')
    
    return df_clean

def load_employees_to_bigquery(client, employee_data):
    """Carga empleados a tabla temporal y reemplaza tabla final"""
    if not employee_data:
        logger.warning("No hay datos de empleados para cargar")
        return

    logger.info(f"[LOAD] Cargando {len(employee_data)} registros de empleados a tabla temporal...")

    df = pd.DataFrame(employee_data)
    df = clean_dataframe_for_bigquery(df)

    schema = get_employee_schema()
    temp_full = f"{project_id}.{dataset_id}.{table_empleados_temp}"
    final_full = f"{project_id}.{dataset_id}.{table_empleados}"

    # Paso 1: Cargar a tabla temporal
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
        autodetect=False,
    )

    try:
        job = client.load_table_from_dataframe(df, temp_full, job_config=job_config)
        job.result()
        logger.info(f"[OK] {len(employee_data)} registros cargados en tabla temporal")
    except Exception as e:
        logger.error(f"[ERROR] Error cargando tabla temporal empleados: {e}")
        return

    # Paso 2: Reemplazar tabla final (atómico)
    try:
        logger.info("[REPLACE] Reemplazando tabla final empleados...")
        merge_start = time.time()

        replace_query = f"""
        CREATE OR REPLACE TABLE `{final_full}` AS
        SELECT * FROM `{temp_full}`
        """
        client.query(replace_query).result()

        merge_elapsed = time.time() - merge_start

        table = client.get_table(final_full)
        logger.info(f"[SUCCESS] Reemplazo EXITOSO empleados")
        logger.info(f"   - Registros en tabla: {table.num_rows:,}")
        logger.info(f"   - Tiempo: {merge_elapsed:.1f}s")

    except Exception as e:
        logger.error(f"[ERROR] Error reemplazando tabla empleados: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        client.delete_table(temp_full, not_found_ok=True)
        logger.info(f"[CLEANUP] Tabla temporal empleados eliminada")


def load_users_to_bigquery(client, user_data):
    """Carga usuarios a tabla temporal y reemplaza tabla final"""
    if not user_data:
        logger.warning("No hay datos de usuarios para cargar")
        return

    logger.info(f"[LOAD] Cargando {len(user_data)} registros de usuarios a tabla temporal...")

    df = pd.DataFrame(user_data)
    df = clean_dataframe_for_bigquery(df)

    schema = get_users_schema()
    temp_full = f"{project_id}.{dataset_id}.{table_usuarios_temp}"
    final_full = f"{project_id}.{dataset_id}.{table_usuarios}"

    # Paso 1: Cargar a tabla temporal
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
        autodetect=False,
    )

    try:
        job = client.load_table_from_dataframe(df, temp_full, job_config=job_config)
        job.result()
        logger.info(f"[OK] {len(user_data)} registros cargados en tabla temporal")
    except Exception as e:
        logger.error(f"[ERROR] Error cargando tabla temporal usuarios: {e}")
        return

    # Paso 2: Reemplazar tabla final (atómico)
    try:
        logger.info("[REPLACE] Reemplazando tabla final usuarios...")
        merge_start = time.time()

        replace_query = f"""
        CREATE OR REPLACE TABLE `{final_full}` AS
        SELECT * FROM `{temp_full}`
        """
        client.query(replace_query).result()

        merge_elapsed = time.time() - merge_start

        table = client.get_table(final_full)
        logger.info(f"[SUCCESS] Reemplazo EXITOSO usuarios")
        logger.info(f"   - Registros en tabla: {table.num_rows:,}")
        logger.info(f"   - Tiempo: {merge_elapsed:.1f}s")

    except Exception as e:
        logger.error(f"[ERROR] Error reemplazando tabla usuarios: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        client.delete_table(temp_full, not_found_ok=True)
        logger.info(f"[CLEANUP] Tabla temporal usuarios eliminada")


def main():
    """Entry point para el orquestador. Carga empleados y usuarios independientemente:
    si una tabla falla, la otra se procesa igual."""
    logger.info("ETL USUARIOS Y EMPLEADOS - VERSIÓN ACTUALIZADA CON INACTIVOS")
    logger.info("="*70)

    employee_data = None
    user_data = None
    try:
        employee_data = get_odoo_employees()
    except Exception as e:
        logger.error(f"Error obteniendo empleados: {type(e).__name__}: {e}")
        import traceback; traceback.print_exc()

    try:
        user_data = get_odoo_users()
    except Exception as e:
        logger.error(f"Error obteniendo usuarios: {type(e).__name__}: {e}")
        import traceback; traceback.print_exc()

    client = get_bigquery_client()

    # Tabla 1: Empleados (independiente)
    try:
        if employee_data:
            load_employees_to_bigquery(client, employee_data)
            empleados_activos = len([e for e in employee_data if e.get('active', True)])
            empleados_inactivos = len([e for e in employee_data if not e.get('active', True)])
            empleados_con_avatar = len([e for e in employee_data if e.get('avatar') and e.get('avatar') != ""])
            logger.info("RESUMEN EMPLEADOS:")
            logger.info(f"- Total: {len(employee_data)} (activos: {empleados_activos}, inactivos: {empleados_inactivos}, con avatar: {empleados_con_avatar})")
        else:
            logger.error("No se obtuvieron datos de empleados")
    except Exception as e:
        logger.error(f"Error en ETL empleados: {type(e).__name__}: {e}")
        import traceback; traceback.print_exc()

    # Tabla 2: Usuarios (independiente)
    try:
        if user_data:
            load_users_to_bigquery(client, user_data)
            usuarios_activos = len([u for u in user_data if u.get('active', True)])
            usuarios_inactivos = len([u for u in user_data if not u.get('active', True)])
            usuarios_con_empleados = len([u for u in user_data if u.get('employee_id_value')])
            logger.info("RESUMEN USUARIOS:")
            logger.info(f"- Total: {len(user_data)} (activos: {usuarios_activos}, inactivos: {usuarios_inactivos}, con empleados: {usuarios_con_empleados})")
        else:
            logger.error("No se obtuvieron datos de usuarios")
    except Exception as e:
        logger.error(f"Error en ETL usuarios: {type(e).__name__}: {e}")
        import traceback; traceback.print_exc()

    logger.info("ETL completado - Incluye usuarios y empleados ACTIVOS + INACTIVOS")


if __name__ == "__main__":
    main()
