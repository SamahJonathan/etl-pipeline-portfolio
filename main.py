import subprocess
import sys
import os
import logging
import time
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

chile_tz = timezone(timedelta(hours=-3))

script_dir = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(script_dir, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/main.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Mapeo: script -> tabla(s) destino en BigQuery
SCRIPT_TABLES = {
    "leads.py": ["prod_crm_lead"],
    "sale_order.py": ["prod_sale_order"],
    "sale_order_line.py": ["prod_sale_order_line"],
    "account_moves.py": ["prod_account_move", "prod_account_move_line"],
    "purchase_order.py": ["prod_purchase_order"],
    "stock_move.py": ["prod_stock_move"],
    "partner.py": ["prod_res_partner"],
    "currency.py": ["prod_currency_rate"],
    "team.py": ["prod_crm_team"],
    "product_product.py": ["prod_product_product"],
    "product_template.py": ["prod_product_template"],
    "helpdesk_ticket.py": ["prod_helpdesk_ticket"],
    "project.project.py": ["prod_project_project"],
    "project.task.py": ["prod_project_task"],
    "account_analytic_line.py": ["prod_account_analytic_line"],
    "prod_mail_message_mail_tracking_value.py": ["prod_mail_message", "prod_mail_tracking_value"],
    "res_user_y_employee_public.py": ["prod_employee_public", "prod_res_users"],
    "rating_rating.py": ["prod_rating_rating"],
}

GROUPS = {
    # Orden dentro de cada grupo: primero cabeceras (parents), luego lineas (children)
    "prod": [
        # Cabeceras
        "leads.py",
        "sale_order.py",
        "account_moves.py",      # internamente: move (cabecera) -> move_line (linea)
        "purchase_order.py",
        "stock_move.py",
        # Lineas
        "sale_order_line.py",    # depende de sale_order
    ],
    "master": [
        # Cabeceras
        "partner.py",
        "res_user_y_employee_public.py",
        "currency.py",
        "team.py",
        "product_template.py",   # plantilla (cabecera)
        # Variantes / hijos
        "product_product.py",    # variantes de product_template
    ],
    "ops": [
        # Cabeceras
        "helpdesk_ticket.py",
        "project.project.py",    # proyecto (cabecera)
        "account_analytic_line.py",
        "prod_mail_message_mail_tracking_value.py",  # internamente: message -> tracking
        # Lineas / hijos
        "project.task.py",       # tareas del proyecto
    ],
    "rating": [
        "rating_rating.py",      # calificaciones de clientes (helpdesk, proyectos, ventas, etc.)
    ],
}


def run_group(group_name):
    if group_name not in GROUPS:
        logger.error(f"Grupo '{group_name}' no existe. Grupos disponibles: {list(GROUPS.keys())}")
        sys.exit(1)

    scripts = GROUPS[group_name]
    logger.info(f"Ejecutando grupo '{group_name}' con {len(scripts)} scripts")
    logger.info("=" * 60)

    results = {}
    start_total = time.time()

    for i, script in enumerate(scripts, 1):
        script_path = os.path.join(script_dir, script)
        tables = SCRIPT_TABLES.get(script, [])

        logger.info(f"[{i}/{len(scripts)}] Ejecutando {script} (tablas: {', '.join(tables)})...")
        start = time.time()

        try:
            result = subprocess.run(
                [sys.executable, "-u", script_path],
                timeout=3600,
                capture_output=True,
                text=True,
            )
            elapsed = time.time() - start

            if result.returncode == 0:
                logger.info(f"[{i}/{len(scripts)}] {script} completado en {elapsed:.1f}s")
                results[script] = {"estado": "OK", "tiempo": elapsed}
            else:
                err_msg = result.stderr.strip().split("\n")[-1] if result.stderr.strip() else f"exit {result.returncode}"
                logger.error(f"[{i}/{len(scripts)}] {script} fallo en {elapsed:.1f}s: {err_msg}")
                results[script] = {"estado": "FAIL", "tiempo": elapsed, "error": err_msg[:300]}

        except subprocess.TimeoutExpired:
            logger.error(f"[{i}/{len(scripts)}] {script} timeout (>3600s)")
            results[script] = {"estado": "TIMEOUT", "tiempo": 3600, "error": "Superó los 3600s de límite"}
        except Exception as e:
            logger.error(f"[{i}/{len(scripts)}] {script} error: {e}")
            results[script] = {"estado": "ERROR", "tiempo": 0, "error": str(e)[:300]}

        # Descanso entre scripts para no sobrecargar Odoo
        if i < len(scripts):
            logger.info(f"    Descansando 40s antes del siguiente script...")
            time.sleep(40)

    # ===== RESUMEN FINAL =====
    elapsed_total = time.time() - start_total
    ok_scripts = [s for s, r in results.items() if r["estado"] == "OK"]
    failed_scripts = [s for s, r in results.items() if r["estado"] != "OK"]

    logger.info("")
    logger.info("=" * 60)
    logger.info(f"  RESUMEN FINAL - Grupo '{group_name}'")
    logger.info(f"  Fecha: {datetime.now(chile_tz).strftime('%Y-%m-%d %H:%M:%S')} (Chile)")
    logger.info(f"  Tiempo total: {elapsed_total:.1f}s")
    logger.info(f"  Scripts ejecutados: {len(scripts)}")
    logger.info(f"  Exitosos: {len(ok_scripts)}  |  Fallidos: {len(failed_scripts)}")
    logger.info("=" * 60)

    if ok_scripts:
        logger.info("")
        logger.info("  ETLs EXITOSOS:")
        for s in ok_scripts:
            tablas = ", ".join(SCRIPT_TABLES.get(s, []))
            logger.info(f"    ✓ {s} ({results[s]['tiempo']:.1f}s) -> {tablas}")

    if failed_scripts:
        logger.info("")
        logger.error("  ETLs CON ERROR:")
        for s in failed_scripts:
            r = results[s]
            tablas = ", ".join(SCRIPT_TABLES.get(s, []))
            logger.error(f"    ✗ {s} [{r['estado']}] -> {tablas}")
            logger.error(f"      Detalle: {r.get('error', 'Sin detalle')}")

    logger.info("=" * 60)

    if failed_scripts:
        sys.exit(1)
    else:
        logger.info(f"Todos los {len(scripts)} ETLs completados exitosamente")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Uso: python main.py <grupo>")
        logger.error(f"Grupos disponibles: {list(GROUPS.keys())}")
        sys.exit(1)

    group = sys.argv[1]
    run_group(group)
