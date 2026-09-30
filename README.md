# ETL Pipeline — Odoo → BigQuery

Production-grade ETL system that extracts data from an Odoo ERP, transforms it with Python, and loads it into Google BigQuery. Designed for scale and reliability.

## Features

- **Multi-module extraction**: 17 Odoo modules (CRM, Sales, Accounting, Projects, Inventory, HR, Helpdesk, etc.)
- **Grouped execution**: Organized into logical groups (prod, master, ops, rating) to manage dependencies
- **Docker containerization**: Portable deployment on any infrastructure
- **Cloud Run integration**: Serverless execution on Google Cloud Platform
- **Comprehensive logging**: Detailed audit trail of all extractions
- **Resilience**: Timeout handling, retry logic, and error reporting
- **BigQuery optimization**: Incremental loads with proper partitioning and indexing

## Architecture

```
Odoo ERP (XML-RPC API)
    ↓
Python ETL Scripts (pandas, xmlrpc2db)
    ↓
Transformations & Validations
    ↓
Google BigQuery (Data Warehouse)
```

## Stack

- **Language**: Python 3
- **Data Processing**: pandas, numpy
- **Cloud Platform**: Google Cloud Platform (Cloud Run, Artifact Registry)
- **Container**: Docker
- **CI/CD**: Cloud Build
- **Data Warehouse**: BigQuery

## Project Structure

```
├── main.py                          # Orchestrator: runs groups of ETL scripts
├── [module].py                      # Individual ETL scripts (leads, sale_order, etc.)
├── requirements.txt                 # Python dependencies
├── Dockerfile                       # Container image definition
├── cloudbuild.yaml                  # CI/CD pipeline (Cloud Build)
├── Procfile                         # Process definition
└── logs/                            # Runtime logs directory
```

## Setup

### Prerequisites

- Python 3.8+
- Google Cloud SDK (`gcloud` CLI)
- Docker
- Active GCP project with Cloud Run and BigQuery enabled

### Local Installation

```bash
# Clone the repository
git clone https://github.com/SamahJonathan/etl-pipeline-portfolio.git
cd etl-pipeline-portfolio

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Create .env file with your Odoo credentials
cp .env.example .env
# Edit .env with your values:
# ODOO_URL=https://your-odoo-instance.com
# ODOO_DB=your_database
# ODOO_USERNAME=your_username
# ODOO_PASSWORD=your_password
# GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
```

### Run Locally

```bash
# Run a specific group (prod, master, ops, rating)
python main.py prod
```

### Deploy to Google Cloud

```bash
# Push to your repository
git push origin main

# Cloud Build will automatically:
# 1. Build the Docker image
# 2. Push to Artifact Registry
# 3. Deploy as Cloud Run Jobs (one per group)
```

## ETL Groups

### prod
Transactional data: leads, sales orders, invoices, purchases, stock movements.

### master
Reference data: customers, employees, products, currencies, teams.

### ops
Operational data: support tickets, projects, tasks, timesheets, communications.

### rating
Customer ratings and feedback across all modules.

## Monitoring

Each ETL run produces detailed logs in the `logs/` directory:

```
logs/main.log                       # Execution log with timing and status
```

Example output:
```
2026-09-30 15:45:23 - INFO - Ejecutando grupo 'prod' con 6 scripts
2026-09-30 15:45:23 - INFO - [1/6] Ejecutando leads.py (tablas: prod_crm_lead)...
2026-09-30 15:45:28 - INFO - [1/6] leads.py completado en 5.2s
...
2026-09-30 15:48:15 - INFO - RESUMEN FINAL - Grupo 'prod'
2026-09-30 15:48:15 - INFO - Tiempo total: 172.4s
2026-09-30 15:48:15 - INFO - Scripts ejecutados: 6
2026-09-30 15:48:15 - INFO - Exitosos: 6  |  Fallidos: 0
```

## Configuration

### Environment Variables

Create a `.env` file in the project root:

```
ODOO_URL=https://your-odoo-instance.com
ODOO_DB=your_database
ODOO_USERNAME=your_username
ODOO_PASSWORD=your_password

GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
GOOGLE_CLOUD_PROJECT=your-gcp-project-id
BIGQUERY_DATASET=your_dataset
```

### GCP Setup

1. Create a service account with BigQuery and Cloud Run permissions
2. Download the JSON key file
3. Set `GOOGLE_APPLICATION_CREDENTIALS` to point to it

## Performance

Typical execution times per group:

| Group | Modules | Rows | Time |
|-------|---------|------|------|
| prod | 6 | 50K–1M | 2–5 min |
| master | 5 | 10K–100K | 1–3 min |
| ops | 6 | 20K–500K | 2–4 min |
| rating | 1 | 1K–50K | <1 min |

## License

This is a portfolio project showcasing ETL and data engineering practices. Original data sources and client information have been anonymized.

## Author

Jonathan Samah  
[LinkedIn](https://linkedin.com/in/jonathan-samah) · [GitHub](https://github.com/SamahJonathan)
