# Project 1: Rapid ETL Pipeline Development
## User Guide

**Course:** DATA 430 – Data Engineering Practicum  
**Orchestration Tool:** Apache Airflow 2.10.5  
**Database:** PostgreSQL 16  
**Deployment:** Docker Compose

---

## 1. Introduction

### 1.1 Purpose

This guide provides step-by-step instructions for installing, starting, operating, and troubleshooting the Rapid ETL Pipeline Development project.

The project uses Apache Airflow to orchestrate an automated Extract, Transform, Load (ETL) workflow that processes customer, product, and order data from CSV, JSON, and YAML files and loads the validated information into PostgreSQL.

The pipeline processes 3,000 unique records representing 1,000 customers, 1,000 products, and 1,000 orders. Each dataset is stored in three formats, producing nine source files.

The pipeline consists of ten tasks and supports both manual execution and daily scheduling.

### 1.2 System Requirements

Before installing the project, ensure the following software is available:

| Software | Requirement |
|---|---|
| Docker Desktop | Docker Compose support |
| Python | Version 3.8 or later |
| Git | Installed and available through Terminal |
| Web browser | Access to the Airflow interface |
| Memory | Sufficient RAM to run PostgreSQL and Airflow containers |

The system was developed and tested on an Intel-based Mac with 16 GB of RAM. Docker was configured with 4 CPUs and an 8 GB memory limit.

## 2. Installation and Initial Configuration

### 2.1 Download the Project

Open Terminal and clone the repository:

```bash
git clone https://github.com/shook2327/data-430-project-1.git
cd data-430-project-1
```

The repository contains the Airflow DAG, Python data-generation script, PostgreSQL schema, Docker Compose configuration, and sample datasets.

### 2.2 Configure Environment Variables

Create a `.env` file in the project root using `.env.example` as a template.

The file must include:

```dotenv
POSTGRES_USER=etl_user
POSTGRES_PASSWORD=your_local_password
POSTGRES_DB=etl_project
AIRFLOW_UID=50000
```

Replace `your_local_password` with a password used only for this project.

**Important:** Never upload the `.env` file containing actual credentials to a public repository.

### 2.3 Generate Source Data

The repository includes nine sample source files. To regenerate them, create a Python virtual environment and install the required libraries.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install Faker pandas PyYAML
python scripts/generate_data.py
```

The script generates 1,000 customers, 1,000 products, and 1,000 orders, saving each dataset in CSV, JSON, and YAML formats inside the `data` directory.

### 2.4 Start PostgreSQL

Open Docker Desktop and confirm that its engine is running.

From Terminal, start PostgreSQL:

```bash
docker compose up -d postgres
```

For the current project configuration, create the separate Airflow metadata database:

```bash
docker compose exec postgres psql -U etl_user -d postgres -c "CREATE DATABASE airflow_db;"
```

This initialization is only needed when setting up a new PostgreSQL database volume.

### 2.5 Initialize and Start Apache Airflow

Initialize the Airflow database:

```bash
docker compose up airflow-init
```

Wait for initialization to finish successfully with exit code 0.

Start the remaining containers:

```bash
docker compose up -d
```

Verify the services:

```bash
docker compose ps
```

The PostgreSQL, Airflow webserver, and Airflow scheduler containers should display running statuses.

### 2.6 Create an Airflow Administrator Account

For a new installation, create an administrator account:

```bash
docker compose exec airflow-webserver airflow users create \
  --username admin \
  --firstname Project \
  --lastname Admin \
  --role Admin \
  --email admin@example.com
```

When prompted, enter and confirm a password.

Open http://localhost:8080 in a browser and sign in using the administrator credentials.

The Airflow dashboard should display the `project1_etl_pipeline` DAG.

## 3. Running the ETL Pipeline

### 3.1 Start a Pipeline Run

After signing in to Apache Airflow at http://localhost:8080:

1. Select **DAGs** from the navigation menu.
2. Locate `project1_etl_pipeline`.
3. Enable the DAG using the toggle switch if it is paused.
4. Click the DAG name to open its details.
5. Select **Trigger DAG** (the play button) to start a manual execution.
6. Open the Grid or Graph view to monitor progress.

The DAG is also scheduled to run automatically once per day using the `@daily` setting.

### 3.2 Monitor Task Execution

The pipeline contains ten tasks that execute in the following sequence:

1. `file_validation`
2. `database_setup`
3. `extract_csv`, `extract_json`, and `extract_yaml` (parallel execution)
4. `load_customers`
5. `load_products`
6. `load_orders`
7. `data_validation`
8. `completion`

In the Airflow interface, green indicates successful execution, while red indicates failure. A successful pipeline run should show all ten tasks completed successfully.

To investigate a problem, select the failed task and open its logs.

## 4. Verifying Pipeline Results

### 4.1 Verify PostgreSQL Records

After a successful run, open Terminal from the project directory and execute:

```bash
docker compose exec postgres psql -U etl_user -d etl_project -c "SELECT 'customers' AS table_name, COUNT(*) FROM customers UNION ALL SELECT 'products', COUNT(*) FROM products UNION ALL SELECT 'orders', COUNT(*) FROM orders;"
```

Expected results:

| Table | Expected Records |
|---|---:|
| customers | 1,000 |
| products | 1,000 |
| orders | 1,000 |
| **Total** | **3,000** |

### 4.2 Review Execution History

To view pipeline auditing information, run:

```bash
docker compose exec postgres psql -U etl_user -d etl_project -c "SELECT run_id, status, records_processed FROM etl_audit ORDER BY run_id DESC LIMIT 5;"
```

Successful runs should show `success` with 3,000 records processed.

Failed runs are recorded with a status of `failed`. The current implementation records zero processed records for failed executions.

### 4.3 Review Archived Files

After successful execution, open the `data/archive` directory.

The completion task saves timestamped copies of the nine processed source files. The original files remain available in `data` for future pipeline executions.

## 5. Troubleshooting

### 5.1 Docker Containers Will Not Start

Confirm Docker Desktop is running, then execute:

```bash
docker compose ps
```

If a container has stopped, check its logs:

```bash
docker compose logs --tail=50
```

If a port is already in use, check whether another application is using the configured port.

### 5.2 Airflow DAG Does Not Appear

Confirm `etl_pipeline.py` exists in the `dags` folder.

Check for DAG import errors:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
```

If no errors are reported, refresh the Airflow dashboard and clear any active DAG filters.

### 5.3 File Validation Fails

Confirm that all nine CSV, JSON, and YAML files exist in the `data` directory.

Each file must contain exactly 1,000 records. Missing, corrupted, or incorrectly sized files will cause validation to fail.

If necessary, regenerate the source files using:

```bash
python scripts/generate_data.py
```

### 5.4 Database Loading Fails

Verify that PostgreSQL is running and the database credentials in `.env` are correct.

Review the failed Airflow task logs for connection errors, duplicate primary keys, or foreign key violations.

After correcting the issue, trigger a new DAG run so the pipeline begins with database setup and reloads the complete dataset.

## 6. Stopping and Restarting the Environment

To safely stop the containers without deleting PostgreSQL data:

```bash
docker compose stop
```

To restart the services:

```bash
docker compose up -d
```

Open http://localhost:8080 to return to the Airflow dashboard.

**Important:** Avoid `docker compose down -v`, because it removes Docker volumes containing stored database information.

## 7. Support and Additional Information

For further technical details, refer to the project's `README.md` and `technical_documentation.md`.

The project source code and documentation are available in the GitHub repository:

https://github.com/shook2327/data-430-project-1

The pipeline was developed as an individual data engineering proof-of-concept project.
