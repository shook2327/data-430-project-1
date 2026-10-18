
# Project 1: Rapid ETL Pipeline Development

**Course:** DATA 430 – Data Engineering Practicum  
**Project Type:** Individual Sprint Project  
**Orchestration Tool:** Apache Airflow

## Project Overview

This project demonstrates the development of an automated Extract, Transform, Load (ETL) pipeline using Apache Airflow, Python, and PostgreSQL. The pipeline processes synthetic customer, product, and order data stored in CSV, JSON, and YAML formats.

The project simulates a real-world data engineering environment in which data must be collected from multiple file formats, validated, and loaded into a relational database. Apache Airflow orchestrates the workflow, ensuring that each task executes in the proper sequence and that dependent tasks are completed successfully before subsequent tasks begin.

The pipeline processes 3,000 unique records consisting of 1,000 customers, 1,000 products, and 1,000 orders. These records are generated using the Python Faker library and exported into nine source files.

All services run in Docker containers to provide a consistent and reproducible development environment.

## Project Objectives

The primary objectives of this project are to:

- Generate synthetic customer, product, and order datasets using Python and Faker.
- Process CSV, JSON, and YAML source files using Apache Airflow.
- Load validated records into PostgreSQL while maintaining referential integrity.
- Automate the execution and scheduling of ETL tasks.
- Validate database record counts and customer-order relationships.
- Implement error handling, logging, and pipeline execution auditing.
- Demonstrate a functional ETL workflow using Docker Compose.

## Technologies Used

| Technology | Purpose |
|---|---|
| Python | Data generation and ETL programming |
| Faker | Synthetic data generation |
| pandas | Data reading, processing, and validation |
| PyYAML | YAML file processing |
| Apache Airflow 2.10.5 | Workflow orchestration and scheduling |
| PostgreSQL 16 | Relational database storage |
| Docker Compose | Containerized deployment |
| Git and GitHub | Version control and project repository |


## Project Structure

The project is organized into the following directories and files:

```text
Project 1/
├── dags/
│   └── etl_pipeline.py
├── scripts/
│   └── generate_data.py
├── sql/
│   └── schema.sql
├── data/
│   ├── customers.csv
│   ├── customers.json
│   ├── customers.yaml
│   ├── products.csv
│   ├── products.json
│   ├── products.yaml
│   ├── orders.csv
│   ├── orders.json
│   └── orders.yaml
├── docs/
├── docker-compose.yml
├── .env.example
├── .gitignore
└── README.md
```

- **dags/** contains the Apache Airflow DAG that orchestrates the ETL process.
- **scripts/** contains the Python Faker data generation script.
- **sql/** contains the PostgreSQL table definitions.
- **data/** contains the nine generated source files.
- **docs/** contains the technical documentation and user guide.
- **docker-compose.yml** defines the containerized services.
- **.env.example** provides the required environment variable template.

## Setup Instructions

### Prerequisites

The following software is required:

- Docker Desktop with Docker Compose
- Python 3.8 or later
- Git
- A web browser

### Installation and Configuration

1. Clone the GitHub repository:

   ```bash
   git clone https://github.com/shook2327/data-430-project-1.git
   cd data-430-project-1
   ```

2. Create a `.env` file using `.env.example` as a template. Replace the placeholder password with a local development password.

3. Start the PostgreSQL service:

   ```bash
   docker compose up -d postgres
   ```

4. Create the Airflow metadata database:

   ```bash
   docker compose exec postgres psql -U etl_user -d postgres -c "CREATE DATABASE airflow_db;"
   ```

5. Initialize Apache Airflow:

   ```bash
   docker compose up airflow-init
   ```

6. Start the remaining services:

   ```bash
   docker compose up -d
   ```

7. Create an administrator account for the Airflow web interface:

   ```bash
   docker compose exec airflow-webserver airflow users create \
     --username admin \
     --firstname Project \
     --lastname Admin \
     --role Admin \
     --email admin@example.com
   ```

   Enter and confirm a password when prompted.

8. Open Apache Airflow at http://localhost:8080 and sign in.

The environment runs PostgreSQL 16 and Apache Airflow 2.10.5 using Docker Compose.


## Running the ETL Pipeline

Once the Docker containers are running, open Apache Airflow at http://localhost:8080 and log in.

1. Navigate to the DAGs dashboard.
2. Locate `project1_etl_pipeline`.
3. Enable the DAG if it is paused.
4. Select **Trigger DAG** to start a manual execution.
5. Monitor the execution using the Grid or Graph view.
6. Confirm that all 10 tasks complete successfully.

The DAG is also configured to run daily using the `@daily` schedule.

### Pipeline Workflow

The pipeline executes the following tasks:

1. **file_validation:** Verifies that all nine source files exist, are readable, and contain 1,000 records each.
2. **database_setup:** Creates the PostgreSQL tables if necessary and clears existing customer, product, and order records.
3. **extract_csv:** Reads and validates the three CSV files.
4. **extract_json:** Reads and validates the three JSON files.
5. **extract_yaml:** Reads and validates the three YAML files.
6. **load_customers:** Loads 1,000 customer records into PostgreSQL.
7. **load_products:** Loads 1,000 product records into PostgreSQL.
8. **load_orders:** Loads 1,000 orders while maintaining valid customer relationships.
9. **data_validation:** Checks database row counts and foreign key integrity.
10. **completion:** Archives copies of processed files and records a successful pipeline execution.

The extraction tasks run in parallel after database setup. All three must succeed before loading begins. CSV files are used for database insertion to prevent duplicate records across file formats.

## Data Validation and Error Handling

The pipeline includes validation and error handling throughout its execution.

- Each source file must contain exactly 1,000 records.
- Customer IDs and email addresses are checked for duplicates.
- Product and order IDs are checked for duplicates.
- PostgreSQL enforces the relationship between orders and customers.
- Database validation confirms that each main table contains 1,000 records.
- Database transactions are committed after successful loading and rolled back when errors occur.
- Airflow logs task execution results and error messages.
- The `etl_audit` table records successful and failed pipeline executions.

### Testing Results

The pipeline successfully processed 3,000 unique records across the customers, products, and orders tables.

A controlled failure test was performed by temporarily renaming `customers.csv`. The file validation task detected the missing file, the DAG failed, and the failure was recorded in the audit table with zero records processed.

After restoring the file, the pipeline was executed again and all 10 tasks completed successfully. The audit table recorded the successful run with 3,000 records processed.

## Troubleshooting

**Docker containers fail to start**

Verify that Docker Desktop is running and check container status using:

    docker compose ps

**Airflow DAG does not appear**

Verify that `etl_pipeline.py` is in the `dags` folder and check for import errors:

    docker compose exec airflow-scheduler airflow dags list-import-errors

**Pipeline fails during file validation**

Confirm that all nine source files are present in the `data` folder and contain exactly 1,000 records each.

**Database connection fails**

Verify that the PostgreSQL container is running and that the database credentials in `.env` match the PostgreSQL configuration.

**Duplicate records or constraint violations**

The database setup task clears the main tables before reloading them. Confirm that the pipeline is running tasks in the correct order.

## Stopping the Environment

To stop the Docker containers without deleting stored database data, run:

    docker compose stop

Avoid using `docker compose down -v`, because that command removes Docker volumes, including PostgreSQL database storage.
