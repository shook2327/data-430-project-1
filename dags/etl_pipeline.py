
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator



from pathlib import Path
import pandas as pd
import yaml


import os 
import psycopg2 

import logging 

import shutil


from airflow.utils.state import DagRunState

# Data directory inside the Airflow Docker container
DATA_DIR = Path("/opt/airflow/data")


def validate_files():
    """Verify that all nine required data files exist and are readable."""

    datasets = ["customers", "products", "orders"]
    extensions = ["csv", "json", "yaml"]

    for dataset in datasets:
        for extension in extensions:
            file_path = DATA_DIR / f"{dataset}.{extension}"

            if not file_path.is_file():
                raise FileNotFoundError(
                    f"Required file is missing: {file_path}"
                )

            try:
                if extension == "csv":
                    df = pd.read_csv(file_path)
                    record_count = len(df)

                elif extension == "json":
                    df = pd.read_json(file_path)
                    record_count = len(df)

                else:
                    with open(file_path, "r", encoding="utf-8") as file:
                        data = yaml.safe_load(file)
                    record_count = len(data)

                if record_count != 1000:
                    raise ValueError(
                        f"{file_path.name} contains {record_count} records; "
                        "expected 1000."
                    )

                print(
                    f"Validated {file_path.name}: "
                    f"{record_count} records"
                )

            except Exception as error:
                raise RuntimeError(
                    f"Validation failed for {file_path.name}: {error}"
                ) from error

    print("File validation successful: All 9 files passed!")


# Connect to the project PostgreSQL database
def get_db_connection():
    return psycopg2.connect(
        host="postgres",
        port=5432,
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def setup_database():
    """Create required tables and prepare for ETL loading."""

    schema_path = Path("/opt/airflow/sql/schema.sql")

    if not schema_path.is_file():
        raise FileNotFoundError(
            f"Database schema file missing: {schema_path}"
        )

    connection = None

    try:
        connection = get_db_connection()

        with schema_path.open("r", encoding="utf-8") as file:
            schema_sql = file.read()

        with connection.cursor() as cursor:
            cursor.execute(schema_sql)

            # Clear the main tables before each load.
            # RESTART IDENTITY resets generated identity sequences.
            cursor.execute(
                "TRUNCATE TABLE orders, products, customers "
                "RESTART IDENTITY;"
            )

        connection.commit()
        print("Database setup completed successfully!")

    except Exception as error:
        if connection is not None:
            connection.rollback()
        print(f"Database setup failed: {error}")
        raise

    finally:
        if connection is not None:
            connection.close()


def extract_csv():
    """Read and validate the three CSV source files."""

    for dataset in ["customers", "products", "orders"]:
        file_path = DATA_DIR / f"{dataset}.csv"

        try:
            df = pd.read_csv(file_path)

            if len(df) != 1000:
                raise ValueError(
                    f"{dataset}.csv has {len(df)} records; expected 1000."
                )

            logging.info(
                "Extracted %s.csv successfully: %d records",
                dataset,
                len(df)
            )

        except Exception:
            logging.exception("CSV extraction failed: %s", file_path)
            raise

    logging.info("CSV extraction completed successfully!")


def extract_json():
    """Read and validate the three JSON source files."""

    for dataset in ["customers", "products", "orders"]:
        file_path = DATA_DIR / f"{dataset}.json"

        try:
            df = pd.read_json(file_path)

            if len(df) != 1000:
                raise ValueError(
                    f"{dataset}.json has {len(df)} records; expected 1000."
                )

            logging.info(
                "Extracted %s.json successfully: %d records",
                dataset,
                len(df)
            )

        except Exception:
            logging.exception("JSON extraction failed: %s", file_path)
            raise

    logging.info("JSON extraction completed successfully!")


def extract_yaml():
    """Read and validate the three YAML source files."""

    for dataset in ["customers", "products", "orders"]:
        file_path = DATA_DIR / f"{dataset}.yaml"

        try:
            with open(file_path, "r", encoding="utf-8") as file:
                records = yaml.safe_load(file)

            df = pd.DataFrame(records)

            if len(df) != 1000:
                raise ValueError(
                    f"{dataset}.yaml has {len(df)} records; expected 1000."
                )

            logging.info(
                "Extracted %s.yaml successfully: %d records",
                dataset,
                len(df)
            )

        except Exception:
            logging.exception("YAML extraction failed: %s", file_path)
            raise

    logging.info("YAML extraction completed successfully!")



def load_customers():
    """Load validated customer records into PostgreSQL."""

    file_path = DATA_DIR / "customers.csv"
    connection = None

    try:
        df = pd.read_csv(file_path)

        if len(df) != 1000:
            raise ValueError(
                f"Expected 1000 customers, found {len(df)}"
            )

        if df["customer_id"].duplicated().any():
            raise ValueError("Duplicate customer IDs found")

        if df["email"].duplicated().any():
            raise ValueError("Duplicate customer emails found")

        connection = get_db_connection()

        insert_sql = """
            INSERT INTO customers (
                customer_id, first_name, last_name, email,
                phone, city, registration_date, customer_type
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        with connection.cursor() as cursor:
            for row in df.itertuples(index=False, name=None):
                cursor.execute(insert_sql, row)

        connection.commit()

        logging.info(
            "Successfully loaded %d customers into PostgreSQL",
            len(df)
        )

    except Exception:
        if connection is not None:
            connection.rollback()
        logging.exception("Customer loading failed")
        raise

    finally:
        if connection is not None:
            connection.close()


def load_products():
    """Load validated product records into PostgreSQL."""

    file_path = DATA_DIR / "products.csv"
    connection = None

    try:
        df = pd.read_csv(file_path)

        if len(df) != 1000:
            raise ValueError(
                f"Expected 1000 products, found {len(df)}"
            )

        if df["product_id"].duplicated().any():
            raise ValueError("Duplicate product IDs found")

        connection = get_db_connection()

        insert_sql = """
            INSERT INTO products (
                product_id, product_name, category, brand,
                unit_price, stock_quantity, is_active
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        with connection.cursor() as cursor:
            for row in df.itertuples(index=False, name=None):
                cursor.execute(insert_sql, row)

        connection.commit()

        logging.info(
            "Successfully loaded %d products into PostgreSQL",
            len(df)
        )

    except Exception:
        if connection is not None:
            connection.rollback()
        logging.exception("Product loading failed")
        raise

    finally:
        if connection is not None:
            connection.close()


def load_orders():
    """Load order records into PostgreSQL."""

    file_path = DATA_DIR / "orders.csv"
    connection = None

    try:
        df = pd.read_csv(file_path)

        if len(df) != 1000:
            raise ValueError(
                f"Expected 1000 orders, found {len(df)}"
            )

        if df["order_id"].duplicated().any():
            raise ValueError("Duplicate order IDs found")

        connection = get_db_connection()

        insert_sql = """
            INSERT INTO orders (
                order_id, customer_id, order_date,
                order_status, payment_method, total_amount,
                discount_amount, shipping_cost
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        with connection.cursor() as cursor:
            for row in df.itertuples(index=False, name=None):
                cursor.execute(insert_sql, row)

        connection.commit()

        logging.info(
            "Successfully loaded %d orders into PostgreSQL",
            len(df)
        )

    except Exception:
        if connection is not None:
            connection.rollback()
        logging.exception("Order loading failed")
        raise

    finally:
        if connection is not None:
            connection.close()



def validate_database():
    """Verify database row counts and foreign key integrity."""

    connection = None

    try:
        connection = get_db_connection()

        with connection.cursor() as cursor:
            # Check all three table counts
            for table in ["customers", "products", "orders"]:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]

                if count != 1000:
                    raise ValueError(
                        f"{table} has {count} records; expected 1000."
                    )

                logging.info(
                    "Validation passed: %s has %d records",
                    table, count
                )

            # Check for orders referencing nonexistent customers
            cursor.execute("""
                SELECT COUNT(*)
                FROM orders o
                LEFT JOIN customers c
                    ON o.customer_id = c.customer_id
                WHERE c.customer_id IS NULL
            """)

            invalid_orders = cursor.fetchone()[0]

            if invalid_orders > 0:
                raise ValueError(
                    f"Found {invalid_orders} orders with invalid customer IDs"
                )

            logging.info(
                "Foreign key validation passed: "
                "All orders reference valid customers"
            )

        logging.info("All database validation checks passed!")

    except Exception:
        logging.exception("Database validation failed")
        raise

    finally:
        if connection is not None:
            connection.close()


def complete_pipeline():
    """Archive processed files and record successful ETL execution."""

    archive_dir = DATA_DIR / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    run_time = datetime.now()
    timestamp = run_time.strftime("%Y%m%d_%H%M%S")

    connection = None

    try:
        # Archive copies of all nine source files
        for dataset in ["customers", "products", "orders"]:
            for extension in ["csv", "json", "yaml"]:
                filename = f"{dataset}.{extension}"

                source = DATA_DIR / filename
                destination = archive_dir / (
                    f"{timestamp}_{filename}"
                )

                shutil.copy2(source, destination)

                logging.info("Archived %s", filename)

        # Record successful pipeline execution
        connection = get_db_connection()

        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO etl_audit (
                    dag_name,
                    start_time,
                    end_time,
                    status,
                    records_processed
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    "project1_etl_pipeline",
                    run_time,
                    datetime.now(),
                    "success",
                    3000
                )
            )

        connection.commit()

        logging.info(
            "ETL pipeline completed successfully: "
            "3000 unique records processed"
        )

    except Exception:
        if connection is not None:
            connection.rollback()

        logging.exception("Pipeline completion failed")
        raise

    finally:
        if connection is not None:
            connection.close()


def record_pipeline_failure(context):
    """Record a failed DAG run in the ETL audit table."""

    dag_run = context.get("dag_run")
    connection = None

    try:
        connection = get_db_connection()

        start_time = (
            dag_run.start_date
            if dag_run is not None
            else datetime.now()
        )

        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO etl_audit (
                    dag_name,
                    start_time,
                    end_time,
                    status,
                    records_processed
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    "project1_etl_pipeline",
                    start_time,
                    datetime.now(),
                    "failed",
                    0
                )
            )

        connection.commit()
        logging.error(
            "ETL pipeline failure recorded in etl_audit"
        )

    except Exception:
        if connection is not None:
            connection.rollback()

        logging.exception(
            "Unable to record pipeline failure in etl_audit"
        )

    finally:
        if connection is not None:
            connection.close()



# Define the Airflow DAG
with DAG(
    dag_id="project1_etl_pipeline",
    description="ETL pipeline for customer, product, and order data",
    start_date=datetime(2026, 10, 1),
    schedule_interval="@daily",
    catchup=False,
    on_failure_callback=record_pipeline_failure,
    default_args={
        "owner": "project1",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["project1", "etl"],
) as dag:

    file_validation = PythonOperator(
        task_id="file_validation",
        python_callable=validate_files,
    )

    database_setup = PythonOperator(
        task_id="database_setup",
        python_callable=setup_database,
    )


    extract_csv_task = PythonOperator(
        task_id="extract_csv",
        python_callable=extract_csv,
    )


    extract_json_task = PythonOperator(
        task_id="extract_json",
        python_callable=extract_json,
    )

    extract_yaml_task = PythonOperator(
        task_id="extract_yaml",
        python_callable=extract_yaml,
    )


    load_customers_task = PythonOperator(
        task_id="load_customers",
        python_callable=load_customers,
    )

    
    load_products_task = PythonOperator(
        task_id="load_products",
        python_callable=load_products,
    )


    load_orders_task = PythonOperator(
        task_id="load_orders",
        python_callable=load_orders,
    )


    data_validation_task = PythonOperator(
        task_id="data_validation",
        python_callable=validate_database,
    )


    completion_task = PythonOperator(
        task_id="completion",
        python_callable=complete_pipeline,
    )

    # Define task dependencies
    file_validation >> database_setup

    database_setup >> [
        extract_csv_task,
        extract_json_task,
        extract_yaml_task,
    ]

    [
        extract_csv_task,
        extract_json_task,
        extract_yaml_task,
    ] >> load_customers_task

    load_customers_task >> load_products_task

    load_products_task >> load_orders_task

    load_orders_task >> data_validation_task

    load_orders_task >> data_validation_task
    data_validation_task >> completion_task