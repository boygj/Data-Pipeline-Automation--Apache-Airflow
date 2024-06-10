# Sparkify Data Warehouse with Apache Airflow

## Project Overview

This project builds an ETL (Extract, Transform, Load) data pipeline using Apache Airflow to process music streaming data for Sparkify, a fictional music streaming company. The pipeline extracts data from S3, transforms it, and loads it into a Redshift data warehouse for further analysis.

## Key Components

- **Apache Airflow:** Orchestrates the ETL workflow, scheduling tasks, handling dependencies, and monitoring execution.
- **Amazon S3:** Stores raw data files (JSON logs and song metadata).
- **Amazon Redshift:** Serves as the data warehouse to store structured data.
- **Python Operators:** Custom Airflow operators handle data staging, fact and dimension table loading, and data quality checks.
- **SQL Queries:** SQL statements (found in `helpers/sql_queries.py`) define data transformations and table creation.

## DAG Structure

The DAG (`final_project.py`) has the following task flow:

1. **Begin_execution:** Dummy task to mark the start.
2. **Stage Events/Songs:** Extract data from S3 and stage it into Redshift tables.
3. **Load Fact Table:** Load the `songplays` fact table.
4. **Load Dimensions:** Load the `users`, `songs`, `artists`, and `time` dimension tables in parallel (using a subDAG).
5. **Run Quality Checks:**  Validate data quality in the loaded tables.
6. **End_execution:** Dummy task to mark the end.

## How to Run

### Prerequisites

- **Airflow:**  Install and configure Apache Airflow.
- **Redshift:** Set up a Redshift cluster and create an Airflow connection (`redshift_conn_id`).
- **AWS Credentials:** Configure an Airflow connection (`aws_credentials_id`) with valid AWS credentials.
- **S3 Bucket:** Ensure the S3 bucket ('udacity-dend') and data paths are correct (modify in the DAG if necessary).

### Steps

1. **Clone Repository:** Clone this repository.
2. **Install Dependencies:** `pip install -r requirements.txt`
3. **Start Airflow:** `airflow webserver` & `airflow scheduler`
4. **Trigger DAG:** Go to the Airflow UI and trigger the `final_project` DAG.

## Data Quality Checks

The DAG includes several data quality checks to ensure data integrity:

- Check for null values in primary key columns.
- Verify row counts in tables.
- **Add more checks!**  Customize the `dq_checks` list in `data_quality.py` for your specific requirements.

## Project Structure

├── dags/
│   └── final_project.py            # Main DAG file
├── final_project_operators/
│   ├── data_quality.py          # Data quality operator
│   ├── load_dimension.py        # Dimension load operator
│   ├── load_fact.py             # Fact table load operator
│   └── stage_redshift.py        # S3 to Redshift stage operator
├── helpers/
│   └── sql_queries.py           # SQL statements
└── README.md                     # This readme file


## Improvements and Considerations

- **Error Handling:** Robust error handling (e.g., task retries, alerts) is crucial for production environments.
- **Backfilling:** Implement a mechanism for backfilling historical data if needed.
- **Logging:** Enhance logging to provide more detailed information during pipeline execution.
- **Documentation:** Add comprehensive comments and docstrings to improve code readability and maintainability.
\