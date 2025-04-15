# Seven Proj1 Data Warehouse Project Documentation

## Project Overview

This project implements a data pipeline using dbt (data build tool) for transformation, with data sourced from a data lake. The pipeline processes user, product, and order data through various stages of transformation to create a structured data warehouse for analytics and reporting.

## Architecture

The project follows a modern data stack architecture:

1. **Data Ingestion**: Raw data is ingested into a data lake (Azure Data Lake Storage)
2. **Data Processing**: Airflow orchestrates the ETL processes
3. **Data Transformation**: dbt models transform the processed data
4. **Data Warehouse**: Transformed data is stored in a structured data warehouse
5. **Reporting**: Business reports are generated from the transformed data

## Project Structure

```
/
├── airflow/                # Airflow configuration
├── dags/                   # Airflow DAG definitions
├── data/                   # Data directory
│   └── desafio/            # Challenge data
│       ├── raw/            # Raw data files
│       ├── processed/      # Processed data (created by pipeline)
│       └── reports/        # Generated reports (created by pipeline)
├── dbt_project.yml         # dbt project configuration
├── models/                 # dbt models (to be implemented)
│   ├── marts/              # Business-level models
│   └── staging/            # Initial data transformation models
├── steps/                  # Processing steps
└── util/                   # Utility scripts
```

## Data Flow

1. **Raw Data Sources**:
   - `user_raw.csv`: User information
   - `produtos_raw.csv`: Product information
   - `pedidos_raw.csv`: Order information

2. **Data Processing**:
   - Data is cleaned and standardized
   - Missing values are handled
   - Data types are converted
   - Data is validated

3. **Data Transformation**:
   - Staging models create clean, typed versions of source tables
   - Mart models create business-level aggregations and metrics

4. **Reporting**:
   - Monthly order counts
   - Revenue by payment method
   - Order status distribution

## dbt Project Configuration

```yaml
name: 'seven_dw'
version: '1.0.0'
config-version: 2

profile: 'seven_ecommerce_dw'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  seven_ecommerce_dw:
    marts:
      +materialized: table
    staging:
      +materialized: view
```

## Models

### Staging Models

Staging models should be implemented as views that clean and prepare source data:

- `stg_users.sql`: Cleaned user data
- `stg_products.sql`: Cleaned product data
- `stg_orders.sql`: Cleaned order data

### Mart Models

Mart models should be implemented as tables that transform data for business use:

- `fct_orders.sql`: Order facts
- `dim_users.sql`: User dimension
- `dim_products.sql`: Product dimension
- `rpt_monthly_orders.sql`: Monthly order aggregations
- `rpt_payment_revenue.sql`: Revenue by payment method

## SQL Transformations

The project includes several SQL transformations for analytics:

1. **Monthly Order Counts**:
   ```sql
   SELECT 
       EXTRACT(YEAR FROM created_at::date) AS ano,
       EXTRACT(MONTH FROM created_at::date) AS mes,
       COUNT(*) AS total_pedidos
   FROM 
       pedidos_raw
   GROUP BY 
       EXTRACT(YEAR FROM created_at::date),
       EXTRACT(MONTH FROM created_at::date)
   ORDER BY 
       ano, mes;
   ```

2. **Revenue by Payment Method**:
   ```sql
   SELECT 
       payment_method,
       SUM(total) AS receita_total
   FROM 
       pedidos_raw
   GROUP BY 
       payment_method
   ORDER BY 
       receita_total DESC;
   ```

3. **Orders by Shipping Status**:
   ```sql
   SELECT 
       shipping_status,
       COUNT(*) AS total_pedidos
   FROM 
       pedidos_raw
   GROUP BY 
       shipping_status
   ORDER BY 
       total_pedidos DESC;
   ```

## Airflow ETL Pipeline

The project includes an Airflow DAG that orchestrates the ETL process:

1. **Process User Data**: Cleans and standardizes user data
2. **Process Product Data**: Cleans and standardizes product data
3. **Process Order Data**: Cleans and standardizes order data
4. **Generate Reports**: Creates business reports from processed data

## Implementation Guide

### Setting Up dbt Models

1. Create the `models` directory with `staging` and `marts` subdirectories
2. Implement staging models for each data source
3. Implement mart models for business metrics
4. Run `dbt run` to execute the models
5. Run `dbt test` to validate the models
6. Run `dbt docs generate` to generate documentation

### Running the Pipeline

1. Start Airflow: `docker-compose up`
2. Enable the `seven_etl_pipeline` DAG
3. Monitor the pipeline execution
4. Verify the generated reports

## Future Enhancements

1. Add data quality tests
2. Implement incremental models for large datasets
3. Add documentation for each model
4. Create a dashboard for visualizing the reports
5. Implement CI/CD for the dbt project