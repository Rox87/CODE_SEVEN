# Seven E-commerce DBT Implementation Guide

This guide provides detailed instructions for implementing and using dbt (data build tool) in the Seven E-commerce data warehouse project.

## Prerequisites

- Python 3.7 or higher
- dbt Core installed (`pip install dbt-core`)
- Database adapter installed (e.g., `pip install dbt-postgres`)
- Access to the data warehouse

## Project Setup

### 1. Initialize the dbt Project

The project already has a `dbt_project.yml` file configured. If you need to create a new project, run:

```bash
dbt init seven_dw
```

### 2. Configure Profiles

Create or update your `~/.dbt/profiles.yml` file with the following configuration:

```yaml
seven_ecommerce_dw:
  target: dev
  outputs:
    dev:
      type: postgres  # or your database type
      host: localhost
      user: your_username
      password: your_password
      port: 5432
      dbname: seven_ecommerce_dw
      schema: dbt_dev
      threads: 4
    prod:
      type: postgres  # or your database type
      host: production_host
      user: prod_username
      password: prod_password
      port: 5432
      dbname: seven_ecommerce_dw
      schema: dbt_prod
      threads: 4
```

## Directory Structure Implementation

Create the following directory structure for your dbt models:

```
models/
├── sources.yml
├── schema.yml
├── staging/
│   ├── stg_users.sql
│   ├── stg_products.sql
│   └── stg_orders.sql
└── marts/
    ├── core/
    │   ├── dim_users.sql
    │   ├── dim_products.sql
    │   └── fct_orders.sql
    └── reporting/
        ├── rpt_monthly_orders.sql
        ├── rpt_payment_revenue.sql
        └── rpt_shipping_status.sql
```

## Model Implementation

### 1. Define Sources

Create `models/sources.yml` to define your raw data sources:

```yaml
version: 2

sources:
  - name: raw
    database: seven_ecommerce_dw  # adjust as needed
    schema: raw  # adjust as needed
    tables:
      - name: user_raw
      - name: produtos_raw
      - name: pedidos_raw
```

### 2. Implement Staging Models

Staging models should clean and standardize the raw data. Implement them as views for efficiency.

Example implementation for `models/staging/stg_users.sql`:

```sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'user_raw') }}
)

SELECT
    user_id,
    INITCAP(name) AS name,
    entry_date,
    entry_time,
    update_date,
    LOWER(TRIM("e-mail")) AS email,
    REGEXP_REPLACE(cpf, '[^0-9]', '') AS cpf
FROM source
WHERE "e-mail" IS NOT NULL
```

### 3. Implement Dimension and Fact Models

Create dimension and fact tables in the marts/core directory.

Example implementation for `models/marts/core/dim_users.sql`:

```sql
WITH users AS (
    SELECT * FROM {{ ref('stg_users') }}
)

SELECT
    user_id,
    name,
    email,
    cpf,
    entry_date,
    update_date,
    -- Additional derived fields
    DATEDIFF('day', entry_date, CURRENT_DATE()) AS days_since_registration
FROM users
```

### 4. Implement Reporting Models

Create reporting models that aggregate data for business insights.

Example implementation for `models/marts/reporting/rpt_monthly_orders.sql`:

```sql
WITH orders AS (
    SELECT * FROM {{ ref('fct_orders') }}
)

SELECT
    DATE_TRUNC('month', order_date) AS month,
    COUNT(*) AS total_orders,
    SUM(order_total) AS total_revenue,
    AVG(order_total) AS avg_order_value,
    COUNT(DISTINCT user_id) AS unique_customers
FROM orders
GROUP BY 1
ORDER BY 1
```

## Running dbt

### 1. Test Your Connection

```bash
dbt debug
```

### 2. Run Models

Run all models:

```bash
dbt run
```

Run specific models:

```bash
dbt run --models staging.stg_users
dbt run --models marts.core
```

### 3. Test Your Models

```bash
dbt test
```

### 4. Generate Documentation

```bash
dbt docs generate
dbt docs serve
```

## Integration with Airflow

The project includes an Airflow DAG (`seven_airflow_etl_dag.py`) that processes the raw data. To integrate dbt with Airflow:

1. Install the dbt Airflow operator:

```bash
pip install airflow-dbt
```

2. Create a new DAG or modify the existing one to include dbt tasks:

```python
from airflow_dbt.operators.dbt_operator import DbtRunOperator, DbtTestOperator

# ... existing DAG setup ...

dbt_run = DbtRunOperator(
    task_id='dbt_run',
    profiles_dir='/path/to/profiles/directory',
    dir='/path/to/dbt/project',
    dag=dag
)

dbt_test = DbtTestOperator(
    task_id='dbt_test',
    profiles_dir='/path/to/profiles/directory',
    dir='/path/to/dbt/project',
    dag=dag
)

# Set task dependencies
generate_reports_task >> dbt_run >> dbt_test
```

## Best Practices

### 1. Model Organization

- Keep staging models simple and focused on cleaning and typing
- Use intermediate models for complex transformations
- Create dimension and fact models following dimensional modeling principles
- Use reporting models for specific business use cases

### 2. Testing

Add tests to your models in `schema.yml`:

```yaml
version: 2

models:
  - name: dim_users
    columns:
      - name: user_id
        tests:
          - unique
          - not_null
      - name: email
        tests:
          - not_null
```

### 3. Documentation

Add documentation to your models:

```yaml
version: 2

models:
  - name: dim_users
    description: "User dimension table containing all user attributes"
    columns:
      - name: user_id
        description: "Primary key for users"
      - name: email
        description: "User's email address, cleaned and standardized"
```

### 4. Incremental Models

For large tables, consider using incremental models:

```sql
{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

WITH orders AS (
    SELECT * FROM {{ source('raw', 'pedidos_raw') }}
    {% if is_incremental() %}
    WHERE created_at > (SELECT MAX(order_date) FROM {{ this }})
    {% endif %}
)

SELECT * FROM orders
```

## Troubleshooting

### Common Issues

1. **Connection Problems**
   - Verify your profiles.yml configuration
   - Check database credentials
   - Run `dbt debug` to test connection

2. **Model Errors**
   - Check SQL syntax
   - Verify source table and column names
   - Look for missing references

3. **Performance Issues**
   - Consider using incremental models for large tables
   - Optimize SQL queries
   - Adjust materialization strategy (view vs table)

## Conclusion

This guide provides a foundation for implementing dbt in the Seven E-commerce data warehouse project. By following these instructions, you can create a robust, maintainable, and well-documented data transformation pipeline that turns raw data into valuable business insights.

For more information, refer to the [dbt documentation](https://docs.getdbt.com/).