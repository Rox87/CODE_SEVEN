# DBT Models Structure

This document outlines the recommended structure for implementing DBT models in this project. Based on the `dbt_project.yml` configuration, the models should be organized into staging and marts directories.

## Directory Structure

```
models/
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

## Example Model Implementations

### Staging Models

Staging models should be implemented as views that clean and prepare source data.

#### stg_users.sql

```sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'user_raw') }}
),

cleaned AS (
    SELECT
        user_id,
        name,
        entry_date,
        entry_time,
        update_date,
        LOWER(TRIM("e-mail")) AS email,
        REGEXP_REPLACE(cpf, '[^0-9]', '') AS cpf_clean
    FROM source
    WHERE "e-mail" IS NOT NULL
      AND LENGTH(REGEXP_REPLACE(cpf, '[^0-9]', '')) = 11
)

SELECT
    user_id,
    INITCAP(name) AS name,
    entry_date,
    entry_time,
    update_date,
    email,
    cpf_clean AS cpf
FROM cleaned
```

#### stg_products.sql

```sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'produtos_raw') }}
)

SELECT
    product_id,
    INITCAP(name) AS name,
    INITCAP(description) AS description,
    price,
    category,
    created_at,
    updated_at
FROM source
WHERE name IS NOT NULL
```

#### stg_orders.sql

```sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'pedidos_raw') }}
),

cleaned AS (
    SELECT
        user_id,
        created_at,
        items,
        total,
        payment_status,
        payment_method,
        payment_date,
        shipping_status,
        shipping_status_date_awaiting_payment,
        shipping_status_date_preparing,
        shipping_status_date_sent,
        shipping_status_date_delivered
    FROM source
    WHERE user_id IS NOT NULL
      AND total > 0
)

SELECT
    *,
    CASE
        WHEN payment_status = 'paid' THEN TRUE
        ELSE FALSE
    END AS is_paid,
    CASE
        WHEN shipping_status = 'delivered' THEN TRUE
        ELSE FALSE
    END AS is_delivered
FROM cleaned
```

### Mart Models

Mart models should be implemented as tables that transform data for business use.

#### dim_users.sql

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

#### dim_products.sql

```sql
WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
)

SELECT
    product_id,
    name,
    description,
    price,
    category,
    created_at,
    updated_at,
    -- Additional derived fields
    CASE
        WHEN price < 50 THEN 'Low'
        WHEN price >= 50 AND price < 200 THEN 'Medium'
        ELSE 'High'
    END AS price_tier
FROM products
```

#### fct_orders.sql

```sql
WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

users AS (
    SELECT * FROM {{ ref('dim_users') }}
)

SELECT
    o.user_id,
    o.created_at AS order_date,
    o.items,
    o.total AS order_total,
    o.payment_status,
    o.payment_method,
    o.payment_date,
    o.shipping_status,
    o.is_paid,
    o.is_delivered,
    -- Join with user dimension
    u.name AS user_name,
    u.email AS user_email
FROM orders o
LEFT JOIN users u ON o.user_id = u.user_id
```

### Reporting Models

#### rpt_monthly_orders.sql

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

#### rpt_payment_revenue.sql

```sql
WITH orders AS (
    SELECT * FROM {{ ref('fct_orders') }}
)

SELECT
    payment_method,
    COUNT(*) AS total_orders,
    SUM(order_total) AS total_revenue,
    AVG(order_total) AS avg_order_value,
    SUM(CASE WHEN is_paid THEN 1 ELSE 0 END) AS paid_orders,
    SUM(CASE WHEN is_paid THEN order_total ELSE 0 END) AS paid_revenue
FROM orders
GROUP BY 1
ORDER BY total_revenue DESC
```

#### rpt_shipping_status.sql

```sql
WITH orders AS (
    SELECT * FROM {{ ref('fct_orders') }}
)

SELECT
    shipping_status,
    COUNT(*) AS total_orders,
    SUM(order_total) AS total_revenue,
    AVG(order_total) AS avg_order_value,
    COUNT(DISTINCT user_id) AS unique_customers
FROM orders
GROUP BY 1
ORDER BY total_orders DESC
```

## Sources Configuration

Create a `models/sources.yml` file to define the raw data sources:

```yaml
version: 2

sources:
  - name: raw
    database: seven_ecommerce_dw
    schema: raw
    tables:
      - name: user_raw
        columns:
          - name: user_id
            tests:
              - unique
              - not_null
          - name: name
          - name: entry_date
          - name: entry_time
          - name: update_date
          - name: e-mail
          - name: cpf

      - name: produtos_raw
        columns:
          - name: product_id
            tests:
              - unique
              - not_null
          - name: name
          - name: description
          - name: price
          - name: category
          - name: created_at
          - name: updated_at

      - name: pedidos_raw
        columns:
          - name: user_id
            tests:
              - not_null
          - name: created_at
          - name: items
          - name: total
          - name: payment_status
          - name: payment_method
          - name: payment_date
          - name: shipping_status
          - name: shipping_status_date_awaiting_payment
          - name: shipping_status_date_preparing
          - name: shipping_status_date_sent
          - name: shipping_status_date_delivered
```

## Data Tests

Create a `models/schema.yml` file to define tests for your models:

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

  - name: dim_products
    columns:
      - name: product_id
        tests:
          - unique
          - not_null

  - name: fct_orders
    columns:
      - name: user_id
        tests:
          - not_null
      - name: order_total
        tests:
          - not_null
```

This structure provides a solid foundation for implementing the DBT models in this project, following best practices for data modeling and transformation.