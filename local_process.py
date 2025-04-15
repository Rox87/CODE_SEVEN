from ..seven_airflow_etl_dag import (
    process_user_data,
    process_product_data,
    process_order_data,
    generate_reports
)

process_user_data()
process_product_data()
process_order_data()
generate_reports()