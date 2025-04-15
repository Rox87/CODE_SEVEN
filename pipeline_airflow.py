from datetime import datetime, timedelta
from steps.b_clean_trasform import validate_clean_email_series, validate_clean_cpf_series
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from io import StringIO
import re
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Task functions
def process_user_data(**kwargs):
    """Process and clean user data"""
    # In a real scenario, you would read from a file or database
    # For this example, we'll assume the file exists in a data directory
    data_dir = os.path.join('data', 'desafio', 'raw')
    user_file_path = os.path.join(data_dir, 'user_raw.csv')
    
    # Read the user data
    user_raw = pd.read_csv(user_file_path, sep=",", encoding="utf-8")
    user_raw.dropna(inplace=True)
    
    # Clean and standardize data
    user_raw['name'] = user_raw['name'].str.title()
    user_raw['e-mail'] = validate_clean_email_series(user_raw['e-mail'])
    user_raw['cpf'] = validate_clean_cpf_series(user_raw['cpf'])
    
    # Save processed data
    processed_dir = os.path.join('data', 'desafio', 'processed')
    os.makedirs(processed_dir, exist_ok=True)
    user_raw.to_csv(os.path.join(processed_dir, 'user_processed.csv'), index=False)
    
    return "User data processing completed"

def process_product_data(**kwargs):
    """Process and clean product data"""
    data_dir = os.path.join('data', 'desafio', 'raw')
    product_file_path = os.path.join(data_dir, 'produtos_raw.csv')
    
    # Read the product data
    produtos_raw = pd.read_csv(product_file_path, sep=",", encoding="utf-8")
    produtos_raw.dropna(inplace=True)
    
    # Clean and standardize data
    produtos_raw['name'] = produtos_raw['name'].str.title()
    produtos_raw['description'] = produtos_raw['description'].str.capitalize()
    
    # Save processed data
    processed_dir = os.path.join('data', 'desafio', 'processed')
    os.makedirs(processed_dir, exist_ok=True)
    produtos_raw.to_csv(os.path.join(processed_dir, 'produtos_processed.csv'), index=False)
    
    return "Product data processing completed"

def process_order_data(**kwargs):
    """Process and clean order data"""
    data_dir = os.path.join('data', 'desafio', 'raw')
    order_file_path = os.path.join(data_dir, 'pedidos_raw.csv')
    
    # Read the order data
    pedidos_raw = pd.read_csv(order_file_path, sep=",", encoding="utf-8")
    pedidos_raw.dropna(inplace=True)
    
    # Convert date columns to datetime
    date_columns = [col for col in pedidos_raw.columns if 'date' in col]
    for col in date_columns:
        pedidos_raw[col] = pd.to_datetime(pedidos_raw[col], errors='coerce')
    
    # Save processed data
    processed_dir = os.path.join('data', 'desafio', 'processed')
    os.makedirs(processed_dir, exist_ok=True)
    pedidos_raw.to_csv(os.path.join(processed_dir, 'pedidos_processed.csv'), index=False)
    
    return "Order data processing completed"

def generate_reports(**kwargs):
    """Generate business reports from processed data"""
    processed_dir = os.path.join('data', 'desafio', 'processed')
    
    # Load processed data
    users = pd.read_csv(os.path.join(processed_dir, 'user_processed.csv'))
    products = pd.read_csv(os.path.join(processed_dir, 'produtos_processed.csv'))
    orders = pd.read_csv(os.path.join(processed_dir, 'pedidos_processed.csv'))
    
    # Convert date columns to datetime
    orders['created_at'] = pd.to_datetime(orders['created_at'])
    
    # Generate reports
    # 1. Orders by month
    orders_by_month = orders.groupby(orders['created_at'].dt.to_period('M')).size().reset_index(name='count')
    
    # 2. Revenue by payment method
    revenue_by_payment = orders.groupby('payment_method')['total'].sum().reset_index()
    
    # 3. Orders by shipping status
    orders_by_status = orders.groupby('shipping_status').size().reset_index(name='count')
    
    # Save reports
    reports_dir = os.path.join('data', 'desafio', 'reports')
    os.makedirs(reports_dir, exist_ok=True)
    
    orders_by_month.to_csv(os.path.join(reports_dir, 'orders_by_month.csv'), index=False)
    revenue_by_payment.to_csv(os.path.join(reports_dir, 'revenue_by_payment.csv'), index=False)
    orders_by_status.to_csv(os.path.join(reports_dir, 'orders_by_status.csv'), index=False)
    
    return "Reports generated successfully"

# Create the DAG
with DAG(
    'seven_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for Seven Inc data',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    
    # Define tasks
    process_users_task = PythonOperator(
        task_id='process_user_data',
        python_callable=process_user_data,
        provide_context=True,
    )
    
    process_products_task = PythonOperator(
        task_id='process_product_data',
        python_callable=process_product_data,
        provide_context=True,
    )
    
    process_orders_task = PythonOperator(
        task_id='process_order_data',
        python_callable=process_order_data,
        provide_context=True,
    )
    
    generate_reports_task = PythonOperator(
        task_id='generate_reports',
        python_callable=generate_reports,
        provide_context=True,
    )
    
    # Define task dependencies
    [process_users_task, process_products_task, process_orders_task] >> generate_reports_task