import os
import pandas as pd
import psycopg2
import datetime
from sqlalchemy import create_engine
import numpy as np

# Database connection parameters
# These should be configured according to your PostgreSQL setup
DB_PARAMS = {
    'dbname': 'postgres',  # Change to your database name
    'user': 'postgres',    # Change to your username
    'password': 'postgres', # Change to your password
    'host': 'localhost',
    'port': '5432'
}

# File paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'desafio', 'processed')

USERS_FILE = os.path.join(DATA_DIR, 'user_processed.csv')
PRODUCTS_FILE = os.path.join(DATA_DIR, 'produtos_processed.csv')
ORDERS_FILE = os.path.join(DATA_DIR, 'pedidos_processed.csv')

# Function to create database connection
def get_connection():
    try:
        conn = psycopg2.connect(
            dbname=DB_PARAMS['dbname'],
            user=DB_PARAMS['user'],
            password=DB_PARAMS['password'],
            host=DB_PARAMS['host'],
            port=DB_PARAMS['port']
        )
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

# Function to create SQLAlchemy engine
def get_engine():
    connection_string = f"postgresql://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"
    return create_engine(connection_string)

# Function to create tables if they don't exist
def create_tables(conn):
    try:
        cursor = conn.cursor()
        
        # Create tables based on the modelagem_fisica.sql schema
        cursor.execute("""
        -- Create dimension table for users
        CREATE TABLE IF NOT EXISTS dim_usuarios (
            user_id INTEGER PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            entry_date DATE NOT NULL,
            entry_time TIME NOT NULL,
            update_date DATE,
            email VARCHAR(100) NOT NULL,
            cpf VARCHAR(14) NOT NULL
        );

        -- Create dimension table for products
        CREATE TABLE IF NOT EXISTS dim_produtos (
            product_id INTEGER PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            price DECIMAL(10, 2) NOT NULL,
            stock INTEGER NOT NULL,
            created_at DATE NOT NULL,
            description TEXT
        );

        -- Create dimension table for time
        CREATE TABLE IF NOT EXISTS dim_tempo (
            tempo_id INTEGER PRIMARY KEY,
            data DATE NOT NULL UNIQUE,
            dia_da_semana VARCHAR(20) NOT NULL,
            eh_final_de_semana BOOLEAN NOT NULL,
            eh_feriado BOOLEAN NOT NULL,
            nome_mes VARCHAR(20) NOT NULL,
            trimestre INTEGER NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
            semestre INTEGER NOT NULL CHECK (semestre BETWEEN 1 AND 2),
            ano INTEGER NOT NULL
        );

        -- Create fact table for orders
        CREATE TABLE IF NOT EXISTS fato_pedidos (
            pedido_id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            product_id INTEGER,
            tempo_id INTEGER,
            created_at TIMESTAMP NOT NULL,
            items TEXT NOT NULL,
            total DECIMAL(10, 2) NOT NULL,
            payment_status VARCHAR(50) NOT NULL,
            payment_method VARCHAR(50) NOT NULL,
            payment_date TIMESTAMP,
            shipping_status VARCHAR(50) NOT NULL,
            shipping_status_date_awaiting_payment TIMESTAMP,
            shipping_status_date_preparing TIMESTAMP,
            shipping_status_date_sent TIMESTAMP,
            shipping_status_date_delivered TIMESTAMP,
            
            -- Foreign key constraints
            FOREIGN KEY (user_id) REFERENCES dim_usuarios(user_id),
            FOREIGN KEY (product_id) REFERENCES dim_produtos(product_id),
            FOREIGN KEY (tempo_id) REFERENCES dim_tempo(tempo_id)
        );
        """)
        
        conn.commit()
        print("Tables created successfully")
    except Exception as e:
        conn.rollback()
        print(f"Error creating tables: {e}")

# Function to generate time dimension data
def generate_time_dimension(conn):
    try:
        cursor = conn.cursor()
        
        # Check if dim_tempo already has data
        cursor.execute("SELECT COUNT(*) FROM dim_tempo")
        count = cursor.fetchone()[0]
        
        if count > 0:
            print("Time dimension already populated")
            return
        
        # Get all unique dates from the orders data
        orders_df = pd.read_csv(ORDERS_FILE, parse_dates=['created_at'])
        dates = pd.to_datetime(orders_df['created_at']).dt.date.unique()
        
        # Add a few days before and after for good measure
        min_date = min(dates) - datetime.timedelta(days=30)
        max_date = max(dates) + datetime.timedelta(days=30)
        
        # Generate all dates in the range
        all_dates = [min_date + datetime.timedelta(days=x) for x in range((max_date - min_date).days + 1)]
        
        # Insert time dimension data
        for i, date in enumerate(all_dates, 1):
            dt = datetime.datetime.combine(date, datetime.time.min)
            
            # Extract date components
            day_of_week = dt.strftime('%A')
            is_weekend = dt.weekday() >= 5  # 5 = Saturday, 6 = Sunday
            is_holiday = False  # Simplified, would need a holiday calendar
            month_name = dt.strftime('%B')
            quarter = (dt.month - 1) // 3 + 1
            semester = 1 if dt.month <= 6 else 2
            year = dt.year
            
            cursor.execute("""
            INSERT INTO dim_tempo (tempo_id, data, dia_da_semana, eh_final_de_semana, eh_feriado, nome_mes, trimestre, semestre, ano)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (data) DO NOTHING
            """, (i, date, day_of_week, is_weekend, is_holiday, month_name, quarter, semester, year))
        
        conn.commit()
        print(f"Time dimension populated with {len(all_dates)} dates")
    except Exception as e:
        conn.rollback()
        print(f"Error generating time dimension: {e}")

# Function to load users data
def load_users_data(engine):
    try:
        # Read users data
        users_df = pd.read_csv(USERS_FILE)
        
        # Rename e-mail column to email to match database schema
        users_df.rename(columns={'e-mail': 'email'}, inplace=True)
        
        # Convert date columns
        users_df['entry_date'] = pd.to_datetime(users_df['entry_date']).dt.date
        users_df['update_date'] = pd.to_datetime(users_df['update_date']).dt.date
        
        # Load data to database
        users_df.to_sql('dim_usuarios', engine, if_exists='append', index=False, method='multi')
        print(f"Loaded {len(users_df)} users records")
    except Exception as e:
        print(f"Error loading users data: {e}")

# Function to load products data
def load_products_data(engine):
    try:
        # Read products data
        products_df = pd.read_csv(PRODUCTS_FILE)
        
        # Convert date columns
        products_df['created_at'] = pd.to_datetime(products_df['created_at']).dt.date
        
        # Load data to database
        products_df.to_sql('dim_produtos', engine, if_exists='append', index=False, method='multi')
        print(f"Loaded {len(products_df)} products records")
    except Exception as e:
        print(f"Error loading products data: {e}")

# Function to load orders data
def load_orders_data(conn, engine):
    try:
        # Read orders data
        orders_df = pd.read_csv(ORDERS_FILE, parse_dates=[
            'created_at', 'payment_date', 
            'shipping_status_date_awaiting_payment', 'shipping_status_date_preparing',
            'shipping_status_date_sent', 'shipping_status_date_delivered'
        ])
        
        # Replace NaN values with None for SQL compatibility
        orders_df = orders_df.replace({np.nan: None})
        
        # Create a cursor
        cursor = conn.cursor()
        
        # Process each order
        for _, row in orders_df.iterrows():
            # Get tempo_id for the order date
            order_date = row['created_at'].date()
            cursor.execute("SELECT tempo_id FROM dim_tempo WHERE data = %s", (order_date,))
            tempo_id_result = cursor.fetchone()
            tempo_id = tempo_id_result[0] if tempo_id_result else None
            
            # For simplicity, we're using the first product mentioned in items
            # In a real scenario, you might want to split items and create multiple records
            # or use a junction table for order_items
            
            # Insert order data
            cursor.execute("""
            INSERT INTO fato_pedidos (
                user_id, product_id, tempo_id, created_at, items, total, 
                payment_status, payment_method, payment_date, shipping_status,
                shipping_status_date_awaiting_payment, shipping_status_date_preparing,
                shipping_status_date_sent, shipping_status_date_delivered
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['user_id'], None, tempo_id, row['created_at'], row['items'], row['total'],
                row['payment_status'], row['payment_method'], row['payment_date'], row['shipping_status'],
                row['shipping_status_date_awaiting_payment'], row['shipping_status_date_preparing'],
                row['shipping_status_date_sent'], row['shipping_status_date_delivered']
            ))
        
        conn.commit()
        print(f"Loaded {len(orders_df)} orders records")
    except Exception as e:
        conn.rollback()
        print(f"Error loading orders data: {e}")

# Main function
def main():
    # Get database connection
    conn = get_connection()
    if not conn:
        return
    
    # Get SQLAlchemy engine
    engine = get_engine()
    
    try:
        # Create tables
        create_tables(conn)
        
        # Generate time dimension
        generate_time_dimension(conn)
        
        # Load dimension tables first
        load_users_data(engine)
        load_products_data(engine)
        
        # Load fact table last
        load_orders_data(conn, engine)
        
        print("Data loading completed successfully")
    except Exception as e:
        print(f"Error in main process: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()