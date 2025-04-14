-- Physical Database Model for Seven Inc Data Warehouse
-- Based on the logical model in modelagem.puml

-- Create dimension table for users
CREATE TABLE dim_usuarios (
    user_id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    entry_date DATE NOT NULL,
    entry_time TIME NOT NULL,
    update_date DATE,
    email VARCHAR(100) NOT NULL,  -- Changed from e-mail to email to avoid issues with special characters
    cpf VARCHAR(14) NOT NULL
);

-- Create dimension table for products
CREATE TABLE dim_produtos (
    product_id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    stock INTEGER NOT NULL,
    created_at DATE NOT NULL,
    description TEXT
);

-- Create dimension table for time
CREATE TABLE dim_tempo (
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
CREATE TABLE fato_pedidos (
    pedido_id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    tempo_id INTEGER NOT NULL,
    created_at DATE NOT NULL,
    items INTEGER NOT NULL,
    total DECIMAL(10, 2) NOT NULL,
    payment_status VARCHAR(50) NOT NULL,
    payment_method VARCHAR(50) NOT NULL,
    payment_date DATE,
    shipping_status VARCHAR(50) NOT NULL,
    shipping_status_date_awaiting_payment DATE,
    shipping_status_date_preparing DATE,
    shipping_status_date_sent DATE,
    shipping_status_date_delivered DATE,
    
    -- Foreign key constraints
    FOREIGN KEY (user_id) REFERENCES dim_usuarios(user_id),
    FOREIGN KEY (product_id) REFERENCES dim_produtos(product_id),
    FOREIGN KEY (tempo_id) REFERENCES dim_tempo(tempo_id)
);

-- Create indexes for better query performance
CREATE INDEX idx_fato_pedidos_user_id ON fato_pedidos(user_id);
CREATE INDEX idx_fato_pedidos_product_id ON fato_pedidos(product_id);
CREATE INDEX idx_fato_pedidos_tempo_id ON fato_pedidos(tempo_id);
CREATE INDEX idx_fato_pedidos_payment_method ON fato_pedidos(payment_method);
CREATE INDEX idx_fato_pedidos_shipping_status ON fato_pedidos(shipping_status);
CREATE INDEX idx_dim_tempo_data ON dim_tempo(data);
CREATE INDEX idx_dim_tempo_ano ON dim_tempo(ano);

-- Helper function to populate the time dimension table
-- This can be used to pre-populate the dim_tempo table with dates
CREATE OR REPLACE FUNCTION populate_dim_tempo(start_date DATE, end_date DATE)
RETURNS VOID AS $$
DECLARE
    current_date DATE := start_date;
    current_id INTEGER := 1;
BEGIN
    WHILE current_date <= end_date LOOP
        INSERT INTO dim_tempo (
            tempo_id,
            data,
            dia_da_semana,
            eh_final_de_semana,
            eh_feriado,
            nome_mes,
            trimestre,
            semestre,
            ano
        ) VALUES (
            current_id,
            current_date,
            CASE EXTRACT(DOW FROM current_date)
                WHEN 0 THEN 'Domingo'
                WHEN 1 THEN 'Segunda-feira'
                WHEN 2 THEN 'Terça-feira'
                WHEN 3 THEN 'Quarta-feira'
                WHEN 4 THEN 'Quinta-feira'
                WHEN 5 THEN 'Sexta-feira'
                WHEN 6 THEN 'Sábado'
            END,
            EXTRACT(DOW FROM current_date) IN (0, 6),  -- Weekend check
            FALSE,  -- Holiday flag (would need to be updated with actual holiday data)
            CASE EXTRACT(MONTH FROM current_date)
                WHEN 1 THEN 'Janeiro'
                WHEN 2 THEN 'Fevereiro'
                WHEN 3 THEN 'Março'
                WHEN 4 THEN 'Abril'
                WHEN 5 THEN 'Maio'
                WHEN 6 THEN 'Junho'
                WHEN 7 THEN 'Julho'
                WHEN 8 THEN 'Agosto'
                WHEN 9 THEN 'Setembro'
                WHEN 10 THEN 'Outubro'
                WHEN 11 THEN 'Novembro'
                WHEN 12 THEN 'Dezembro'
            END,
            EXTRACT(QUARTER FROM current_date),
            CASE WHEN EXTRACT(MONTH FROM current_date) <= 6 THEN 1 ELSE 2 END,
            EXTRACT(YEAR FROM current_date)
        );
        
        current_date := current_date + INTERVAL '1 day';
        current_id := current_id + 1;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Example usage of the populate_dim_tempo function:
-- SELECT populate_dim_tempo('2018-01-01', '2025-12-31');