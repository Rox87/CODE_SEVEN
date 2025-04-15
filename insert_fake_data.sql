-- Script para inserção de dados fake no PostgreSQL
-- Baseado no modelo dimensional definido em modelagem.puml

-- Limpar tabelas existentes (caso necessário)
TRUNCATE TABLE fato_pedidos CASCADE;
TRUNCATE TABLE dim_usuarios CASCADE;
TRUNCATE TABLE dim_produtos CASCADE;
TRUNCATE TABLE dim_tempo CASCADE;

-- Inserir dados na dimensão de usuários
INSERT INTO dim_usuarios (user_id, name, entry_date, entry_time, update_date, email, cpf) VALUES
(1, 'João Silva', '2020-01-15', '09:30:00', '2022-03-10', 'joao.silva@email.com', '123.456.789-10'),
(2, 'Maria Santos', '2020-02-20', '14:45:00', '2022-04-05', 'maria.santos@email.com', '234.567.890-21'),
(3, 'Pedro Oliveira', '2020-03-10', '11:15:00', '2022-05-12', 'pedro.oliveira@email.com', '345.678.901-32'),
(4, 'Ana Souza', '2020-04-05', '16:20:00', '2022-06-18', 'ana.souza@email.com', '456.789.012-43'),
(5, 'Carlos Ferreira', '2020-05-22', '10:00:00', '2022-07-25', 'carlos.ferreira@email.com', '567.890.123-54'),
(6, 'Juliana Lima', '2020-06-18', '13:30:00', '2022-08-30', 'juliana.lima@email.com', '678.901.234-65'),
(7, 'Roberto Costa', '2020-07-30', '15:45:00', '2022-09-15', 'roberto.costa@email.com', '789.012.345-76'),
(8, 'Fernanda Almeida', '2020-08-12', '08:50:00', '2022-10-20', 'fernanda.almeida@email.com', '890.123.456-87'),
(9, 'Marcelo Rodrigues', '2020-09-25', '17:10:00', '2022-11-05', 'marcelo.rodrigues@email.com', '901.234.567-98'),
(10, 'Patrícia Gomes', '2020-10-08', '12:25:00', '2022-12-12', 'patricia.gomes@email.com', '012.345.678-09');

-- Inserir dados na dimensão de produtos
INSERT INTO dim_produtos (product_id, name, price, stock, created_at, description) VALUES
(1, 'Smartphone Galaxy X10', 1299.99, 50, '2021-01-10', 'Smartphone de última geração com câmera de alta resolução'),
(2, 'Notebook UltraSlim', 3499.90, 30, '2021-02-15', 'Notebook leve e potente para trabalho e entretenimento'),
(3, 'Smart TV 55"', 2799.50, 25, '2021-03-20', 'Smart TV com resolução 4K e sistema operacional integrado'),
(4, 'Fone de Ouvido Bluetooth', 199.90, 100, '2021-04-25', 'Fone de ouvido sem fio com cancelamento de ruído'),
(5, 'Câmera Digital Profissional', 4999.00, 15, '2021-05-30', 'Câmera DSLR para fotografia profissional'),
(6, 'Relógio Inteligente', 599.90, 60, '2021-06-05', 'Smartwatch com monitoramento de saúde e notificações'),
(7, 'Console de Videogame', 3999.00, 20, '2021-07-10', 'Console de última geração para jogos em alta definição'),
(8, 'Tablet Premium', 1899.90, 40, '2021-08-15', 'Tablet com tela de alta resolução e processador rápido'),
(9, 'Impressora Multifuncional', 799.50, 35, '2021-09-20', 'Impressora com scanner e copiadora integrados'),
(10, 'Caixa de Som Portátil', 349.90, 70, '2021-10-25', 'Caixa de som bluetooth à prova d\'água');

-- Inserir dados na dimensão de tempo (para um período de exemplo)
-- Primeiro, vamos criar uma função para facilitar a inserção de datas
DO $$
DECLARE
    current_date DATE := '2022-01-01';
    end_date DATE := '2022-12-31';
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
            CASE 
                WHEN (EXTRACT(MONTH FROM current_date) = 1 AND EXTRACT(DAY FROM current_date) = 1) THEN TRUE  -- Ano Novo
                WHEN (EXTRACT(MONTH FROM current_date) = 4 AND EXTRACT(DAY FROM current_date) = 21) THEN TRUE  -- Tiradentes
                WHEN (EXTRACT(MONTH FROM current_date) = 5 AND EXTRACT(DAY FROM current_date) = 1) THEN TRUE   -- Dia do Trabalho
                WHEN (EXTRACT(MONTH FROM current_date) = 9 AND EXTRACT(DAY FROM current_date) = 7) THEN TRUE   -- Independência
                WHEN (EXTRACT(MONTH FROM current_date) = 10 AND EXTRACT(DAY FROM current_date) = 12) THEN TRUE -- Nossa Senhora
                WHEN (EXTRACT(MONTH FROM current_date) = 11 AND EXTRACT(DAY FROM current_date) = 2) THEN TRUE  -- Finados
                WHEN (EXTRACT(MONTH FROM current_date) = 11 AND EXTRACT(DAY FROM current_date) = 15) THEN TRUE -- Proclamação da República
                WHEN (EXTRACT(MONTH FROM current_date) = 12 AND EXTRACT(DAY FROM current_date) = 25) THEN TRUE -- Natal
                ELSE FALSE
            END,
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
$$;

-- Inserir dados na tabela de fatos (pedidos)
-- Vamos criar 50 pedidos de exemplo
DO $$
DECLARE
    i INTEGER;
    random_user_id INTEGER;
    random_product_id INTEGER;
    random_tempo_id INTEGER;
    random_date DATE;
    random_items INTEGER;
    random_total DECIMAL(10,2);
    payment_methods TEXT[] := ARRAY['credit_card', 'debit_card', 'bank_transfer', 'pix', 'boleto'];
    random_payment_method TEXT;
    shipping_statuses TEXT[] := ARRAY['awaiting_payment', 'preparing', 'sent', 'delivered', 'canceled'];
    random_shipping_status TEXT;
    payment_date DATE;
    awaiting_date DATE;
    preparing_date DATE;
    sent_date DATE;
    delivered_date DATE;
BEGIN
    FOR i IN 1..50 LOOP
        -- Selecionar valores aleatórios
        random_user_id := floor(random() * 10) + 1;
        random_product_id := floor(random() * 10) + 1;
        
        -- Selecionar uma data aleatória de 2022
        random_date := '2022-01-01'::DATE + (random() * 364)::INTEGER;
        
        -- Encontrar o tempo_id correspondente à data
        SELECT tempo_id INTO random_tempo_id FROM dim_tempo WHERE data = random_date;
        
        -- Gerar quantidade e total aleatórios
        random_items := floor(random() * 5) + 1;
        
        -- Calcular o total baseado no preço do produto e na quantidade
        SELECT price * random_items INTO random_total FROM dim_produtos WHERE product_id = random_product_id;
        
        -- Selecionar método de pagamento aleatório
        random_payment_method := payment_methods[floor(random() * array_length(payment_methods, 1)) + 1];
        
        -- Selecionar status de envio aleatório
        random_shipping_status := shipping_statuses[floor(random() * array_length(shipping_statuses, 1)) + 1];
        
        -- Definir datas de status baseadas na data do pedido
        awaiting_date := random_date;
        preparing_date := CASE WHEN random_shipping_status IN ('preparing', 'sent', 'delivered') THEN random_date + (random() * 2)::INTEGER ELSE NULL END;
        sent_date := CASE WHEN random_shipping_status IN ('sent', 'delivered') THEN random_date + (random() * 3 + 2)::INTEGER ELSE NULL END;
        delivered_date := CASE WHEN random_shipping_status = 'delivered' THEN random_date + (random() * 5 + 5)::INTEGER ELSE NULL END;
        
        -- Definir data de pagamento (NULL se awaiting_payment)
        payment_date := CASE WHEN random_shipping_status != 'awaiting_payment' THEN random_date + (random() * 2)::INTEGER ELSE NULL END;
        
        -- Inserir o pedido
        INSERT INTO fato_pedidos (
            pedido_id,
            user_id,
            product_id,
            tempo_id,
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
        ) VALUES (
            i,
            random_user_id,
            random_product_id,
            random_tempo_id,
            random_date,
            random_items,
            random_total,
            CASE WHEN payment_date IS NULL THEN 'pending' ELSE 'paid' END,
            random_payment_method,
            payment_date,
            random_shipping_status,
            awaiting_date,
            preparing_date,
            sent_date,
            delivered_date
        );
    END LOOP;
END;
$$;

-- Verificar os dados inseridos
SELECT 'Usuários inseridos: ' || COUNT(*) FROM dim_usuarios;
SELECT 'Produtos inseridos: ' || COUNT(*) FROM dim_produtos;
SELECT 'Registros de tempo inseridos: ' || COUNT(*) FROM dim_tempo;
SELECT 'Pedidos inseridos: ' || COUNT(*) FROM fato_pedidos;

-- Exemplos de consultas para verificar os dados
-- 1. Top 5 usuários com mais pedidos
SELECT u.name, COUNT(f.pedido_id) as total_pedidos
FROM dim_usuarios u
JOIN fato_pedidos f ON u.user_id = f.user_id
GROUP BY u.name
ORDER BY total_pedidos DESC
LIMIT 5;

-- 2. Produtos mais vendidos por quantidade
SELECT p.name, SUM(f.items) as quantidade_total
FROM dim_produtos p
JOIN fato_pedidos f ON p.product_id = f.product_id
GROUP BY p.name
ORDER BY quantidade_total DESC;

-- 3. Vendas por mês
SELECT t.nome_mes, t.ano, COUNT(f.pedido_id) as total_pedidos, SUM(f.total) as receita_total
FROM dim_tempo t
JOIN fato_pedidos f ON t.tempo_id = f.tempo_id
GROUP BY t.nome_mes, t.ano, t.data
ORDER BY t.data;

-- 4. Distribuição de pedidos por status de envio
SELECT shipping_status, COUNT(*) as total
FROM fato_pedidos
GROUP BY shipping_status
ORDER BY total DESC;

-- 5. Receita por método de pagamento
SELECT payment_method, SUM(total) as receita_total
FROM fato_pedidos
GROUP BY payment_method
ORDER BY receita_total DESC;