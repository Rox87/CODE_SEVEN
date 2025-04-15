-- Total de pedidos por mês
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

-- Receita total por método de pagamento
SELECT 
    payment_method,
    SUM(total) AS receita_total
FROM 
    pedidos_raw
GROUP BY 
    payment_method
ORDER BY 
    receita_total DESC;

-- Produto mais vendido (por quantidade)
-- Nota: Assumindo que existe uma tabela de itens de pedido não mostrada nos arquivos
-- Esta query é uma aproximação baseada nos dados disponíveis
SELECT 
    p.product_id,
    p.name AS nome_produto,
    SUM(i.quantity) AS quantidade_total
FROM 
    produtos_raw p
JOIN 
    itens_pedido i ON p.product_id = i.product_id
GROUP BY 
    p.product_id, p.name
ORDER BY 
    quantidade_total DESC
LIMIT 1;


-- Ticket médio por cliente
SELECT 
    o.user_id,
    u.name AS nome_cliente,
    AVG(o.total) AS ticket_medio
FROM 
    pedidos_raw o
JOIN 
    user_raw u ON o.user_id = u.user_id
GROUP BY 
    o.user_id, u.name
ORDER BY 
    ticket_medio DESC;

-- Pedidos por status de envio
SELECT 
    shipping_status,
    COUNT(*) AS total_pedidos
FROM 
    pedidos_raw
GROUP BY 
    shipping_status
ORDER BY 
    total_pedidos DESC;