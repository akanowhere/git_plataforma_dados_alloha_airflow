WITH orders AS (
  SELECT * FROM {{ ref('vw_hub_sva_orders') }}
),

customers AS (
  SELECT * FROM {{ ref('vw_hub_sva_customers') }}
),

products AS (
  SELECT * FROM {{ ref('vw_hub_sva_products') }}
),

providers AS (
  SELECT * FROM {{ ref('vw_hub_sva_providers') }}
),

brands AS (
  SELECT * FROM {{ ref('vw_hub_sva_brands') }}
),

partners AS (
  SELECT * FROM {{ ref('vw_hub_sva_partners') }}
),

joined AS (
  SELECT
    orders.id AS id_ordem_hub,
    orders.id_transacao,
    orders.token,
    orders.data_criacao,
    orders.data_atualizacao,
    orders.status,
    orders.status_esperado,
    customers.id AS id_cliente_hub,
    customers.cpf_cnpj,
    customers.id_contrato,
    products.id AS id_produto_hub,
    products.sku_produto,
    products.produto,
    providers.id AS id_provedor_hub,
    providers.provedor,
    brands.id AS id_marca_hub,
    brands.marca,
    partners.id AS id_polo_hub,
    partners.polo

  FROM orders
  LEFT JOIN customers ON orders.customer_id = customers.id
  LEFT JOIN products ON orders.product_id = products.id
  LEFT JOIN providers ON products.provider_id = providers.id
  LEFT JOIN brands ON orders.brand_id = brands.id
  LEFT JOIN partners ON brands.partner_id = partners.id
)

SELECT *
FROM
  joined
