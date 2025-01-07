SELECT

  authorized,
  created_at,
  customer_id,
  deleted,
  favorite,
  id,
  origin,
  phone,
  updated_at

FROM {{ get_catalogo('silver') }}.stage.vw_assine_tbl_contact
