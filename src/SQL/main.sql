set hive.ppd.remove.duplicatefilters = false;
set start_date = '2017-06-03';
set end_date = '2017-06-08';

DROP TABLE IF EXISTS medintsev.ms_expres_cpa_clicks;
CREATE TEMPORARY TABLE medintsev.ms_expres_cpa_clicks AS
WITH ms_cpa_clicks_raw AS (
  SELECT
    show_uid,
    day,
    pp,
    geo_id,
    price,
    test_buckets,

    row_number() OVER (PARTITION BY ware_md5, block_id) rn
  FROM
    analyst.cart_log
  WHERE
    day BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date}
    AND COALESCE(filter, 0) = 0 -- убираю накрутку
    AND COALESCE(state, 1) = 1 -- убираю действия сотрудников яндекса
    
    -- проверяем, что индентификаторы заполнены
    AND COALESCE(show_uid, '') <> ''
    AND COALESCE(test_buckets, '') <> ''
),

ms_cpa_orders AS (
  SELECT
    show_uid,
    IF(order_is_billed = 1, 1, 0) AS cpa_order_is_billed,
    IF(order_is_billed = 1, item_revenue * 30, 0) as revenue_is_billed -- в рублях

  FROM analyst.orders_dict
  WHERE
    creation_day BETWEEN ${hiveconf:start_date} AND date_add(${hiveconf:end_date}, 7)
    
    AND NOT order_is_fake
    AND NOT buyer_is_fake
    AND NOT shop_is_fake

    -- проверяем, что индентификаторы заполнены
    AND COALESCE(show_uid, '') <> ''
),

cpa_clicks_with_orders_raw AS (
  SELECT
    ms_cpa_clicks_raw.show_uid,
    ms_cpa_clicks_raw.day,
    ms_cpa_clicks_raw.pp,
    ms_cpa_clicks_raw.geo_id,
    ms_cpa_clicks_raw.price / 100 * 30 AS click_price_rub, -- в рублях
    ms_cpa_clicks_raw.test_buckets,

    IF(ms_cpa_orders.cpa_order_is_billed IS NOT NULL, 1, 0) AS orders_num,
    IF(ms_cpa_orders.cpa_order_is_billed = 1, 1, 0) AS order_is_billed,
    IF(ms_cpa_orders.cpa_order_is_billed IS NOT NULL, ms_cpa_orders.revenue_is_billed, 0) AS revenue_is_billed_rub
  FROM
    ms_cpa_clicks_raw LEFT JOIN ms_cpa_orders
      ON ms_cpa_clicks_raw.show_uid = ms_cpa_orders.show_uid
  WHERE
    ms_cpa_clicks_raw.rn = 1
)

SELECT
  show_uid,
  'cpa' AS click_type,
  day,
  pp,
  geo_id,
  click_price_rub,

  orders_num,
  order_is_billed,
  revenue_is_billed_rub,

  split(splits_data, ',')[0] AS test_bucket_id,
  split(splits_data, ',')[2] AS test_bucket_split_id
FROM cpa_clicks_with_orders_raw
  LATERAL VIEW explode(split(test_buckets, ';')) tmp_table AS splits_data;

DROP TABLE IF EXISTS medintsev.ms_expres_cpc_clicks;
CREATE TEMPORARY TABLE medintsev.ms_expres_cpc_clicks AS
WITH ms_cpc_clicks_raw AS (
  SELECT
    show_uid,
    day,
    pp,
    geo_id,
    price,
    test_buckets,

    hid,
    IF(geo_id = 213, 1, 0) AS is_moscow,
    cpa,

    row_number() OVER (PARTITION BY ware_md5, block_id) rn
  FROM analyst.clicks_log
  WHERE
    day BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date}
    AND COALESCE(filter, 0) = 0  -- убираю накрутку
    AND COALESCE(state, 1) = 1 -- убираю действия сотрудников яндекса
    
    -- проверяем, что индентификаторы заполнены
    AND COALESCE(show_uid, '') <> ''
    AND COALESCE(test_buckets, '') <> ''
),

ms_cpc_clicks_with_orders_raw AS (
  SELECT
    ms_cpc_clicks_raw.show_uid,
    ms_cpc_clicks_raw.day,
    ms_cpc_clicks_raw.pp,
    ms_cpc_clicks_raw.geo_id,
    ms_cpc_clicks_raw.price / 100 * 30 AS click_price_rub, -- в рублях
    ms_cpc_clicks_raw.test_buckets,

    conversions_ma2807_27mar.aconv_b AS orders_num,
    NULL AS order_is_billed,
    NULL AS revenue_is_billed_rub
  FROM
    ms_cpc_clicks_raw LEFT JOIN medintsev.conversions_ma2807_27mar
      ON ms_cpc_clicks_raw.hid = conversions_ma2807_27mar.hid
      AND ms_cpc_clicks_raw.is_moscow = conversions_ma2807_27mar.is_moscow
      AND ms_cpc_clicks_raw.cpa = conversions_ma2807_27mar.cpa
  WHERE
    ms_cpc_clicks_raw.rn = 1
)

SELECT
  show_uid,
  'cpc' AS click_type,
  day,
  pp,
  geo_id,
  click_price_rub, -- в рублях

  orders_num,
  order_is_billed,
  revenue_is_billed_rub,

  split(splits_data, ',')[0] AS test_bucket_id,
  split(splits_data, ',')[2] AS test_bucket_split_id
FROM ms_cpc_clicks_with_orders_raw
  LATERAL VIEW explode(split(test_buckets, ';')) tmp_table AS splits_data;

DROP TABLE IF EXISTS medintsev.ms_expres_pre_result;
-- TEMPORARY тут нельзя, так как не видно такую таблицу через JDBC
CREATE TABLE medintsev.ms_expres_pre_result AS
SELECT
  ms_clicks.show_uid,
  ms_clicks.click_type,
  ms_clicks.day,
  ms_clicks.pp,
  ms_clicks.geo_id,
  ms_clicks.click_price_rub,

  ms_clicks.orders_num,
  ms_clicks.order_is_billed,
  ms_clicks.revenue_is_billed_rub,

  ms_clicks.test_bucket_id,
  ms_clicks.test_bucket_split_id,

  -- https://wiki.yandex-team.ru/market/money/pp/aliases/
  IF(ms_clicks.pp in (200, 201, 205, 206, 207, 208, 209), true, false) AS pp_DO,

  IF(ms_clicks.pp in (6, 61, 62, 63, 64,
              13, 21,
              200, 201, 205, 206, 207, 208, 209,
              210, 211,
              26, 27,
              251,
              144), true, false) AS pp_KM,

  IF(ms_clicks.pp in (7, 28,
              24, 25,
              68, 69,

              6, 61, 62, 63, 64,
              13, 21,
              200, 201, 205, 206, 207, 208, 209,
              210, 211,
              26, 27,
              251,
              144,

              106, 146,
              59,
              238, 239,
              160, 161,
              22,
              145,
              143,
              250), true, false) AS pp_Market
FROM
  (
  SELECT
    show_uid,
    click_type,
    day,
    pp,
    geo_id,
    click_price_rub,

    orders_num,
    order_is_billed,
    revenue_is_billed_rub,

    test_bucket_id,
    test_bucket_split_id
  FROM medintsev.ms_expres_cpa_clicks
    UNION ALL
  SELECT
    show_uid,
    click_type,
    day,
    pp,
    geo_id,
    click_price_rub,

    orders_num,
    order_is_billed,
    revenue_is_billed_rub,
    
    test_bucket_id,
    test_bucket_split_id
  FROM medintsev.ms_expres_cpc_clicks) as ms_clicks;