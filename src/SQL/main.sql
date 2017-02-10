set hive.ppd.remove.duplicatefilters = false;
set start_date = '2016-12-01';
set end_date = '2017-01-08';

DROP TABLE IF EXISTS medintsev.ms_expres_cpa_clicks;
CREATE TABLE medintsev.ms_expres_cpa_clicks AS
SELECT
  ware_md5,
  block_id,
  'cpa' AS click_type,
  day,
  pp,
  geo_id,
  price / 100 * 30 AS price, -- в рублях
  split(splits_data, ',')[0] AS test_bucket_id,
  split(splits_data, ',')[2] AS test_bucket_split_id
FROM (
  SELECT
    ware_md5,
    block_id,
    day,
    pp,
    geo_id,
    price,
    test_buckets
  FROM (
    SELECT
      ware_md5,
      block_id,
      day,
      pp,
      geo_id,
      price,
      test_buckets,
      row_number() OVER (PARTITION BY ware_md5, block_id) rn
    FROM analyst.cart_log
    WHERE
      day BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date}
      AND nvl(filter, 0) = 0 -- убираю накрутку
      AND state = 1 -- убираю действия сотрудников яндекса
      AND nvl(type_id, 0) = 0  -- только нажатия на кнопку в корзину
      AND nvl(yandexuid, '') <> '' -- указан yandexuid
      -- проверяем, что индентификаторы заполнены
      AND COALESCE(ware_md5, '') <> ''
      AND COALESCE(block_id, '') <> ''
      AND COALESCE(test_buckets, '') <> ''
    ) ms_cpa_clicks
  WHERE rn = 1
) ms_cpa_clicks_filtered LATERAL VIEW explode(split(test_buckets, ';')) tmp_table AS splits_data;

DROP TABLE IF EXISTS medintsev.ms_expres_cpc_clicks;
CREATE TABLE medintsev.ms_expres_cpc_clicks AS
SELECT
  ware_md5,
  block_id,
  'cpc' AS click_type,
  day,
  pp,
  geo_id,
  price / 100 * 30 AS price, -- в рублях
  split(splits_data, ',')[0] AS test_bucket_id,
  split(splits_data, ',')[2] AS test_bucket_split_id
FROM (
  SELECT
    ware_md5,
    block_id,
    day,
    pp,
    geo_id,
    price,
    test_buckets
  FROM (
    SELECT
      ware_md5,
      block_id,
      day,
      pp,
      geo_id,
      price,
      test_buckets,
      row_number() OVER (PARTITION BY ware_md5, block_id) rn
    FROM analyst.clicks_log
    WHERE
      day BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date} AND
      nvl(filter, 0) = 0 AND  -- убираю накрутку
      state = 1 AND -- убираю действия сотрудников яндекса
      nvl(yandexuid, '') <> '' -- указан yandexuid
      -- проверяем, что индентификаторы заполнены
      AND COALESCE(ware_md5, '') <> ''
      AND COALESCE(block_id, '') <> ''
      AND COALESCE(test_buckets, '') <> ''
    ) ms_cpa_clicks
  WHERE rn = 1
) ms_cpa_clicks_filtered LATERAL VIEW explode(split(test_buckets, ';')) tmp_table AS splits_data;

DROP TABLE IF EXISTS medintsev.ms_expres_cpa_orders;
CREATE TABLE medintsev.ms_expres_cpa_orders AS
SELECT
  ware_md5,
  block_id,
  cpa_order_is_billed,
  revenue_is_billed
FROM (
  SELECT
    offer_ware_md5 AS ware_md5,
    show_block_id AS block_id,
    IF(order_is_billed = 1, 1, 0) AS cpa_order_is_billed,
    IF(order_is_billed = 1, item_revenue * 30, 0) as revenue_is_billed, -- в рублях
    row_number() OVER (PARTITION BY offer_ware_md5, show_block_id) rn
  FROM analyst.orders_dict
  WHERE
    creation_day BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date}
    AND NOT order_is_fake
    AND NOT buyer_is_fake
    AND NOT shop_is_fake
    -- проверяем, что индентификаторы заполнены
    AND COALESCE(offer_ware_md5, '') <> ''
    AND COALESCE(show_block_id, '') <> ''
) t1
WHERE rn = 1;

DROP TABLE IF EXISTS medintsev.ms_expres_pre_result;
CREATE TABLE medintsev.ms_expres_pre_result AS
SELECT
  ms_clicks.click_type,
  ms_clicks.day,
  ms_clicks.pp,
  ms_clicks.geo_id,
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
              250), true, false) AS pp_Market,

  ms_clicks.price,
  IF(ms_expres_cpa_orders.ware_md5 IS NULL, 0, 1) AS cpa_order,
  IF(ms_expres_cpa_orders.ware_md5 IS NULL, 0, cpa_order_is_billed) AS cpa_order_is_billed,
  IF(ms_expres_cpa_orders.ware_md5 IS NULL, 0, revenue_is_billed) AS revenue_is_billed
FROM (
  SELECT
    ware_md5,
    block_id,
    click_type,
    day,
    pp,
    geo_id,
    price,
    test_bucket_id,
    test_bucket_split_id
  FROM medintsev.ms_expres_cpa_clicks
    UNION ALL
  SELECT
    ware_md5,
    block_id,
    click_type,
    day,
    pp,
    geo_id,
    price,
    test_bucket_id,
    test_bucket_split_id
  FROM medintsev.ms_expres_cpc_clicks) as ms_clicks LEFT JOIN medintsev.ms_expres_cpa_orders
    ON ms_clicks.ware_md5 = ms_expres_cpa_orders.ware_md5
      AND ms_clicks.block_id = ms_expres_cpa_orders.block_id
      AND ms_clicks.click_type = 'cpa';