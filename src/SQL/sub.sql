SELECT
  test_bucket_id,
  test_bucket_split_id % ${hiveconf:num_splits} AS split_id,
  pp_DO,
  pp_KM,
  pp_Market,

  SUM(IF(click_type = 'cpc', 1, 0)) AS cpc_clicks_num,
  SUM(IF(click_type = 'cpa', 1, 0)) AS cpa_clicks_num,
  SUM(IF(click_type = 'cpc', click_price_rub, 0)) AS cpc_clicks_price, -- в рублях

  SUM(IF(click_type = 'cpc', orders_num, 0)) AS cpc_orders_num,
  SUM(IF(click_type = 'cpa', orders_num, 0)) AS cpa_orders_num,

  SUM(IF(click_type = 'cpa', order_is_billed, 0)) AS cpa_order_is_billed_num,
  SUM(IF(click_type = 'cpa', revenue_is_billed_rub, 0)) AS cpa_order_is_billed_revenue, -- в рублях

  COUNT(*) AS all_clicks_num,
  COALESCE(SUM(click_price_rub), 0) + COALESCE(SUM(revenue_is_billed_rub), 0) AS all_money,
  COALESCE(SUM(orders_num), 0) AS all_orders

FROM medintsev.ms_expres_pre_result
WHERE
  day BETWEEN ${hiveconf:start_day} AND ${hiveconf:end_day}
  AND pp_Market = True
  AND array_contains(${hiveconf:test_bucket_ids}, INT(test_bucket_id))
  ${hiveconf:ext_conditions}
  
GROUP BY
  test_bucket_id,
  test_bucket_split_id % ${hiveconf:num_splits},
  pp_DO,
  pp_KM,
  pp_Market