WITH order_data AS (

  SELECT

  waybill_source,
  month,
  report_date,
  sender_name,
  branch_name,
  sender_city_name,
  sender_province_name,
  SUM(waybill_alias) total_order_created,
  -- SUM(total_rpu) total_rpu,
  0 AS total_shipping,

  FROM (

  SELECT
  oo.waybill_no,
  CASE WHEN oo.waybill_no IS NOT NULL THEN 1 ELSE 0 END AS waybill_alias,
  -- CASE WHEN oo.waybill_no IS NOT NULL THEN 1 ELSE 0 END AS total_rpu,
  sr.option_name waybill_source,
  FORMAT_DATE("%b %Y", DATE(oo.input_time,'Asia/Jakarta')) AS month,
  -- FORMAT_DATE("%b %Y", DATE(tp.request_pickup_time)) AS month,
  DATE(oo.input_time,'Asia/Jakarta') report_date,
  -- DATE(tp.request_pickup_time) report_date,
  oo.sender_name,
  CASE WHEN oo.pickup_time IS NOT NULL THEN oo.pickup_branch_name ELSE oo.scheduling_target_branch_name END AS branch_name,
  oo.sender_province_name,
  oo.sender_city_name,



FROM `datawarehouse_idexp.order_order` oo
-- LEFT OUTER JOIN `dev_idexp.temp_table_tokped_rpu` tp ON oo.order_no = tp.order_no
left join `grand-sweep-324604.datawarehouse_idexp.system_option` sr on oo.order_source  = sr.option_value and sr.type_option = 'orderSource'
-- left join `grand-sweep-324604.datawarehouse_idexp.system_option` et on ww.express_type  = et.option_value and et.type_option = 'expressType'

WHERE DATE(oo.input_time,'Asia/Jakarta') BETWEEN '2023-10-01' AND '2023-10-31'
-- WHERE DATE(tp.request_pickup_time) BETWEEN '2023-10-01' AND '2023-10-30'
-- AND sr.option_name IN ('Tokopedia')
AND sr.option_name IN ('Shopee platform')

)
GROUP BY 1,2,3,4,5,6,7
ORDER BY report_date ASC
),

pickup_data AS (
  SELECT

  waybill_source,
  month,
  report_date,
  sender_name,
  branch_name,
  sender_city_name,
  sender_province_name,
  0 AS total_order_created,
  -- 0 AS total_rpu,
  SUM(waybill_alias) total_shipping,

  FROM (

    SELECT 
  
  ww.waybill_no,
  CASE WHEN ww.waybill_no IS NOT NULL THEN 1 ELSE 0 END AS waybill_alias,
  sr.option_name waybill_source,
  FORMAT_DATE("%b %Y", DATE(ww.shipping_time,'Asia/Jakarta')) AS month,
  DATE(ww.shipping_time,'Asia/Jakarta') report_date,
  ww.sender_name,
  ww.pickup_branch_name branch_name,
  ww.sender_province_name,
  ww.sender_city_name,

FROM `datawarehouse_idexp.waybill_waybill` ww
left join `grand-sweep-324604.datawarehouse_idexp.system_option` sr on ww.waybill_source  = sr.option_value and sr.type_option = 'waybillSource'
-- left join `grand-sweep-324604.datawarehouse_idexp.system_option` et on ww.express_type  = et.option_value and et.type_option = 'expressType'

WHERE DATE(ww.shipping_time,'Asia/Jakarta') BETWEEN '2023-10-01' AND '2023-10-31'
AND ww.void_flag = '0' AND ww.deleted= '0'

AND sr.option_name IN ('Shopee platform')
  )
GROUP BY 1,2,3,4,5,6,7
ORDER BY report_date ASC
)

SELECT

  waybill_source,
  month,
  report_date,
  -- sender_name,
  -- branch_name,
  -- sender_city_name,
  -- sender_province_name,
  SUM(total_order_created) total_order_created,
  -- SUM(total_rpu) total_rpu,
  SUM(total_shipping) total_shipping,

FROM (
SELECT * FROM order_data UNION ALL
SELECT * FROM pickup_data
)

-- WHERE branch_name IS NULL

GROUP BY 1,2,3
ORDER BY report_date ASC


