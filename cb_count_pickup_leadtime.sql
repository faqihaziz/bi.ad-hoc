SELECT 
leadtime_pickup,
-- *
COUNT(waybill_no) count_waybill,

FROM (

  SELECT

  oo.waybill_no,
  sr.option_name AS order_source,
  DATETIME(oo.input_time,'Asia/Jakarta') input_time,
  DATETIME(oo.pickup_time,'Asia/Jakarta') pickup_time,
  date_diff(DATE(oo.pickup_time,'Asia/Jakarta'), DATE(oo.input_time, 'Asia/Jakarta'), DAY) AS leadtime_pickup,

  FROM `datawarehouse_idexp.order_order` oo
  LEFT OUTER join `grand-sweep-324604.datawarehouse_idexp.system_option` sr on oo.order_source  = sr.option_value and sr.type_option = 'orderSource'
  WHERE DATE(input_time,'Asia/Jakarta') BETWEEN '2023-01-01' AND '2023-03-31'

GROUP BY 1

ORDER BY leadtime_pickup DESC
-- LIMIT 100000
