WITH data_pod AS (

  SELECT

-- branch_name,
-- district_name district_name_2,
-- recipient_district_name district_name,
waybill_source,
vip_customer_name,
pod_date report_date,
-- SUM(total_pickup) total_pickup,
SUM(total_pod) total_pod,
-- SUM(total_return_confirmed) total_return_confirmed,
-- SUM(total_pod_return) total_pod_return,

  FROM (

SELECT
  ww.waybill_no,
  CASE WHEN ww.waybill_no IS NOT NULL THEN 1 END AS waybill_alias,
  FORMAT_DATE("%b %Y", DATE(ww.pod_record_time,'Asia/Jakarta')) AS month,
  DATE(ww.pod_record_time,'Asia/Jakarta') pod_date,
  sr.option_name AS waybill_source,
  ww.pod_branch_name branch_name,
  ww.recipient_district_name,
  ww.parent_shipping_cleint vip_customer_name,
  fk,district_name,
  0 AS total_pickup,
CASE WHEN pod_record_time IS NOT NULL THEN 1 ELSE 0 END AS total_pod,
  0 AS total_return_confirmed,
  0 AS total_pod_return,


  FROM `datawarehouse_idexp.waybill_waybill` ww
  -- LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON ww.waybill_no = rr.waybill_no AND rr.deleted = '0'
left join `grand-sweep-324604.datawarehouse_idexp.system_option` sr on ww.waybill_source  = sr.option_value and sr.type_option = 'waybillSource'
left join `grand-sweep-324604.datawarehouse_idexp.system_option` et on ww.express_type  = et.option_value and et.type_option = 'expressType'
LEFT OUTER JOIN `dev_idexp.masterdata_branch_coverage_th` fk ON ww.pickup_branch_name = fk.branch_name

WHERE DATE(ww.update_time,'Asia/Jakarta') BETWEEN '2023-10-01' AND '2023-10-31'
AND DATE(ww.pod_record_time,'Asia/Jakarta') BETWEEN '2023-10-01' AND '2023-10-31'
AND ww.void_flag = '0' AND ww.deleted= '0'
-- AND sr.option_name IN ('KiriminAja','Mengantar','Komerce','everpro','Hadid',"pt auto serba digital","PT. Auto Serba","OrderOnline","Berdu","BiteShip")
AND sr.option_name IN ('VIP Customer Portal')

QUALIFY ROW_NUMBER() OVER (PARTITION BY ww.waybill_no ORDER BY ww.update_time DESC)=1
  )
GROUP BY 1,2,3
)

SELECT * FROM data_pod

ORDER BY report_date ASC
