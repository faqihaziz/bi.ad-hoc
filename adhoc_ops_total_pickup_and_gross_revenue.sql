
WITH
waybill_data AS (

  SELECT

  ww.waybill_no,
  FORMAT_DATE("%b %Y", DATE(ww.shipping_time,'Asia/Jakarta')) AS month_pickup,
  DATE(ww.shipping_time,'Asia/Jakarta') shipping_date,
  CAST(ww.item_calculated_weight AS NUMERIC) item_calculated_weight,
  et.option_name express_type,
  ww.recipient_district_name,
  ww.recipient_city_name,
  ww.recipient_province_name,
  ww.standard_shipping_fee,
  CASE
      WHEN ww.pod_record_time IS NOT NULL THEN ww.pod_branch_name
      WHEN ww.pod_record_time IS NULL AND ww.return_flag = '0' THEN ww.delivery_branch_name
      WHEN ww.pod_record_time IS NULL AND ww.return_flag = '0' AND ww.delivery_branch_name IS NULL THEN th.branch_name
      ELSE th.branch_name
      END AS th_destination,
  kw.kanwil_name,
  ww.recipient_district_id,

  FROM `datawarehouse_idexp.waybill_waybill` ww
  LEFT JOIN `datawarehouse_idexp.system_option` AS et ON ww.express_type = et.option_value AND et.type_option = 'expressType'
  LEFT JOIN `datamart_idexp.masterdata_facility_to_kanwil` kw ON ww.recipient_province_name = kw.province_name
  LEFT JOIN `dev_idexp.masterdata_branch_coverage_th` th ON ww.recipient_district_id = th.district_id

  -- WHERE DATE(ww.shipping_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY))
  WHERE DATE(ww.shipping_time,'Asia/Jakarta') BETWEEN '2024-02-01' AND '2024-02-29'
  AND ww.void_flag = '0' AND ww.deleted = '0'

  QUALIFY ROW_NUMBER() OVER(PARTITION BY ww.waybill_no ORDER BY ww.update_time DESC)=1
),

first_deliv_attempt AS (

SELECT
sc.waybill_no,
MIN(DATETIME(sc.record_time,'Asia/Jakarta')) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) AS first_deliv_attempt,
MIN(rd16.option_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) AS scan_type,
MIN(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) AS first_deliv_branch,

FROM `datawarehouse_idexp.waybill_waybill_line` sc 
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd16 ON rd16.option_value = sc.operation_type AND rd16.type_option = 'operationType'

WHERE 
DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))
AND operation_type = "09"

QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC)=1

),

waybill_to_facility AS (

  SELECT 
  ww.*,
  CASE
      WHEN ww.th_destination IS NULL THEN fd.first_deliv_branch 
      WHEN ww.th_destination IS NULL AND fd.first_deliv_branch IS NULL THEN th.branch_name
      ELSE ww.th_destination END AS th_name,
  th.branch_name,

  FROM waybill_data ww
  LEFT JOIN `dev_idexp.masterdata_branch_coverage_th` th ON ww.recipient_district_id = th.district_id
  LEFT JOIN first_deliv_attempt fd ON ww.waybill_no = fd.waybill_no
),

count_dest_waybill AS (

SELECT * FROM (

  SELECT

month_pickup,
recipient_district_name,
CASE
    WHEN th_name IS NULL THEN branch_name ELSE th_name END AS th_name,
-- branch_name,
recipient_city_name,
recipient_province_name,
SUM(standard_shipping_fee) gross_revenue,
COUNT(waybill_no) total_awb,


-- FROM waybill_data
FROM waybill_to_facility

GROUP BY 1,2,3,4,5
)
)

-- SELECT * FROM waybill_data
SELECT
*
-- recipient_district_name,
-- CASE
--     WHEN th_name IS NULL THEN branch_name 
--     ELSE th_name END AS th_name,

FROM count_dest_waybill
-- WHERE th_name IS NULL


