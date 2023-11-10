WITH
root_bm_loading AS (

  SELECT
  sc.waybill_no,
  sc.bag_no bag_no_loading,
  MAX(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) mh_loading,
  -- sc.operation_branch_name mh_loading,
  sc.next_location_name next_location,
  MAX(DATE(sc.record_time,'Asia/Jakarta')) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) loading_date,
  -- DATE(sc.record_time, 'Asia/Jakarta') AS unloading_date,
  -- CONCAT(sc.vehicle_tag_no,' ','-',' ',sc.waybill_no) AS vm_awb_concat,
  MAX(rd1.option_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC)  AS loading_scan,


FROM `datawarehouse_idexp.waybill_waybill_line` sc
  LEFT JOIN `datawarehouse_idexp.system_option` rd1 ON sc.operation_type = rd1.option_value AND rd1.type_option = 'operationType'

WHERE DATE(sc.record_time,'Asia/Jakarta') BETWEEN '2023-10-01' AND '2023-10-31' --CURRENT_DATE() -->= (DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY)))
AND sc.operation_type = '03'
AND SUBSTR(sc.operation_branch_name,1,2) IN ('MH','DC')
-- AND SUBSTR(sc.next_location_name,1,2) IN ('TH','VT','VH')
AND SUBSTR(sc.bag_no,1,2) IN ('BM')

QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC)=1
),

get_data_th_arrival AS (

  SELECT
  a.waybill_no,
  a.bag_no_loading,
  a.mh_loading,
  a.loading_date,
  a.loading_scan,
  MAX(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) th_arrival,
  MAX(sc.previous_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) previous_branch_name,
  MAX(DATE(sc.record_time,'Asia/Jakarta')) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) arrival_date,
  MAX(rd1.option_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) arrival_scan,
  MAX(sc.bag_no) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) bag_no_arrival,

  FROM root_bm_loading a
  LEFT JOIN `datawarehouse_idexp.waybill_waybill_line` sc ON a.waybill_no = sc.waybill_no
  AND DATE(sc.record_time,'Asia/Jakarta') BETWEEN '2023-10-01' AND '2023-10-31'
    AND sc.operation_type = '05'
  LEFT JOIN `datawarehouse_idexp.system_option` rd1 ON sc.operation_type = rd1.option_value AND rd1.type_option = 'operationType'

WHERE SUBSTR(sc.operation_branch_name,1,2) IN ('TH','VT','VH')
AND SUBSTR(sc.previous_branch_name,1,2) IN ('MH','DC')
AND SUBSTR(sc.bag_no,1,2) IN ('BM')

QUALIFY ROW_NUMBER() OVER (PARTITION BY a.waybill_no ORDER BY sc.record_time DESC)=1
)

SELECT 

loading_date,
mh_loading,
th_arrival next_location,
SUM(bag_no_alias) total_bm,

FROM (

SELECT *,
CASE WHEN bag_no_loading IS NOT NULL THEN 1 ELSE 0 END AS bag_no_alias,

FROM get_data_th_arrival

WHERE mh_loading = previous_branch_name

QUALIFY ROW_NUMBER() OVER (PARTITION BY bag_no_loading)=1
)
GROUP BY 1,2,3
ORDER BY loading_date ASC
