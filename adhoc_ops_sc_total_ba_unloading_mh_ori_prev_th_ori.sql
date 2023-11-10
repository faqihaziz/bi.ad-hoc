WITH 

root_ba_arrival AS (

  SELECT
  sc.waybill_no,
  sc.bag_no bag_no_arrival,
  -- sc.vehicle_tag_no,
  sc.previous_branch_name th_origin,
  sc.operation_branch_name mh_arrival,
  -- sc.next_location_name next_location,
  DATE(sc.record_time, 'Asia/Jakarta') AS arrival_date,
  -- CONCAT(sc.vehicle_tag_no,' ','-',' ',sc.waybill_no) AS vm_awb_concat,
  rd1.option_name AS arrival_scan,


FROM `datawarehouse_idexp.waybill_waybill_line` sc
  LEFT JOIN `datawarehouse_idexp.system_option` rd1 ON sc.operation_type = rd1.option_value AND rd1.type_option = 'operationType'

WHERE DATE(sc.record_time,'Asia/Jakarta') BETWEEN '2023-10-01' AND '2023-10-31' --CURRENT_DATE() -->= (DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY)))
AND sc.operation_type = '05'
AND SUBSTR(sc.operation_branch_name,1,2) IN ('MH','DC')
AND SUBSTR(sc.previous_branch_name,1,2) IN ('TH','VT','VH')
AND SUBSTR(sc.bag_no,1,2) IN ('BA')

QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC)=1

),

get_data_unloading AS (

  SELECT

a.waybill_no,
a.bag_no_arrival,
a.th_origin,
a.mh_arrival,
a.arrival_date,
a.arrival_scan,
sc.bag_no bag_no_unloading,
MIN(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) mh_unloading,
MIN(DATE(sc.record_time,'Asia/Jakarta')) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) unloading_date,
MIN(rd1.option_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) unloading_scan,

  FROM root_ba_arrival a
  LEFT JOIN `datawarehouse_idexp.waybill_waybill_line` sc ON a.waybill_no = sc.waybill_no
  AND DATE(sc.record_time,'Asia/Jakarta') BETWEEN '2023-10-01' AND '2023-10-31'
    AND sc.operation_type = '06'
  LEFT JOIN `datawarehouse_idexp.system_option` rd1 ON sc.operation_type = rd1.option_value AND rd1.type_option = 'operationType'

WHERE SUBSTR(sc.operation_branch_name,1,2) IN ('MH','DC')
-- AND SUBSTR(sc.previous_branch_name,1,2) IN ('TH','VT','VH')
AND SUBSTR(sc.bag_no,1,2) IN ('BA')

QUALIFY ROW_NUMBER() OVER (PARTITION BY a.waybill_no ORDER BY sc.record_time ASC)=1
)

SELECT 

-- bag_no_unloading,
unloading_date,
th_origin prev_branch_name,
mh_unloading operation_branch_name,
SUM(bag_no_alias) total_ba,


FROM (
SELECT *,
CASE WHEN bag_no_unloading IS NOT NULL THEN 1 ELSE 0 END AS bag_no_alias,

FROM get_data_unloading

WHERE mh_unloading = mh_arrival
AND bag_no_arrival = bag_no_unloading

QUALIFY ROW_NUMBER() OVER (PARTITION BY bag_no_unloading)=1
)
GROUP BY 1,2,3
ORDER BY unloading_date ASC

