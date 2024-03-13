
-- NOTE: Langkah2
-- 1. Run file Query Dummy Scan Record VM sertakan listed vehicle_tag_no nya
-- 2. Run file Query Dummy Count AWB-Weight From VM sertakan listed vehicle_tag_no nya
-- 3. Setelah itu baru running Query ini


WITH
scan_record_vm AS (

SELECT *

FROM (

SELECT
            currenttab.waybill_no,
            currenttab.vehicle_tag_no,
            currenttab.bag_no,
            option.option_name AS operation_type,
            currenttab.operation_branch_name AS operation_branch_name,
            currenttab.recipient_city_name,
            -- currenttab.recipient_province_name,
            -- currenttab.register_reason_bahasa,
            -- currenttab.return_type_bahasa,
            DATETIME(currenttab.record_time,'Asia/Jakarta') AS record_time,
            LAG(currenttab.operation_branch_name,1) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS previous_branch_name,
            LAG(DATETIME(currenttab.record_time,'Asia/Jakarta'),1) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS previous_scan_time,
            -- LAG(currenttab.operation_branch_name,2) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS previous_branch_name_2,
            LEAD(currenttab.operation_branch_name) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_location_name,
            LEAD(DATETIME(currenttab.record_time,'Asia/Jakarta')) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_scan_time,
            LEAD(option.option_name) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_scan_type,
            LEAD(currenttab.operation_branch_name,1) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_location_name_2,
            LEAD(currenttab.operation_branch_name,2) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_location_name_3,
            LEAD(currenttab.operation_branch_name,3) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_location_name_4,
            LEAD(currenttab.operation_branch_name,4) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_location_name_5,
            -- MAX(currenttab.operation_branch_name) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time DESC) AS last_location,
            
            FROM
                `datawarehouse_idexp.dm_waybill_waybill_line` AS currenttab
                LEFT JOIN `datawarehouse_idexp.system_option` AS option ON currenttab.operation_type = option.option_value AND option.type_option = 'operationType'
                                                
            WHERE DATE(currenttab.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -45 DAY))
            -- WHERE DATE(currenttab.record_time,'Asia/Jakarta') BETWEEN '2024-01-01' AND '2024-01-31'
            AND currenttab.deleted = '0'

            AND currenttab.vehicle_tag_no IN (
"VM0100007749"

)

ORDER BY record_time DESC
            )

),

waybill_data AS (

  SELECT

  ww.waybill_no,
  CAST(ww.item_calculated_weight AS NUMERIC) item_weight,
  et.option_name express_type,


  FROM `datawarehouse_idexp.waybill_waybill` ww
  LEFT JOIN `datawarehouse_idexp.system_option` AS et ON ww.express_type = et.option_value AND et.type_option = 'expressType'

  WHERE DATE(ww.shipping_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY))

  QUALIFY ROW_NUMBER() OVER(PARTITION BY ww.waybill_no ORDER BY ww.update_time ASC)=1
),

join_scan_record_waybill AS (

  SELECT
  *

  FROM (
   
   SELECT
  sc.*,
  ww.item_weight,
  CONCAT(sc.vehicle_tag_no," ","-"," ",ww.waybill_no) vm_awb,

  FROM scan_record_vm sc
  LEFT JOIN waybill_data ww ON sc.waybill_no = ww.waybill_no
  )
  QUALIFY ROW_NUMBER() OVER(PARTITION BY vm_awb)=1
),

count_waybill_vm AS (

  WITH
scan_record_main AS (

SELECT
            currenttab.waybill_no,
            currenttab.vehicle_tag_no,
            -- currenttab.bag_no,
            option.option_name AS operation_type,
            currenttab.operation_branch_name AS operation_branch_name,
            CAST(ww.item_calculated_weight AS NUMERIC) item_calculated_weight,
            DATETIME(currenttab.record_time,'Asia/Jakarta') AS record_time,
            LAG(currenttab.operation_branch_name,1) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS previous_branch_name,
            LEAD(currenttab.operation_branch_name) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_location_name,
            LEAD(currenttab.operation_branch_name,1) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_location_name_2,
            LEAD(currenttab.operation_branch_name,2) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_location_name_3,
            LEAD(currenttab.operation_branch_name,3) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_location_name_4,
            LEAD(currenttab.operation_branch_name,4) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_location_name_5,
            
            FROM
                `datawarehouse_idexp.dm_waybill_waybill_line` AS currenttab
                LEFT JOIN `datawarehouse_idexp.system_option` AS option ON currenttab.operation_type = option.option_value AND option.type_option = 'operationType'
                LEFT OUTER JOIN `datawarehouse_idexp.waybill_waybill` ww ON currenttab.waybill_no = ww.waybill_no
                -- LEFT OUTER JOIN waybill_to_return rr ON currenttab.waybill_no = rr.waybill_no
                                                
            WHERE DATE(currenttab.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -45 DAY))
            -- WHERE DATE(currenttab.record_time,'Asia/Jakarta') BETWEEN '2023-10-01' AND '2024-02-20' -->= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -45 DAY))
            -- AND DATE(currenttab.record_time,'Asia/Jakarta') < DATE(rr.return_confirm_record_time)
            AND currenttab.deleted = '0'

            AND currenttab.vehicle_tag_no IN (
"VM0100007749"

)

            ORDER BY record_time DESC
),

get_vm_awb AS (

SELECT *

FROM (

  SELECT

vehicle_tag_no,
waybill_no,
item_calculated_weight,
CONCAT(vehicle_tag_no," ","-"," ",waybill_no) vm_awb,

FROM scan_record_main
)
QUALIFY ROW_NUMBER() OVER(PARTITION BY vm_awb)=1
)

SELECT 

vehicle_tag_no,
COUNT(waybill_no) total_awb,
SUM(item_calculated_weight) total_weight

FROM get_vm_awb

GROUP BY 1
),

get_first_scan_record_vm AS (

  SELECT

  sc.vehicle_tag_no,
  MIN(sc.operation_type) OVER (PARTITION BY sc.vehicle_tag_no ORDER BY sc.record_time ASC) ops_type,
  MIN(sc.operation_branch_name) OVER (PARTITION BY sc.vehicle_tag_no ORDER BY sc.record_time ASC) operation_branch_name,
  MIN(sc.record_time) OVER (PARTITION BY sc.vehicle_tag_no ORDER BY sc.record_time ASC) first_record_time,
  
  MIN(sc.previous_branch_name) OVER (PARTITION BY sc.vehicle_tag_no ORDER BY sc.record_time ASC) previous_branch_name,
  MIN(sc.next_location_name) OVER (PARTITION BY sc.vehicle_tag_no ORDER BY sc.record_time ASC) next_location_name,
  MIN(sc.next_location_name_3) OVER (PARTITION BY sc.vehicle_tag_no ORDER BY sc.record_time ASC) next_location_name_3,

  FROM scan_record_vm sc
  -- WHERE sc.operation_type = 'Sending scan'

QUALIFY ROW_NUMBER() OVER(PARTITION BY sc.vehicle_tag_no ORDER BY sc.record_time ASC)=1

),

get_sending_scan_vm AS (

  SELECT

  sc.vehicle_tag_no,
  MAX(sc.operation_type) OVER (PARTITION BY sc.vehicle_tag_no ORDER BY sc.record_time DESC) scan_type_2,
  MAX(sc.operation_branch_name) OVER (PARTITION BY sc.vehicle_tag_no ORDER BY sc.record_time DESC) max_sending_branch_vm,
  MAX(sc.record_time) OVER (PARTITION BY sc.vehicle_tag_no ORDER BY sc.record_time DESC) max_sending_time_vm,
  MAX(sc.next_location_name) OVER (PARTITION BY sc.vehicle_tag_no ORDER BY sc.record_time DESC) next_location_vm,


  FROM scan_record_vm sc
  WHERE sc.operation_type = 'Sending scan'

QUALIFY ROW_NUMBER() OVER(PARTITION BY sc.vehicle_tag_no ORDER BY sc.record_time ASC)=1
),

get_arrival_scan_vm AS (

  SELECT

  sc.vehicle_tag_no,
  MAX(sc.operation_type) OVER (PARTITION BY sc.vehicle_tag_no ORDER BY sc.record_time DESC) scan_type_2,
  MAX(sc.operation_branch_name) OVER (PARTITION BY sc.vehicle_tag_no ORDER BY sc.record_time DESC) max_arrivalbranch_vm,
  MAX(sc.record_time) OVER (PARTITION BY sc.vehicle_tag_no ORDER BY sc.record_time DESC) max_arrival_time_vm,
  MAX(sc.next_location_name) OVER (PARTITION BY sc.vehicle_tag_no ORDER BY sc.record_time DESC) next_location_vm,


  FROM scan_record_vm sc
  WHERE sc.operation_type = 'Arrival scan'

QUALIFY ROW_NUMBER() OVER(PARTITION BY sc.vehicle_tag_no ORDER BY sc.record_time ASC)=1
),

get_unloading_scan_vm AS (

  SELECT

  sc.vehicle_tag_no,
  MAX(sc.operation_type) OVER (PARTITION BY sc.vehicle_tag_no ORDER BY sc.record_time DESC) scan_type_3,
  MAX(sc.operation_branch_name) OVER (PARTITION BY sc.vehicle_tag_no ORDER BY sc.record_time DESC) max_unloading_branch_vm,
  MAX(sc.record_time) OVER (PARTITION BY sc.vehicle_tag_no ORDER BY sc.record_time DESC) max_unloading__time_vm,
  MAX(sc.next_location_name) OVER (PARTITION BY sc.vehicle_tag_no ORDER BY sc.record_time DESC) next_location_vm,


  FROM scan_record_vm sc
  WHERE sc.operation_type = 'Unloading scan'

QUALIFY ROW_NUMBER() OVER(PARTITION BY sc.vehicle_tag_no ORDER BY sc.record_time ASC)=1
),

get_weight__awb_vm AS (

  SELECT

  vehicle_tag_no,
  SUM(item_weight) total_weight,
  COUNT(waybill_no) total_awb,

  FROM (

  SELECT
  *
  FROM join_scan_record_waybill
  )
GROUP BY 1
),

-- SELECT * FROM scan_record_vm
gabung_all AS (

  SELECT

  -- sc.*,
  c.vehicle_tag_no,
  -- sc.previous_branch_name,
  sc.operation_branch_name,
  -- sc.first_record_time record_time,
  -- sc.ops_type operation_type,
  sc.next_location_name_3 next_branch_name,
  c.total_awb,
  c.total_weight,
  -- a.max_sending_branch_vm,
  a.max_sending_time_vm sending_time_vm,
  b.max_arrival_time_vm arrival_time_vm,
  d.max_unloading__time_vm unloading_time_vm,


  -- FROM get_first_scan_record_vm sc
  FROM count_waybill_vm c
  LEFT JOIN get_first_scan_record_vm sc ON c.vehicle_tag_no = sc.vehicle_tag_no
  LEFT JOIN get_sending_scan_vm a ON c.vehicle_tag_no = a.vehicle_tag_no
  LEFT JOIN get_arrival_scan_vm b ON c.vehicle_tag_no = b.vehicle_tag_no
  -- LEFT JOIN count_waybill_vm c ON sc.vehicle_tag_no = c.vehicle_tag_no
  LEFT JOIN get_unloading_scan_vm d ON c.vehicle_tag_no = d.vehicle_tag_no

)

-- SELECT * FROM get_first_scan_record_vm
SELECT * FROM gabung_all

