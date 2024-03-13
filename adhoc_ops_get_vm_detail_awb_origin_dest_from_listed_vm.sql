
-- NOTE: Langkah2
-- 1. Run file Query Dummy Scan Record VM sertakan listed vehicle_tag_no nya
-- 2. Run file Query Dummy Count AWB-Weight From VM sertakan listed vehicle_tag_no nya
-- 3. Setelah itu baru running Query ini

WITH
    get_vm_record AS (
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
            -- AND currenttab.operation_type = '18'
            
            -- AND waybill_no IN (
                        -- ) --'IDE703620797395', 'IDM500837282987'
            -- AND bag_no IN ('BM1830016569')
            -- AND operation_branch_name NOT IN ('MH JAKARTA')
            -- AND option.option_name IN ('Problem On Shipment scan','Delivery scan')
            AND currenttab.vehicle_tag_no IN (
"VF1130035449",
"VF1130034640",
"VA0630039536"
            )

ORDER BY record_time DESC
            )

),

waybill_data AS (

  SELECT

  ww.waybill_no,
  CAST(ww.item_calculated_weight AS NUMERIC) item_calculated_weight,

  FROM `datawarehouse_idexp.waybill_waybill` ww
  WHERE DATE(ww.shipping_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY))

  QUALIFY ROW_NUMBER() OVER(PARTITION BY ww.waybill_no ORDER BY ww.update_time DESC)=1
),

join_scan_record_waybill AS (

  SELECT
  *

  FROM (
   
   SELECT
  sc.*,
  ww.item_calculated_weight,
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
            currenttab.bag_no,
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
            
            DATETIME(ww.shipping_time,'Asia/Jakarta') shipping_time,
            ww.pickup_branch_name,
            ww.sender_district_name,
            ww.sender_city_name,
            ww.sender_province_name,
            ww.recipient_district_name,
            ww.recipient_city_name,
            ww.recipient_province_name,
            CAST(ww.standard_shipping_fee AS NUMERIC) standard_shipping_fee,
            et.option_name express_type,


            FROM
                `datawarehouse_idexp.dm_waybill_waybill_line` AS currenttab
                LEFT JOIN `datawarehouse_idexp.system_option` AS option ON currenttab.operation_type = option.option_value AND option.type_option = 'operationType'
                LEFT OUTER JOIN `datawarehouse_idexp.waybill_waybill` ww ON currenttab.waybill_no = ww.waybill_no
                AND DATE(ww.shipping_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -124 DAY))
                LEFT JOIN `datawarehouse_idexp.system_option` AS et ON ww.express_type = et.option_value AND et.type_option = 'expressType'
                -- LEFT OUTER JOIN waybill_to_return rr ON currenttab.waybill_no = rr.waybill_no
                                                
            WHERE DATE(currenttab.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -45 DAY))
            -- WHERE DATE(currenttab.record_time,'Asia/Jakarta') BETWEEN '2023-10-01' AND '2024-02-20' -->= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -45 DAY))
            -- AND DATE(currenttab.record_time,'Asia/Jakarta') < DATE(rr.return_confirm_record_time)
            AND currenttab.deleted = '0'

            AND currenttab.vehicle_tag_no IN (
"VF1130035449",
"VF1130034640",
"VA0630039536"
)

            ORDER BY record_time DESC
),

get_vm_awb AS (

SELECT *

FROM (

  SELECT

vehicle_tag_no,
bag_no,
waybill_no,
item_calculated_weight,
shipping_time,
pickup_branch_name,
sender_district_name,
sender_city_name,
sender_province_name,
recipient_district_name,
recipient_city_name,
recipient_province_name,
standard_shipping_fee,
express_type,
CONCAT(vehicle_tag_no," ","-"," ",waybill_no) vm_awb,

FROM scan_record_main
)
QUALIFY ROW_NUMBER() OVER(PARTITION BY vm_awb)=1
)

SELECT 

vehicle_tag_no,
bag_no,
waybill_no,
shipping_time,
pickup_branch_name,
sender_district_name,
sender_city_name,
sender_province_name,
recipient_district_name,
recipient_city_name,
recipient_province_name,
express_type,
COUNT(waybill_no) total_awb,
SUM(item_calculated_weight) item_calculated_weight,
SUM(standard_shipping_fee) standard_shipping_fee,

FROM get_vm_awb

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
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
  SUM(item_calculated_weight) item_calculated_weight,
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
  c.bag_no,
  c.waybill_no,
  -- sc.previous_branch_name,
  sc.operation_branch_name operation_branch_name_vm,
  -- sc.first_record_time record_time,
  -- sc.ops_type operation_type,
  a.max_sending_time_vm sending_time_vm,
  sc.next_location_name_3 arrival_branch_name_vm,
  b.max_arrival_time_vm arrival_time_vm,
  d.max_unloading__time_vm unloading_time_vm,
  -- c.total_awb,
  -- c.total_weight,
  -- a.max_sending_branch_vm,
c.shipping_time,
c.pickup_branch_name,
c.sender_district_name,
c.sender_city_name,
c.sender_province_name,
c.recipient_district_name,
c.recipient_city_name,
c.recipient_province_name,
c.standard_shipping_fee,
c.express_type,
c.item_calculated_weight,

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
    ),

scan_record_main AS (

  WITH dummy_sc_scan_record_urutan AS (

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
            -- MAX(currenttab.operation_branch_name) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time DESC) AS last_location,
            
            FROM
                `datawarehouse_idexp.dm_waybill_waybill_line` AS currenttab
                LEFT JOIN `datawarehouse_idexp.system_option` AS option ON currenttab.operation_type = option.option_value AND option.type_option = 'operationType'
                                                
            WHERE DATE(currenttab.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -45 DAY))
            -- WHERE DATE(currenttab.record_time,'Asia/Jakarta') BETWEEN '2024-01-01' AND '2024-02-20'
            AND currenttab.deleted = '0'
            -- AND currenttab.operation_type = '18'
            
            -- AND waybill_no IN (
                        -- ) --'IDE703620797395', 'IDM500837282987'
            -- AND bag_no IN ('BM1830016569')
            -- AND operation_branch_name IN ('MH JAKARTA')
            -- AND option.option_name IN ('Problem On Shipment scan','Delivery scan')
            -- AND currenttab.vehicle_tag_no IN (

            -- )

ORDER BY record_time DESC
            ))

  SELECT

  sc.waybill_no,
    sc.operation_type,
    sc.operation_branch_name,
    sc.record_time,
    sc.previous_branch_name,
    sc.next_location_name,

  FROM dummy_sc_scan_record_urutan sc
),


get_arrival_mh_ori AS (

  SELECT
  sc.waybill_no,
  MIN(sc.operation_type) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) scan_type_2,
  MIN(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) mh_ori_arrival,
  MIN(sc.record_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) mh_ori_arrival_time,
  MIN(sc.next_location_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) next_location_name,
  -- MIN(sc.next_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) next_scan_time_arr_fm,
  MIN(sc.previous_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) previous_branch_name_arr_fm,
  -- MIN(sc.previous_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) previous_scan_time_arr_fm,


  FROM scan_record_main sc

-- WHERE sc.record_time <= DATETIME(rr.return_confirm_record_time,'Asia/Jakarta')  
  WHERE sc.operation_type = 'Arrival scan'
  AND SUBSTR(sc.operation_branch_name,1,2) IN ('MH','DC')
  AND SUBSTR(sc.previous_branch_name,1,2) IN ('TH','VH','VT','PD','MH')

QUALIFY ROW_NUMBER() OVER(PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC)=1

),

get_sending_mh_ori AS (

  SELECT
  sc.waybill_no,
  MIN(sc.operation_type) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) scan_type_2,
  MIN(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) mh_ori_sending,
  MIN(sc.record_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) mh_ori_sending_time,
  MIN(sc.next_location_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) next_location_name,
  -- MIN(sc.next_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) next_scan_time_arr_fm,
  MIN(sc.previous_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) previous_branch_name_arr_fm,
  -- MIN(sc.previous_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) previous_scan_time_arr_fm,


  FROM scan_record_main sc
  -- LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON sc.waybill_no = rr.waybill_no
    -- AND sc.record_time <= DATETIME(rr.return_confirm_record_time,'Asia/Jakarta')
  
  WHERE sc.operation_type = 'Sending scan'
  AND SUBSTR(sc.operation_branch_name,1,2) IN ('MH','DC')
  -- AND SUBSTR(sc.previous_branch_name,1,2) IN ('TH','VH','VT','PD')

QUALIFY ROW_NUMBER() OVER(PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC)=1

),

get_arrival_mh_dest AS (

  SELECT
  sc.waybill_no,
  MAX(sc.operation_type) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) scan_type_2,
  MAX(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) mh_dest_arrival,
  MAX(sc.record_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) mh_dest_arrival_time,
  MAX(sc.next_location_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) next_location_mh_dest,
  -- MAX(sc.next_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) next_scan_time_arr_fm,
  MAX(sc.previous_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) previous_branch_name_arr_fm,
  -- MAX(sc.previous_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) previous_scan_time_arr_fm,


  FROM scan_record_main sc
  -- FROM `datawarehouse_idexp.waybill_waybill_line` sc
  -- LEFT JOIN `datawarehouse_idexp.system_option` AS option ON sc.operation_type = option.option_value AND option.type_option = 'operationType'
  -- LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON sc.waybill_no = rr.waybill_no
  
  -- WHERE DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -45 DAY))
  WHERE sc.operation_type = 'Arrival scan'
  AND SUBSTR(sc.operation_branch_name,1,2) IN ('MH','DC')
  -- AND SUBSTR(sc.next_location_name,1,2) IN ('TH','VH','VT','PD')
  AND SUBSTR(sc.previous_branch_name,1,2) IN ('MH','DC')
    -- AND DATETIME(sc.record_time,'Asia/Jakarta') < DATETIME(rr.return_confirm_record_time,'Asia/Jakarta')

QUALIFY ROW_NUMBER() OVER(PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC)=1

),

get_sending_mh_dest AS (

  SELECT
  sc.waybill_no,
  MAX(sc.operation_type) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) scan_type_3,
  MAX(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) mh_dest_name,
  MAX(sc.record_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) sending_time_mh_dest,
  MAX(sc.next_location_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) next_location_name,
  -- MAX(sc.next_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) next_scan_time_mh_dest,


  FROM scan_record_main sc
  -- FROM `datawarehouse_idexp.waybill_waybill_line` sc
  -- LEFT JOIN `datawarehouse_idexp.system_option` AS option ON sc.operation_type = option.option_value AND option.type_option = 'operationType'
  -- LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON sc.waybill_no = rr.waybill_no
  
  -- WHERE DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -45 DAY))
    -- AND DATETIME(sc.record_time,'Asia/Jakarta') < DATETIME(rr.return_confirm_record_time,'Asia/Jakarta')

  WHERE sc.operation_type = 'Sending scan'

  AND SUBSTR(sc.operation_branch_name,1,2) IN ('MH','DC')
  AND SUBSTR(sc.next_location_name,1,2) IN ('TH','VH','VT','PD')


QUALIFY ROW_NUMBER() OVER(PARTITION BY waybill_no ORDER BY record_time DESC)=1

),

next_sending_vm AS (

  SELECT * FROM (

  SELECT
  sc.waybill_no,
  MIN(sc.operation_type) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) scan_type_3,
  MIN(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) next_mh_sending_name,
  MIN(sc.record_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) next_sending_time_vm,
  MIN(sc.next_location_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) next_location_name,
  vm.arrival_branch_name_vm,
  arrival_time_vm
  -- MAX(sc.next_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) next_scan_time_mh_dest,


  FROM scan_record_main sc
  LEFT JOIN get_vm_record vm ON sc.waybill_no = vm.waybill_no
  -- FROM `datawarehouse_idexp.waybill_waybill_line` sc
  -- LEFT JOIN `datawarehouse_idexp.system_option` AS option ON sc.operation_type = option.option_value AND option.type_option = 'operationType'
  -- LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON sc.waybill_no = rr.waybill_no
  
  -- WHERE DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -45 DAY))
    -- AND DATETIME(sc.record_time,'Asia/Jakarta') < DATETIME(rr.return_confirm_record_time,'Asia/Jakarta')

  WHERE sc.operation_type = 'Sending scan'

  AND SUBSTR(sc.operation_branch_name,1,2) IN ('MH','DC')
  -- AND SUBSTR(sc.next_location_name,1,2) IN ('MH','DC')
AND record_time > arrival_time_vm

QUALIFY ROW_NUMBER() OVER(PARTITION BY waybill_no ORDER BY record_time ASC)=1
  )

  -- WHERE next_mh_sending_name = arrival_branch_name_vm
  -- WHERE next_sending_time_vm > arrival_time_vm
),

next_arrival_vm AS (

  SELECT * FROM (

  SELECT
  sc.waybill_no,
  MIN(sc.operation_type) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) scan_type_3,
  MIN(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) next_mh_arrival_name,
  MIN(sc.record_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) next_arrival_time_vm,
  MIN(sc.next_location_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) next_location_name,
  vm.arrival_branch_name_vm,
  arrival_time_vm
  -- MAX(sc.next_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) next_scan_time_mh_dest,


  FROM scan_record_main sc
  LEFT JOIN get_vm_record vm ON sc.waybill_no = vm.waybill_no
  -- FROM `datawarehouse_idexp.waybill_waybill_line` sc
  -- LEFT JOIN `datawarehouse_idexp.system_option` AS option ON sc.operation_type = option.option_value AND option.type_option = 'operationType'
  -- LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON sc.waybill_no = rr.waybill_no
  
  -- WHERE DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -45 DAY))
    -- AND DATETIME(sc.record_time,'Asia/Jakarta') < DATETIME(rr.return_confirm_record_time,'Asia/Jakarta')

  WHERE sc.operation_type = 'Arrival scan'

  AND SUBSTR(sc.operation_branch_name,1,2) IN ('MH','DC')
  -- AND SUBSTR(sc.next_location_name,1,2) IN ('MH','DC')
AND record_time > arrival_time_vm

QUALIFY ROW_NUMBER() OVER(PARTITION BY waybill_no ORDER BY record_time ASC)=1
  )

  -- WHERE next_mh_sending_name = arrival_branch_name_vm
  -- WHERE next_sending_time_vm > arrival_time_vm
),

root_next_arrival_vm AS (

  SELECT

  sc.waybill_no,
  vm.next_mh_arrival_name,
  vm.next_arrival_time_vm,

  FROM get_vm_record sc
  LEFT JOIN next_arrival_vm vm ON sc.waybill_no = vm.waybill_no
),

next_sending_vm_next AS (

  SELECT * FROM (

  SELECT
  sc.waybill_no,
  MIN(sc.operation_type) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) scan_type_3,
  MIN(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) next_mh_sending_name,
  MIN(sc.record_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) next_sending_time_vm,
  MIN(sc.next_location_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) next_location_name,
  vm.next_mh_arrival_name,
  vm.next_arrival_time_vm,
  -- MAX(sc.next_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) next_scan_time_mh_dest,


  FROM scan_record_main sc
  LEFT JOIN root_next_arrival_vm vm ON sc.waybill_no = vm.waybill_no
  -- FROM `datawarehouse_idexp.waybill_waybill_line` sc
  -- LEFT JOIN `datawarehouse_idexp.system_option` AS option ON sc.operation_type = option.option_value AND option.type_option = 'operationType'
  -- LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON sc.waybill_no = rr.waybill_no
  
  -- WHERE DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -45 DAY))
    -- AND DATETIME(sc.record_time,'Asia/Jakarta') < DATETIME(rr.return_confirm_record_time,'Asia/Jakarta')

  WHERE sc.operation_type = 'Sending scan'

  AND SUBSTR(sc.operation_branch_name,1,2) IN ('MH','DC')
  -- AND SUBSTR(sc.next_location_name,1,2) IN ('MH','DC')
AND record_time > next_arrival_time_vm

QUALIFY ROW_NUMBER() OVER(PARTITION BY waybill_no ORDER BY record_time ASC)=1
  )

  -- WHERE next_mh_sending_name = arrival_branch_name_vm
  -- WHERE next_sending_time_vm > arrival_time_vm
),

next_sending_vm_old AS (

  SELECT * FROM (

  SELECT
  sc.waybill_no,
  MAX(sc.operation_type) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) scan_type_3,
  MAX(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) next_mh_sending_name,
  MAX(sc.record_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) next_sending_time_vm,
  MAX(sc.next_location_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) next_location_name,
  vm.arrival_branch_name_vm,
  arrival_time_vm
  -- MAX(sc.next_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) next_scan_time_mh_dest,


  FROM scan_record_main sc
  LEFT JOIN get_vm_record vm ON sc.waybill_no = vm.waybill_no
  -- FROM `datawarehouse_idexp.waybill_waybill_line` sc
  -- LEFT JOIN `datawarehouse_idexp.system_option` AS option ON sc.operation_type = option.option_value AND option.type_option = 'operationType'
  -- LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON sc.waybill_no = rr.waybill_no
  
  -- WHERE DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -45 DAY))
    -- AND DATETIME(sc.record_time,'Asia/Jakarta') < DATETIME(rr.return_confirm_record_time,'Asia/Jakarta')

  WHERE sc.operation_type = 'Sending scan'

  AND SUBSTR(sc.operation_branch_name,1,2) IN ('MH','DC')
  -- AND SUBSTR(sc.next_location_name,1,2) IN ('MH','DC')


QUALIFY ROW_NUMBER() OVER(PARTITION BY waybill_no ORDER BY record_time DESC)=1
  )

  -- WHERE next_mh_sending_name = arrival_branch_name_vm
  WHERE next_sending_time_vm > arrival_time_vm
),

get_arrival_th_dest AS (

  SELECT
  sc.waybill_no,
  MAX(sc.operation_type) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) scan_type_2,
  MAX(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) th_dest_arrival,
  MAX(sc.record_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) th_dest_arrival_time,
  MAX(sc.next_location_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) next_location_mh_dest,
  -- MAX(sc.next_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) next_scan_time_arr_fm,
  MAX(sc.previous_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) previous_branch_name_arr_fm,
  -- MAX(sc.previous_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) previous_scan_time_arr_fm,


  -- FROM `datawarehouse_idexp.waybill_waybill_line` sc
  FROM scan_record_main sc
  -- LEFT JOIN `datawarehouse_idexp.system_option` AS option ON sc.operation_type = option.option_value AND option.type_option = 'operationType'
  -- LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON sc.waybill_no = rr.waybill_no
  
  -- WHERE DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -45 DAY))
    -- AND sc.record_time < DATETIME(rr.return_confirm_record_time,'Asia/Jakarta')

  WHERE sc.operation_type = 'Arrival scan'
  AND SUBSTR(sc.operation_branch_name,1,2) IN ('TH','VH','VT','PD')
  -- AND SUBSTR(sc.next_location_name,1,2) IN ('TH','VH','VT','PD')
  AND SUBSTR(sc.previous_branch_name,1,2) IN ('MH','DC')

QUALIFY ROW_NUMBER() OVER(PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC)=1

),

get_unloading_th_dest AS (

  SELECT
  sc.waybill_no,
  MAX(sc.operation_type) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) scan_type_2,
  MAX(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) th_dest_unloading,
  MAX(sc.record_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) th_dest_unloading_time,
  MAX(sc.next_location_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) next_location_mh_dest,
  -- MAX(sc.next_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) next_scan_time_arr_fm,
  MAX(sc.previous_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) previous_branch_name_arr_fm,
  -- MAX(sc.previous_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) previous_scan_time_arr_fm,


  -- FROM `datawarehouse_idexp.waybill_waybill_line` sc
  FROM scan_record_main sc
  -- LEFT JOIN `datawarehouse_idexp.system_option` AS option ON sc.operation_type = option.option_value AND option.type_option = 'operationType'
  -- LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON sc.waybill_no = rr.waybill_no
  
  -- WHERE DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -45 DAY))
    -- AND sc.record_time < DATETIME(rr.return_confirm_record_time,'Asia/Jakarta')

  WHERE sc.operation_type = 'Unloading scan'
  AND SUBSTR(sc.operation_branch_name,1,2) IN ('TH','VH','VT','PD')
  -- AND SUBSTR(sc.next_location_name,1,2) IN ('TH','VH','VT','PD')
  -- AND SUBSTR(sc.previous_branch_name,1,2) IN ('MH','DC')

QUALIFY ROW_NUMBER() OVER(PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC)=1

),

gabung_all_2 AS (

    SELECT

    vm.vehicle_tag_no,
    vm.bag_no,
    vm.waybill_no,

vm.express_type,
vm.pickup_branch_name origin_branch,
vm.sender_district_name origin_district,
vm.sender_city_name origin_city,
vm.sender_province_name origin_province,
vm.shipping_time pickup_time,

  a.mh_ori_arrival mh_origin_name,
  a.mh_ori_arrival_time,
  b.mh_ori_sending_time,
  
vm.operation_branch_name_vm,
vm.sending_time_vm,
vm.arrival_branch_name_vm,
vm.arrival_time_vm,
-- nx.next_mh_sending_name mh_sending_vm,
nx.next_sending_time_vm mh_sending_time_vm,

g.next_mh_arrival_name,
g.next_arrival_time_vm, 
-- h.next_mh_sending_name,
h.next_sending_time_vm,

  e.th_dest_arrival_time,
  f.th_dest_unloading_time,
  cASE
      WHEN e.th_dest_arrival IS NULL THEN f.th_dest_unloading
      ELSE e.th_dest_arrival
      END AS th_dest_name,
  
  vm.recipient_district_name dest_district,
  vm.recipient_city_name dest_city,
  vm.recipient_province_name dest_province,
  vm.standard_shipping_fee,
  vm.item_calculated_weight,


    -- FROM get_arrival_vm vm
    -- FROM gabung_all vm
    FROM get_vm_record vm
    -- LEFT JOIN get_waybill_data ww ON vm.waybill_no = ww.waybill_no
    -- LEFT JOIN get_sending_vm sd ON vm.waybill_no = sd.waybill_no
    -- LEFT JOIN get_next_arrival_vm n1 ON vm.waybill_no = n1.waybill_no
    -- LEFT JOIN get_next_sending_vm n2 ON vm.waybill_no = n2.waybill_no
    LEFT JOIN get_arrival_mh_ori a ON vm.waybill_no = a.waybill_no
  LEFT JOIN get_sending_mh_ori b ON vm.waybill_no = b.waybill_no
  LEFT JOIN get_arrival_mh_dest c ON vm.waybill_no = c.waybill_no
  LEFT JOIN get_sending_mh_dest d ON vm.waybill_no = d.waybill_no
  LEFT JOIN get_arrival_th_dest e ON vm.waybill_no = e.waybill_no
  LEFT JOIN get_unloading_th_dest f ON vm.waybill_no = f.waybill_no
  LEFT JOIN next_sending_vm nx ON vm.waybill_no = nx.waybill_no
  LEFT JOIN next_arrival_vm g ON vm.waybill_no = g.waybill_no
  LEFT JOIN next_sending_vm_next h ON vm.waybill_no = h.waybill_no
    -- WHERE sd.next_location_name IN ('MH SURABAYA','MH DENPASAR')

)

-- SELECT * FROM get_vm_bm_awb
-- SELECT * FROM get_arrival_vm

SELECT 
-- * 
vehicle_tag_no,
waybill_no,
origin_city,
dest_city,
item_calculated_weight,
standard_shipping_fee,
express_type,


FROM gabung_all_2

ORDER BY vehicle_tag_no ASC
-- SELECT * FROM get_vm_record

