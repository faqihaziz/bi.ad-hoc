WITH
get_vm_awb AS (

  WITH root_vehicle_tag AS (

SELECT
  sc.vehicle_tag_no,
  sc.bag_no,
  sc.waybill_no,
  CONCAT(sc.vehicle_tag_no,'-',sc.waybill_no) vm_awb,
  DATETIME(sc.record_time, 'Asia/Jakarta') AS record_time,
  sc.operation_branch_name mh_operation_vm_name,
  sc.next_location_name next_loc_vm,
  rd1.option_name vm_operation_type,
              LAG(sc.operation_branch_name,1) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time) AS previous_branch_name,
            LAG(DATETIME(sc.record_time,'Asia/Jakarta'),1) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time) AS previous_scan_time,
            -- LAG(currenttab.operation_branch_name,2) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS previous_branch_name_2,
            LEAD(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time) AS next_location_name,
            LEAD(DATETIME(sc.record_time,'Asia/Jakarta')) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time) AS next_scan_time,
            LEAD(option.option_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time) AS next_scan_type,


FROM `datawarehouse_idexp.waybill_waybill_line` sc
  LEFT JOIN `datawarehouse_idexp.system_option` rd1 ON sc.operation_type = rd1.option_value AND rd1.type_option = 'operationType'
  LEFT JOIN `datawarehouse_idexp.system_option` option ON sc.operation_type = option.option_value AND option.type_option = 'operationType'

WHERE DATE(sc.record_time, 'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -35 DAY))
-- WHERE DATE(sc.record_time, 'Asia/Jakarta') BETWEEN '2024-01-01' AND '2024-01-31'

AND sc.vehicle_tag_no NOT IN ('') 
AND sc.vehicle_tag_no IS NOT NULL
-- AND sc.operation_branch_name IN ('MH JEMBER','MH DENPASAR','MH SURABAYA')
-- AND sc.next_location_name IN ('MH SURABAYA','MH DENPASAR')
AND sc.vehicle_tag_no IN (
-- AND sc.waybill_no IN (
"VF0230000515",
"VF0230000516",
"VF0230000520",
"VF0230000519",
"VF0230000518",
"VF0230000517",
"VF0230000529",
"VF0230000530",
"VF0230000521",
"VF0230000522",
"VF0230000528",
"VF0230000527",
"VF0230000526",
"VF0230000525",
"VF0230000523",
"VF0230000524",
"VF0230000620",
"VF0230000619",
"VF0230000718",
"VF0230000717",
"VF0230000498",
"VF0230000499",
"VF0230000497",
"VF0230000496",
"VF0230000492",
"VF0230000491",
"VF0230000490",
"VF0230000489"
)

-- QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.waybill_no)=1
-- QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.vehicle_tag_no)=1
),

dummy_sc_get_vm_bm_awb AS (

SELECT

*

FROM root_vehicle_tag vm

-- WHERE vehicle_tag_no IS NOT NULL
QUALIFY ROW_NUMBER() OVER(PARTITION BY vm_awb)=1
)

  SELECT

  sc.vehicle_tag_no,
  sc.waybill_no,
  DATETIME(ww.shipping_time,'Asia/Jakarta') pickup_time,

  FROM dummy_sc_get_vm_bm_awb sc
  LEFT JOIN `datawarehouse_idexp.waybill_waybill` ww ON sc.waybill_no = ww.waybill_no
),

scan_record_main AS (

WITH
scan_record_main AS (

SELECT
            currenttab.vehicle_tag_no,
            currenttab.waybill_no,
            currenttab.bag_no,
            option.option_name AS operation_type,
            currenttab.operation_branch_name AS operation_branch_name,
            DATETIME(currenttab.record_time,'Asia/Jakarta') AS record_time,
            LAG(currenttab.operation_branch_name,1) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS previous_branch_name,
            LEAD(currenttab.operation_branch_name) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_location_name,
            LEAD(currenttab.operation_branch_name,1) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_location_name_2,
            LEAD(currenttab.operation_branch_name,2) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_location_name_3,
            LEAD(currenttab.operation_branch_name,3) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_location_name_4,
            LEAD(currenttab.operation_branch_name,4) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_location_name_5,
            
            -- FROM `dev_idexp.dummy_count_awb_and_weight_from_vm` sc
            -- LEFT JOIN 
            FROM
                `datawarehouse_idexp.dm_waybill_waybill_line` AS currenttab --ON sc.vehicle_tag_no = currenttab.vehicle_tag_no

                -- AND DATE(currenttab.record_time,'Asia/Jakarta') BETWEEN '2023-10-01' AND '2024-02-20' 

                LEFT JOIN `datawarehouse_idexp.system_option` AS option ON currenttab.operation_type = option.option_value AND option.type_option = 'operationType'
                -- LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON currenttab.waybill_no = rr.waybill_no
                -- LEFT OUTER JOIN waybill_to_return rr ON currenttab.waybill_no = rr.waybill_no
                                                
            WHERE DATE(currenttab.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -35 DAY))
            -- WHERE DATE(currenttab.record_time,'Asia/Jakarta') BETWEEN '2024-01-01' AND '2024-02-20' 
            -- AND DATE(currenttab.record_time,'Asia/Jakarta') < DATE(rr.return_confirm_record_time)
            AND currenttab.deleted = '0'

            -- AND currenttab.vehicle_tag_no IN (
               -- AND currenttab.waybill_no IN (
-- )

            ORDER BY record_time DESC
),

scan_record_fwd AS (

    SELECT 
    
    sc.waybill_no,
    sc.vehicle_tag_no,
    sc.bag_no,
    sc.operation_type,
    sc.operation_branch_name,
    sc.record_time,
    sc.previous_branch_name,
    sc.next_location_name,
    sc.next_location_name_2,
    sc.next_location_name_3,
    sc.next_location_name_4,
    sc.next_location_name_5,

    
    FROM scan_record_main sc
    LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON sc.waybill_no = rr.waybill_no
    -- WHERE DATE(record_time) <= DATE(rr.return_confirm_record_time)
    WHERE rr.return_confirm_record_time IS NULL
),

scan_record_return AS (

    SELECT 
    
    sc.waybill_no,
    sc.vehicle_tag_no,
    sc.bag_no,
    sc.operation_type,
    sc.operation_branch_name,
    sc.record_time,
    sc.previous_branch_name,
    sc.next_location_name,
    sc.next_location_name_2,
    sc.next_location_name_3,
    sc.next_location_name_4,
    sc.next_location_name_5,

    
    FROM scan_record_main sc
    LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON sc.waybill_no = rr.waybill_no
        
    WHERE DATE(record_time) <= DATE(rr.return_confirm_record_time)
    AND rr.return_confirm_record_time IS NOT NULL

),

join_scan_record AS (

SELECT * FROM (

SELECT * FROM scan_record_fwd UNION ALL
SELECT * FROM scan_record_return
)
-- QUALIFY ROW_NUMBER() OVER(PARTITION BY record_time)=1
)

-- SELECT * FROM join_scan_record
SELECT * FROM scan_record_main
-- WHERE vehicle_tag_no IS NOT NULL
-- WHERE vehicle_tag_no IN (

-- )

ORDER BY record_time DESC

),

get_arrival_fm AS (

  SELECT
  sc.waybill_no,
  MIN(sc.operation_type) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) scan_type_2,
  MIN(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) mh_arrival,
  MIN(sc.record_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) mh_arrival_time,
  MIN(sc.next_location_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) next_location_name,
  -- MIN(sc.next_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) next_scan_time_arr_fm,
  MIN(sc.previous_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) previous_branch_name_arr_fm,
  -- MIN(sc.previous_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) previous_scan_time_arr_fm,


  FROM scan_record_main sc
  -- FROM get_vm_awb vm
  -- LEFT JOIN scan_record_main sc ON vm.waybill_no = sc.waybill_no
  LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON sc.waybill_no = rr.waybill_no
    -- AND sc.record_time <= DATETIME(rr.return_confirm_record_time,'Asia/Jakarta')
  
  WHERE sc.operation_type = 'Arrival scan'
  AND SUBSTR(sc.operation_branch_name,1,2) IN ('MH','DC')
  AND SUBSTR(sc.previous_branch_name,1,2) IN ('TH','VH','VT','PD','MH')

QUALIFY ROW_NUMBER() OVER(PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC)=1

),

get_arrival_mh_dest AS (

  SELECT
  sc.waybill_no,
  MAX(sc.operation_type) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) scan_type_3,
  MAX(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) mh_arrival_dest,
  MAX(sc.record_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) mh_arrival_dest_time,
  MAX(sc.next_location_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) next_location_name,
  MAX(sc.previous_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) previous_branch_name,


  FROM scan_record_main sc
  LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON sc.waybill_no = rr.waybill_no
    AND sc.record_time <= DATETIME(rr.return_confirm_record_time,'Asia/Jakarta')

  WHERE sc.operation_type = 'Arrival scan'

  AND SUBSTR(sc.operation_branch_name,1,2) IN ('MH','DC')
  -- AND SUBSTR(sc.previous_branch_name,1,2) IN ('MH','DC')
  -- AND SUBSTR(sc.previous_branch_name,1,2) IN ('TH','VH','VT','PD')

QUALIFY ROW_NUMBER() OVER(PARTITION BY waybill_no ORDER BY record_time DESC)=1

),

get_arrival_th_dest AS (

  SELECT
  sc.waybill_no,
  MAX(sc.operation_type) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) scan_type_3,
  MAX(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) th_arrival_dest,
  MAX(sc.record_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) th_arrival_dest_time,
  MAX(sc.next_location_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) next_location_name,
  MAX(sc.previous_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) previous_branch_name,


  FROM scan_record_main sc
  LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON sc.waybill_no = rr.waybill_no
    AND sc.record_time <= DATETIME(rr.return_confirm_record_time,'Asia/Jakarta')

  WHERE sc.operation_type = 'Arrival scan'

  AND SUBSTR(sc.operation_branch_name,1,2) IN ('TH','VH','VT','PD')
  AND SUBSTR(sc.previous_branch_name,1,2) IN ('MH','DC')
  -- AND SUBSTR(sc.operation_branch_name,1,2) IN ('MH','DC')

QUALIFY ROW_NUMBER() OVER(PARTITION BY waybill_no ORDER BY record_time DESC)=1

),

arrival_dest AS (
  
  SELECT 
b.waybill_no,
b.waybill_source,
b.arrival_time arrival_time_dest,
-- b.arrival_time_dest
b.th_arrival,
b.pod_time,


-- FROM `dev_idexp.temp_table_tokopedia_arrival_dest` b
FROM `datamart_idexp.dashboard_productivity_lm` b
),

gabung_all AS (

  SELECT

  vm.*,
  fm.mh_arrival mh_ori_name,
  fm.mh_arrival_time mh_ori_arrival_time,
  sd.mh_arrival_dest,
  sd.mh_arrival_dest_time,
  -- lm.arrival_time_dest,
  -- lm.th_arrival,
  a.th_arrival_dest,
  a.th_arrival_dest_time,



  FROM get_vm_awb vm
  LEFT JOIN get_arrival_fm fm ON vm.waybill_no = fm.waybill_no
  LEFT JOIN get_arrival_mh_dest sd ON vm.waybill_no = sd.waybill_no
  LEFT JOIN arrival_dest lm ON vm.waybill_no = lm.waybill_no
  LEFT JOIN get_arrival_th_dest a ON vm.waybill_no = a.waybill_no

),

gabung_2 AS (

  SELECT 
  *,
  DATETIME_DIFF(mh_ori_arrival_time, pickup_time, SECOND) AS leadtime_pickup_to_mh_ori_arrival,
  DATETIME_DIFF(mh_arrival_dest_time, pickup_time, SECOND) AS leadtime_pickup_to_mh_dest_arrival,
  DATETIME_DIFF(th_arrival_dest_time, pickup_time, SECOND) AS leadtime_pickup_to_th_dest_arrival,

  FROM gabung_all
)


-- SELECT * FROM get_vm_awb
-- SELECT * FROM gabung_all
SELECT * FROM gabung_2

-- WHERE waybill_no IN ('IDE702606169382')

