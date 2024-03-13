WITH
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
                                                
            -- WHERE DATE(currenttab.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -65 DAY))
            WHERE DATE(currenttab.record_time,'Asia/Jakarta') BETWEEN '2024-01-01' AND '2024-02-20' 
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

ORDER BY record_time DESC

),

waybill_data AS (

  SELECT

  ww.waybill_no,
  ww.recipient_city_name,
  ww.recipient_province_name,
  kw.kanwil_name kanwil_area,

  FROM `datawarehouse_idexp.waybill_waybill` ww
  LEFT JOIN `datamart_idexp.mapping_kanwil_area` kw ON ww.recipient_province_name = kw.province_name

 WHERE DATE(ww.shipping_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -186 DAY)) --BETWEEN '2024-01-01' AND '2024-01-31'
  AND ww.void_flag = '0' AND ww.deleted= '0' 

QUALIFY ROW_NUMBER() OVER (PARTITION BY ww.waybill_no ORDER BY ww.update_time DESC)=1
),

get_unpacking AS (

  SELECT
  sc.waybill_no,
  MIN(sc.operation_type) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) scan_type_1,
  MIN(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) unpacking_branch,
  MIN(sc.record_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) unpacking_time,
  MIN(sc.next_location_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) next_location_name,
  -- MIN(sc.next_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) next_scan_time_sending_fm,


  FROM scan_record_main sc
--   LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON sc.waybill_no = rr.waybill_no

  WHERE sc.operation_type = 'Unpacking scan'
  AND SUBSTR(operation_branch_name,1,2) IN ('TH','VH','VT','PD')
--   AND sc.record_time <= DATETIME(rr.return_confirm_record_time,'Asia/Jakarta')
  -- AND SUBSTR(sc.next_location_name,1,2) IN ('MH','DC')


QUALIFY ROW_NUMBER() OVER(PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC)=1

),

get_unloading AS (

  SELECT
  sc.waybill_no,
  MIN(sc.operation_type) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) scan_type_1,
  MIN(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) unloadng_branch,
  MIN(sc.record_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) unloading_time,
  MIN(sc.next_location_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) next_location_name,
  -- MIN(sc.next_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) next_scan_time_sending_fm,


  FROM scan_record_main sc
--   LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON sc.waybill_no = rr.waybill_no

  WHERE sc.operation_type = 'Unloading scan'
  AND SUBSTR(operation_branch_name,1,2) IN ('TH','VH','VT','PD')

QUALIFY ROW_NUMBER() OVER(PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC)=1

),

get_delivery_attempt as(

  SELECT
ps.waybill_no,
ps.deliv_attempt_1,
ps.deliv_attempt_2,
ps.deliv_attempt_3,

FROM (
  SELECT
        waybill_no,
        MAX(IF(id = 1, DATETIME(record_time), NULL)) AS deliv_attempt_1,
        MAX(IF(id = 2, DATETIME(record_time), NULL)) AS deliv_attempt_2,
        MAX(IF(id = 3, DATETIME(record_time), NULL)) AS deliv_attempt_3,

        FROM (
              SELECT sc.waybill_no, 
              sc.record_time, 
              sc.operation_type,
                        
              RANK() OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC ) AS id
              -- FROM `datawarehouse_idexp.waybill_waybill_line` sc
              FROM scan_record_main sc

              -- WHERE DATE(sc.record_time) >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -45 DAY))
            WHERE sc.operation_type IN ('Delivery scan')
        ) 

        GROUP BY 1 
) ps
QUALIFY ROW_NUMBER() OVER (PARTITION BY ps.waybill_no)=1
),

get_delivery_scan AS (

  SELECT
  sc.waybill_no,
  MIN(sc.operation_type) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) scan_type_1,
  MIN(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) delivery_branch,
  MIN(sc.record_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) delivery_time,
  MIN(sc.next_location_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) next_location_name,
  -- MIN(sc.next_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) next_scan_time_sending_fm,


  FROM scan_record_main sc
--   LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON sc.waybill_no = rr.waybill_no

  WHERE sc.operation_type = 'Delivery scan'
  AND SUBSTR(operation_branch_name,1,2) IN ('TH','VH','VT','PD')

QUALIFY ROW_NUMBER() OVER(PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC)=1

),

gabung_1 AS (

  SELECT

  waybill_no,
  recipient_city_name,
  recipient_province_name,
  kanwil_area,
  CASE
      WHEN unpacking_branch IS NULL THEN unloadng_branch
      ELSE unpacking_branch END AS unpacking_branch_name,
  CASE
      WHEN unpacking_time IS NULL THEN unloading_time
      ELSE unpacking_time END AS unpacking_time,
  
  delivery_time,

FROM (

  SELECT

  ww.waybill_no,
  ww.recipient_city_name,
  ww.recipient_province_name,
  ww.kanwil_area,
  ul.unloadng_branch,
  unloading_time,
  up.unpacking_branch,
  up.unpacking_time,
  -- dl.delivery_branch,
  -- dl.deliv_attempt_1 delivery_time,
  dl.delivery_time


  FROM waybill_data ww
  LEFT JOIN get_unpacking up ON ww.waybill_no = up.waybill_no
  LEFT JOIN get_unloading ul ON ww.waybill_no = ul.waybill_no
  LEFT JOIN get_delivery_scan dl ON ww.waybill_no = dl.waybill_no
  -- LEFT JOIN get_delivery_attempt dl ON ww.waybill_no = dl.waybill_no
)
),

get_leadtime AS (

    SELECT

    month_report,
    waybill_no,
    unpacking_branch_name,
    -- recipient_city_name,
    -- recipient_province_name,
    -- kanwil_area,
    city_name,
    province_name,
    kanwil_name,
    leadtime_unpack_to_deliv,
    -- AVG(leadtime_unpack_to_deliv) leadtime_unpack_to_deliv,

    FROM (

      SELECT

  ww.waybill_no,
  ww.recipient_city_name,
  ww.recipient_province_name,
  ww.kanwil_area,
  ww.unpacking_branch_name,
  FORMAT_DATE("%b %Y", DATE(ww.unpacking_time)) month_report,
  ww.unpacking_time,
  ww.delivery_time,
  DATETIME_DIFF(ww.delivery_time, ww.unpacking_time, DAY) leadtime_unpack_to_deliv,
  kw.city_name,
  kw.province_name,
  kw.kanwil_name,

  FROM gabung_1 ww
  LEFT JOIN `datamart_idexp.masterdata_facility_to_kanwil` kw ON ww.unpacking_branch_name = kw.branch_name
  WHERE DATE(unpacking_time) BETWEEN '2024-01-01' AND '2024-02-20'

)
-- GROUP BY 1,2,3,4,5
),

get_leadtime_range AS (

  SELECT 
  *,
  CASE
    WHEN leadtime_unpack_to_deliv BETWEEN 0 AND 0.99 THEN 1 
    WHEN leadtime_unpack_to_deliv <0 THEN 1
    ELSE 0
    END AS attempt_0,
CASE
    WHEN leadtime_unpack_to_deliv BETWEEN 1.00 AND 1.99 THEN 1 ELSE 0
    END AS attempt_1,
CASE
    WHEN leadtime_unpack_to_deliv BETWEEN 2 AND 2.99 THEN 1 ELSE 0
    END AS attempt_2,
CASE
    WHEN leadtime_unpack_to_deliv BETWEEN 3 AND 3.99 THEN 1 ELSE 0
    END AS attempt_3,    
CASE
    WHEN leadtime_unpack_to_deliv BETWEEN 4 AND 4.9 THEN 1 ELSE 0
    END AS attempt_4,
CASE
    WHEN leadtime_unpack_to_deliv BETWEEN 5 AND 5.99 THEN 1 ELSE 0
    END AS attempt_5,
CASE
    WHEN leadtime_unpack_to_deliv BETWEEN 6 AND 6.99 THEN 1 ELSE 0
    END AS attempt_6,
CASE
    WHEN leadtime_unpack_to_deliv BETWEEN 7 AND 7.99 THEN 1 ELSE 0
    END AS attempt_7,
CASE
    WHEN leadtime_unpack_to_deliv BETWEEN 8 AND 8.9 THEN 1 ELSE 0
    END AS attempt_8,
CASE
    WHEN leadtime_unpack_to_deliv BETWEEN 9 AND 9.99 THEN 1 ELSE 0
    END AS attempt_9,
CASE
    WHEN leadtime_unpack_to_deliv BETWEEN 10 AND 10.99 THEN 1 ELSE 0
    END AS attempt_10,
CASE
    WHEN leadtime_unpack_to_deliv > 10 THEN 1 ELSE 0
    END AS attempt_10_more,

  FROM get_leadtime
),

count_leadtime_unpack_to_deliv AS (

  SELECT

    month_report,
    unpacking_branch_name,
    city_name,
    province_name,
    kanwil_name,
SUM(attempt_0) lt_0_day,
SUM(attempt_1) lt_1_day,
SUM(attempt_2) lt_2_day,
SUM(attempt_3) lt_3_day,
SUM(attempt_4) lt_4_day,
SUM(attempt_5) lt_5_day,
SUM(attempt_6) lt_6_day,
SUM(attempt_7) lt_7_day,
SUM(attempt_8) lt_8_day,
SUM(attempt_9) lt_9_day,
SUM(attempt_10) lt_10_day,
SUM(attempt_10_more) lt_10_day_and_more,

FROM get_leadtime_range
GROUP BY 1,2,3,4,5
)

-- SELECT * FROM get_leadtime_range
SELECT * FROM count_leadtime_unpack_to_deliv
-- SELECT * FROM gabung_1



