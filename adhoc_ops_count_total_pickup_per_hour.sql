
WITH waybill_data AS (
SELECT

  ww.waybill_no,
  FORMAT_DATE("%b %Y", DATE(ww.shipping_time,'Asia/Jakarta')) AS month_pickup,
  DATETIME(ww.shipping_time,'Asia/Jakarta') shipping_time,
  CAST(ww.item_calculated_weight AS NUMERIC) item_calculated_weight,
  et.option_name express_type,
  CASE
      WHEN ww.express_type IN ('00') THEN "Reguler"
      WHEN ww.express_type IN ('03') THEN "Lite"
      WHEN ww.express_type IN ('06','20') THEN "Cargo"
      END AS express_type_category,

  ww.sender_district_name,
  ww.pickup_branch_name,
  ww.sender_city_name,
  ww.sender_province_name,
  ww.vip_customer_name,

  ww.recipient_district_name,
  ww.recipient_city_name,
  ww.recipient_province_name,
  ww.standard_shipping_fee,

  DATETIME(ww.pod_record_time,'Asia/Jakarta') pod_record_time,
  DATETIME(ww.update_time,'Asia/Jakarta') update_time,
  ww.pod_branch_name,
  ww.delivery_branch_name,
  ww.return_flag,
  -- CASE
  --     WHEN ww.pod_record_time IS NOT NULL THEN ww.pod_branch_name
  --     WHEN ww.pod_record_time IS NULL AND ww.return_flag = '0' THEN ww.delivery_branch_name
  --     WHEN ww.pod_record_time IS NULL AND ww.return_flag = '0' AND ww.delivery_branch_name IS NULL THEN th.branch_name
  --     ELSE th.branch_name
  --     END AS th_destination,
  kw.kanwil_name,
  ww.recipient_district_id,
  CASE 
    WHEN ww.cod_amount > 0 THEN "COD"
    WHEN ww.cod_amount <= 0 THEN "Non-COD"
    END AS cod_type,

  sr.option_name waybill_source,

    CONCAT (ww.recipient_province_name," ","-"," ",ww.recipient_city_name," ","-"," ",ww.recipient_district_name) concat_dest_prov_city_district,

  FROM `datawarehouse_idexp.waybill_waybill` ww
  LEFT JOIN `datawarehouse_idexp.system_option` AS et ON ww.express_type = et.option_value AND et.type_option = 'expressType'
  LEFT JOIN `datawarehouse_idexp.system_option` AS sr ON ww.waybill_source = sr.option_value AND sr.type_option = 'waybillSource'
  LEFT JOIN `datamart_idexp.masterdata_facility_to_kanwil` kw ON ww.recipient_province_name = kw.province_name
  LEFT JOIN `dev_idexp.masterdata_branch_coverage_th` th ON ww.recipient_district_id = th.district_id

  -- WHERE DATE(ww.shipping_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY))
  WHERE DATE(ww.shipping_time,'Asia/Jakarta') BETWEEN '2024-05-06' AND '2024-05-12'
  AND ww.void_flag = '0' AND ww.deleted = '0'
  -- AND sr.option_name IN ('BukaSend','pt buka usaha indonesia')
  -- AND ww.vip_customer_name IN ('universitasterbuka01p')

  QUALIFY ROW_NUMBER() OVER(PARTITION BY ww.waybill_no ORDER BY ww.update_time DESC)=1
),

scan_record_main AS (
  SELECT *

FROM (

SELECT
            currenttab.waybill_no,
            option.option_name AS operation_type,
            currenttab.operation_branch_name AS operation_branch_name,
            DATETIME(currenttab.record_time,'Asia/Jakarta') AS record_time,
            LAG(currenttab.operation_branch_name,1) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS previous_branch_name,
            LAG(DATETIME(currenttab.record_time,'Asia/Jakarta'),1) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS previous_scan_time,
            -- LAG(currenttab.operation_branch_name,2) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS previous_branch_name_2,
            LEAD(currenttab.operation_branch_name) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_location_name,
            
            FROM
                `datawarehouse_idexp.dm_waybill_waybill_line` AS currenttab
                LEFT JOIN `datawarehouse_idexp.system_option` AS option ON currenttab.operation_type = option.option_value AND option.type_option = 'operationType'
                                                
            WHERE DATE(currenttab.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -35 DAY))
            -- WHERE DATE(currenttab.record_time,'Asia/Jakarta') BETWEEN '2024-01-01' AND '2024-03-15'
            AND currenttab.deleted = '0'

ORDER BY record_time DESC
            )
),

get_sending_scan_th AS (

  SELECT

  sc.waybill_no,
  MIN(sc.operation_type) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) scan_type_sending,
  MIN(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) th_sending_name,
  MIN(sc.record_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) th_sending_time,
  -- MIN(sc.next_location_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) next_location_branch,
  next_location_name next_location_branch,


  FROM scan_record_main sc
  WHERE sc.operation_type = 'Sending scan'
  AND SUBSTR(sc.operation_branch_name,1,2) IN ('TH')
  AND SUBSTR(sc.next_location_name,1,2) IN ('MH','DC')

QUALIFY ROW_NUMBER() OVER(PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC)=1
),

join_pickup_to_mh AS (
SELECT

ww.waybill_no,
ww.shipping_time,
ww.pickup_branch_name,
-- SUM(standard_shipping_fee) gross_revenue,
-- COUNT(waybill_no) total_awb,
sc.th_sending_time,
sc.th_sending_name,
sc.next_location_branch,

FROM waybill_data ww
LEFT JOIN get_sending_scan_th sc ON ww.waybill_no = sc.waybill_no

-- GROUP BY 1,2,3
),

count_waybill_pickup_hour AS (
  SELECT 
  
    pickup_branch_name,
    mh_afiliate,
    SUM(hour_0) hour_0,
    SUM(hour_1) hour_1,
    SUM(hour_2) hour_2,
    SUM(hour_3) hour_3,
    SUM(hour_4) hour_4,
    SUM(hour_5) hour_5,
    SUM(hour_6) hour_6,
    SUM(hour_7) hour_7,
    SUM(hour_8) hour_8,
    SUM(hour_9) hour_9,
    SUM(hour_10) hour_10,
    SUM(hour_11) hour_11,
    SUM(hour_12) hour_12,
    SUM(hour_13) hour_13,

    SUM(hour_14) hour_14,
    SUM(hour_15) hour_15,
    SUM(hour_16) hour_16,
    SUM(hour_17) hour_17,
    SUM(hour_18) hour_18,
    SUM(hour_19) hour_19,
    SUM(hour_20) hour_20,
    SUM(hour_21) hour_21,
    SUM(hour_22) hour_22,
    SUM(hour_23) hour_23,
  
  FROM (

    SELECT

    pickup_branch_name,
    next_location_branch mh_afiliate,
    CASE
        WHEN TIME(shipping_time) BETWEEN '00:00:00' AND '00:59:59' THEN 1 ELSE 0 END AS hour_0,
    CASE
        WHEN TIME(shipping_time) BETWEEN '01:00:00' AND '01:59:59' THEN 1 ELSE 0 END AS hour_1,
    CASE
        WHEN TIME(shipping_time) BETWEEN '02:00:00' AND '02:59:59' THEN 1 ELSE 0 END AS hour_2,
    CASE
        WHEN TIME(shipping_time) BETWEEN '03:00:00' AND '03:59:59' THEN 1 ELSE 0 END AS hour_3,
    CASE
        WHEN TIME(shipping_time) BETWEEN '04:00:00' AND '04:59:59' THEN 1 ELSE 0 END AS hour_4,
    CASE
        WHEN TIME(shipping_time) BETWEEN '05:00:00' AND '05:59:59' THEN 1 ELSE 0 END AS hour_5,
    CASE
        WHEN TIME(shipping_time) BETWEEN '06:00:00' AND '06:59:59' THEN 1 ELSE 0 END AS hour_6,
    CASE
        WHEN TIME(shipping_time) BETWEEN '07:00:00' AND '07:59:59' THEN 1 ELSE 0 END AS hour_7,
    CASE
        WHEN TIME(shipping_time) BETWEEN '08:00:00' AND '08:59:59' THEN 1 ELSE 0 END AS hour_8,
    CASE
        WHEN TIME(shipping_time) BETWEEN '09:00:00' AND '09:59:59' THEN 1 ELSE 0 END AS hour_9,
    CASE
        WHEN TIME(shipping_time) BETWEEN '10:00:00' AND '10:59:59' THEN 1 ELSE 0 END AS hour_10,
    CASE
        WHEN TIME(shipping_time) BETWEEN '11:00:00' AND '11:59:59' THEN 1 ELSE 0 END AS hour_11,
    CASE
        WHEN TIME(shipping_time) BETWEEN '12:00:00' AND '12:59:59' THEN 1 ELSE 0 END AS hour_12,
    
    ----------------------------------------------------------------------------------------------
    CASE
        WHEN TIME(shipping_time) BETWEEN '13:00:00' AND '13:59:59' THEN 1 ELSE 0 END AS hour_13,
    CASE
        WHEN TIME(shipping_time) BETWEEN '14:00:00' AND '14:59:59' THEN 1 ELSE 0 END AS hour_14,
    CASE
        WHEN TIME(shipping_time) BETWEEN '15:00:00' AND '15:59:59' THEN 1 ELSE 0 END AS hour_15,
    CASE
        WHEN TIME(shipping_time) BETWEEN '16:00:00' AND '16:59:59' THEN 1 ELSE 0 END AS hour_16,
    CASE
        WHEN TIME(shipping_time) BETWEEN '17:00:00' AND '17:59:59' THEN 1 ELSE 0 END AS hour_17,
    CASE
        WHEN TIME(shipping_time) BETWEEN '18:00:00' AND '18:59:59' THEN 1 ELSE 0 END AS hour_18,
    CASE
        WHEN TIME(shipping_time) BETWEEN '19:00:00' AND '19:59:59' THEN 1 ELSE 0 END AS hour_19,
    CASE
        WHEN TIME(shipping_time) BETWEEN '20:00:00' AND '20:59:59' THEN 1 ELSE 0 END AS hour_20,
    CASE
        WHEN TIME(shipping_time) BETWEEN '21:00:00' AND '21:59:59' THEN 1 ELSE 0 END AS hour_21,
    CASE
        WHEN TIME(shipping_time) BETWEEN '22:00:00' AND '22:59:59' THEN 1 ELSE 0 END AS hour_22,
    CASE
        WHEN TIME(shipping_time) BETWEEN '23:00:00' AND '23:59:59' THEN 1 ELSE 0 END AS hour_23,


    FROM join_pickup_to_mh
WHERE next_location_branch IS NOT NULL
  )
  GROUP BY 1,2
)

SELECT * FROM count_waybill_pickup_hour
--SELECT * FROM join_pickup_to_mh
--WHERE next_location_branch IS NULL
