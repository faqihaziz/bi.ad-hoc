WITH waybill_data AS (

SELECT * FROM (

  SELECT
  ww.waybill_no,
  CASE WHEN ww.waybill_no IS NOT NULL THEN 1 ELSE 0 END AS waybill_alias,
  FORMAT_DATE("%b %Y", DATE(ww.shipping_time,'Asia/Jakarta')) AS month_pickup,
  DATE(ww.shipping_time,'Asia/Jakarta') shipping_date,
  ww.sender_province_name,
  ww.sender_city_name,
  ww.recipient_city_name,
  ww.recipient_province_name,
  ww.recipient_district_name,
  sr.option_name AS waybill_source,
  ww.sender_name,
  ww.parent_shipping_cleint vip_username,
  CASE
    WHEN sr.option_name IN ("arveoli",
"Baleomol",
"Berdu",
"BiteShip",
"BukaSend",
"Clodeo",
"pt clodeo indonesia jaya",
"diorderin",
"eBelanja",
"everpro",
"IdeJualan",
"KiriminAja",
"MauLagi",
"Mengantar",
"Oexpress",
"OrderOnline",
"pt admin cerdas indonesia",
"pt multi kurir digital",
"pt auto serba digital",
"Komerce",
"pt usaha logistik indonesia",
"Hadid",
"PT. Auto Serba",
"Juragan COD",
"Ngorder",
"Jubelio",
"Lincah.id") THEN "Aggregator"
    WHEN sr.option_name IN ('VIP Customer Portsl') THEN "VIP"
    WHEN sr.option_name IN ("pt fashion marketplace indonesia","pt fashion eservices indonesia","Blibli","Alfatrex","Shopee express","Shopee platform", "Shopee Crossborder","Shopee CB Return","Shopee Express Platform","Tokopedia") THEN "E-Commerce"
    ELSE "Others"
    END AS source_category,

CASE 
    WHEN ww.cod_amount > 0 THEN "COD"
    WHEN ww.cod_amount <= 0 THEN "Non-COD"
    END AS cod_type,

DATE(ww.pod_record_time,'Asia/Jakarta') pod_record_time,
FORMAT_DATE("%b %Y", DATE(ww.pod_record_time,'Asia/Jakarta')) AS month_pod,
ww.pod_branch_name,
-- CASE WHEN ww.pod_record_time IS NOT NULL THEN 1 ELSE 0 END AS pod_alias,
CASE WHEN ww.pod_record_time IS NULL AND rr.return_confirm_record_time IS NULL AND rr.return_pod_record_time IS NULL THEN 1 ELSE 0 END AS stuck_alias,
-- CASE WHEN ww.pod_record_time IS NULL AND rr.return_confirm_record_time IS NOT NULL AND rr.return_pod_record_time IS NULL THEN 1 ELSE 0 END AS return_process_alias,
-- CASE WHEN rr.return_confirm_record_time IS NOT NULL THEN 1 ELSE 0 END AS return_confirm_alias,

-- DATE(rr.return_record_time,'Asia/Jakarta') return_regist_time,
-- DATE(rr.return_confirm_record_time,'Asia/Jakarta') return_confirm_record_time,
-- rc.option_name AS return_confirm_status,
-- DATE(rr.return_pod_record_time,'Asia/Jakarta') return_pod_record_time,
kw.kanwil_name AS kanwil_area_deliv,
kw1.kanwil_name kanwil_origin,
CASE WHEN ww.void_flag = '01' THEN 1 ELSE 0 END AS total_void,
CAST(ww.item_calculated_weight AS NUMERIC) item_calculated_weight,
-- ww.standard_shipping_fee,
et.option_name AS express_type,
ww.item_name,
ww.cod_amount,
-- CASE
--     WHEN ww.pod_record_time IS NOT NULL THEN ww.standard_shipping_fee
--     WHEN ww.pod_record_time IS NULL THEN 0 
--     END AS shipping_fee_pod,


  -- FROM `datawarehouse_idexp.waybill_waybill` ww
  FROM `datawarehouse_idexp.dm_waybill_waybill` ww
  LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON ww.waybill_no = rr.waybill_no AND rr.return_void_flag = '0'
  -- AND DATE(rr.update_time,'Asia/Jakarta') BETWEEN '2023-10-01' ANDAND '2023-11-15' -- CURRENT_DATE('Asia/Jakarta') --'2023-10-24'
left join `grand-sweep-324604.datawarehouse_idexp.system_option` sr on ww.waybill_source  = sr.option_value and sr.type_option = 'waybillSource'
left join `grand-sweep-324604.datawarehouse_idexp.system_option` ws on ww.waybill_status  = ws.option_value and ws.type_option = 'waybillStatus'
left join `grand-sweep-324604.datawarehouse_idexp.system_option` et on ww.express_type  = et.option_value and et.type_option = 'expressType'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rc ON rc.option_value = rr.return_confirm_status AND rc.type_option = 'returnConfirmStatus'
LEFT JOIN `datamart_idexp.mapping_kanwil_area` kw ON ww.recipient_province_name = kw.province_name
LEFT JOIN `datamart_idexp.mapping_kanwil_area` kw1 ON ww.sender_province_name = kw1.province_name

WHERE DATE(ww.pod_record_time,'Asia/Jakarta') BETWEEN '2023-12-01' AND '2024-02-20'
AND ww.void_flag = '0' AND ww.deleted= '0'
-- AND DATE(ww.update_time,'Asia/Jakarta') <= '2024-02-20' --BETWEEN '2023-12-01' AND '2024-01-15' --CURRENT_DATE('Asia/Jakarta')
-- AND kw.kanwil_name IN ('SUMATERA BAGIAN UTARA','SUMATERA BAGIAN SELATAN','KALIMANTAN','SULAWESI')
-- AND ww.recipient_city_name IN ('MANADO')
-- AND sr.option_name NOT IN ("pt fashion marketplace indonesia","pt fashion eservices indonesia","Blibli","Alfatrex","Shopee express","Shopee platform", "Shopee Crossborder","Shopee CB Return","Shopee Express Platform","Tokopedia")
-- AND sr.option_name IN ("Shopee platform")
-- AND sr.option_name IN ('VIP Customer Portal')
-- AND ww.pod_record_time IS NULL 
-- AND rr.return_confirm_record_time IS NOT NULL
-- AND rr.return_pod_record_time IS NULL
-- AND ww.recipient_province_name IN (
-- "NTB",
-- "NTT",
-- "PAPUA",
-- "MALUKU",
-- "PAPUA BARAT",
-- "MALUKU UTARA"
-- )

QUALIFY ROW_NUMBER() OVER (PARTITION BY ww.waybill_no ORDER BY ww.update_time DESC)=1
)
-- WHERE source_category IN ("Aggregator")
-- AND waybill_source IN ('arveoli','Oexpress')
-- AND waybill_source IN ('everpro')
),

pod_data AS (

  SELECT
  waybill_no,
  waybill_source,
  pod_record_time,
  month_pod,
  recipient_city_name,
  recipient_province_name,
  kanwil_area_deliv,
  pod_branch_name,

  FROM waybill_data
),

dummy_pos_and_delivery_scan AS (

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
                                                
            -- WHERE DATE(currenttab.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -35 DAY))
            WHERE DATE(currenttab.record_time,'Asia/Jakarta') BETWEEN '2024-01-01' AND '2024-02-20'
            AND currenttab.deleted = '0'
            AND currenttab.operation_type IN ('18','09')
            
            -- AND waybill_no IN (
                        -- ) --'IDE703620797395', 'IDM500837282987'
            -- AND bag_no IN ('BM1830016569')
            -- AND operation_branch_name IN ('MH JAKARTA')
            -- AND option.option_name IN ('Problem On Shipment scan','Delivery scan')
            -- AND currenttab.vehicle_tag_no IN (

            -- )

ORDER BY record_time DESC
            )

),

get_pos_scan AS (

  SELECT

  waybill_no,
  DATE(record_time) pos_scan,
  operation_type,


  -- FROM `dev_idexp.dummy_pos_and_delivery_scan`
  FROM dummy_pos_and_delivery_scan
  WHERE operation_type = 'Problem On Shipment scan'
),

get_deliv_scan AS (

  SELECT

  waybill_no,
  DATE(record_time) deliv_scan,
  operation_type,


  -- FROM `dev_idexp.dummy_pos_and_delivery_scan`
  FROM dummy_pos_and_delivery_scan
  WHERE operation_type = 'Delivery scan'
),

count_deliv_scan AS (

  SELECT

  waybill_no,
  SUM(deliv_pos_scan) deliv_pos_scan,

  FROM (

    SELECT
  a.waybill_no,
  CASE
      WHEN deliv_scan = pos_scan THEN 1 ELSE 0
      END AS deliv_pos_scan,
  
  FROM get_pos_scan a
  LEFT JOIN get_deliv_scan b ON a.waybill_no = b.waybill_no
)
GROUP BY 1
),

join_pod_and_count_attempt AS (

  SELECT

  ww.waybill_no,
  ww.waybill_source,
  ww.month_pod,
  ww.recipient_city_name,
  ww.recipient_province_name,
  ww.kanwil_area_deliv,
  ww.pod_branch_name,
  CASE
      WHEN sc.deliv_pos_scan IS NULL THEN 0
      ELSE sc.deliv_pos_scan
      END AS deliv_pos_scan,

  FROM pod_data ww
  LEFT JOIN count_deliv_scan sc ON ww.waybill_no = sc.waybill_no
),

gabung_all AS (

SELECT 

-- waybill_no,
month_pod month,
-- waybill_source,
  pod_branch_name,
recipient_city_name,
  recipient_province_name,
  kanwil_area_deliv,

SUM(attempt_0) attempt_0,
SUM(attempt_1) attempt_1,
SUM(attempt_2) attempt_2,
SUM(attempt_3) attempt_3,
SUM(attempt_4) attempt_4,
SUM(attempt_5) attempt_5,
SUM(attempt_6) attempt_6,
SUM(attempt_7) attempt_7,
SUM(attempt_8) attempt_8,
SUM(attempt_9) attempt_9,
SUM(attempt_10) attempt_10,
SUM(attempt_10_more) attempt_10_and_more,


FROM (

SELECT 

*,
CASE
    WHEN deliv_pos_scan = 0 THEN 1 ELSE 0
    END AS attempt_0,
CASE
    WHEN deliv_pos_scan = 1 THEN 1 ELSE 0
    END AS attempt_1,
CASE
    WHEN deliv_pos_scan = 2 THEN 1 ELSE 0
    END AS attempt_2,
CASE
    WHEN deliv_pos_scan = 3 THEN 1 ELSE 0
    END AS attempt_3,    
CASE
    WHEN deliv_pos_scan = 4 THEN 1 ELSE 0
    END AS attempt_4,
CASE
    WHEN deliv_pos_scan = 5 THEN 1 ELSE 0
    END AS attempt_5,
CASE
    WHEN deliv_pos_scan = 6 THEN 1 ELSE 0
    END AS attempt_6,
CASE
    WHEN deliv_pos_scan = 7 THEN 1 ELSE 0
    END AS attempt_7,
CASE
    WHEN deliv_pos_scan = 8 THEN 1 ELSE 0
    END AS attempt_8,
CASE
    WHEN deliv_pos_scan = 9 THEN 1 ELSE 0
    END AS attempt_9,
CASE
    WHEN deliv_pos_scan = 10 THEN 1 ELSE 0
    END AS attempt_10,
CASE
    WHEN deliv_pos_scan > 10 THEN 1 ELSE 0
    END AS attempt_10_more,

FROM join_pod_and_count_attempt

)
-- GROUP BY 1,2,3,4,5,6
GROUP BY 1,2,3,4,5
)

SELECT * FROM gabung_all
WHERE pod_branch_name NOT IN ('')
-- SELECT * FROM count_deliv_scan
-- WHERE deliv_pos_scan = 10

