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
-- CASE WHEN ww.pod_record_time IS NOT NULL THEN 1 ELSE 0 END AS pod_alias,
CASE WHEN ww.pod_record_time IS NOT NULL AND DATE(ww.pod_record_time,'Asia/Jakarta') <= '2024-02-15' THEN 1 ELSE 0 END AS pod_alias, --pakai cutoff
CASE WHEN ww.pod_record_time IS NULL AND rr.return_confirm_record_time IS NULL AND rr.return_pod_record_time IS NULL THEN 1 ELSE 0 END AS stuck_alias,
CASE WHEN ww.pod_record_time IS NULL AND rr.return_confirm_record_time IS NOT NULL AND rr.return_pod_record_time IS NULL THEN 1 ELSE 0 END AS return_process_alias,
-- CASE WHEN rr.return_confirm_record_time IS NOT NULL THEN 1 ELSE 0 END AS return_confirm_alias,
CASE WHEN rr.return_confirm_record_time IS NOT NULL AND DATE(rr.return_confirm_record_time,'Asia/Jakarta') <= '2024-02-15' THEN 1 ELSE 0 END AS return_confirm_alias, --pakai cutoff
-- CASE WHEN rr.return_pod_record_time IS NOT NULL THEN 1 ELSE 0 END AS return_pod_alias,
CASE WHEN rr.return_pod_record_time IS NOT NULL AND DATE(rr.return_pod_record_time,'Asia/Jakarta') <= '2024-02-15' THEN 1 ELSE 0 END AS return_pod_alias, --pakai cut off

DATE(rr.return_record_time,'Asia/Jakarta') return_regist_time,
DATE(rr.return_confirm_record_time,'Asia/Jakarta') return_confirm_record_time,
rc.option_name AS return_confirm_status,
DATE(rr.return_pod_record_time,'Asia/Jakarta') return_pod_record_time,
kw.kanwil_name AS kanwil_area_deliv,
kw1.kanwil_name kanwil_origin,
CASE WHEN ww.void_flag = '01' THEN 1 ELSE 0 END AS total_void,
CAST(ww.item_calculated_weight AS NUMERIC) item_calculated_weight,
ww.standard_shipping_fee,
et.option_name AS express_type,
ww.item_name,
ww.cod_amount,
CASE
    WHEN ww.pod_record_time IS NOT NULL THEN ww.standard_shipping_fee
    WHEN ww.pod_record_time IS NULL THEN 0 
    END AS shipping_fee_pod,


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

WHERE DATE(ww.shipping_time,'Asia/Jakarta') BETWEEN '2024-01-01' AND '2024-01-31'
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

QUALIFY ROW_NUMBER() OVER (PARTITION BY ww.waybill_no ORDER BY rr.update_time DESC)=1
)
-- WHERE source_category IN ("Aggregator")
-- AND waybill_source IN ('arveoli','Oexpress')
-- AND waybill_source IN ('everpro')
)

SELECT

-- waybill_no,
-- shipping_date,
-- month_arrival dest_month,
-- sender_city_name origin_city,
-- sender_province_name origin_province,
-- kanwil_origin,
recipient_city_name destination_city,
recipient_province_name destination_province,
month_pickup,
-- kanwil_area_deliv kanwil_area_dest,
-- waybill_source,
-- vip_username,
-- source_category,
-- sender_name,
-- item_name,
-- recipient_district_name destination_district,
-- return_confirm_record_time,
-- return_or_not,
-- cod_type,
-- express_type,
-- pod_record_time,
-- return_confirm_record_time,
-- return_pod_record_time,
-- pod_alias,
-- return_confirm_alias,
-- return_pod_alias,
-- SUM(waybill_alias) AS total_pickup,
COUNT(waybill_no) AS total_pickup,
-- SUM(pod_alias) AS total_pod,
-- SUM(return_confirm_alias) total_return_confirmed,
-- SUM(return_pod_alias) total_pod_return,
-- SUM(item_calculated_weight) weight,
-- SUM(total_pickup_cod) total_pickup_cod,
-- SUM(total_pod_cod) total_pod_cod,
-- SUM(return_confirm_cod) total_return_confirm_cod,
-- SUM(return_pod_cod) total_return_pod_cod,
SUM(standard_shipping_fee) gross_revenue_pickup,
-- SUM(shipping_fee_pod) gross_revenue_pod,
-- SUM(cod_amount) total_cod_amount,
-- SUM(total_void) total_void,
-- SUM(stuck_alias) total_stuck_awb,
-- SUM(return_process_alias) total_return_process,


FROM (

SELECT a.*,
CASE WHEN a.return_confirm_record_time IS NOT NULL THEN "Return" ELSE "Non Return" END AS return_or_not,
-- CASE WHEN a.return_confirm_record_time IS NOT NULL THEN 1 ELSE 0 END AS return_confirm_alias,
-- CASE WHEN a.return_pod_record_time IS NOT NULL THEN 1 ELSE 0 END AS return_pod_alias,
-- CASE WHEN a.return_pod_record_time IS NOT NULL AND DATE(a.return_pod_record_time) <= '2024-02-15' THEN 1 ELSE 0 END AS return_pod_alias, --pakai cutoff
CASE WHEN cod_type = "COD" AND waybill_alias = 1 THEN 1 ELSE 0 END AS total_pickup_cod,
CASE WHEN cod_type = "COD" AND pod_alias = 1 THEN 1 ELSE 0 END AS total_pod_cod,
CASE WHEN cod_type = "COD" AND return_confirm_alias = 1 THEN 1 ELSE 0 END AS return_confirm_cod,
CASE WHEN cod_type = "COD" AND return_pod_alias = 1 THEN 1 ELSE 0 END AS return_pod_cod,

FROM waybill_data a
-- LEFT OUTER JOIN arrival_th_dest b ON a.waybill_no = b.waybill_no
)
-- GROUP BY 1,2,3,4,5
-- GROUP BY 1,2,3,4,5,6,7,8,9,10,11
GROUP BY 1,2,3
-- GROUP BY 1,2,3,4,5,6,7


