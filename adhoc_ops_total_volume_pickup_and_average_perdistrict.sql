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

  CASE WHEN ww.sender_district_name IS NULL THEN oo.sender_district_name ELSE ww.sender_district_name END AS sender_district_name,
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
  LEFT JOIN `datawarehouse_idexp.order_order` oo ON ww.waybill_no = oo.waybill_no

  -- WHERE DATE(ww.shipping_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY))
  WHERE DATE(ww.shipping_time,'Asia/Jakarta') BETWEEN '2024-05-01' AND '2024-05-15'
  AND ww.void_flag = '0' AND ww.deleted = '0'
  AND ww.sender_province_name IN ('BALI')
  -- AND sr.option_name IN ('BukaSend','pt buka usaha indonesia')
  -- AND ww.vip_customer_name IN ('universitasterbuka01p')

  QUALIFY ROW_NUMBER() OVER(PARTITION BY ww.waybill_no ORDER BY ww.update_time DESC)=1
)

SELECT

waybill_source,
month_pickup,
sender_province_name,
sender_city_name,
sender_district_name,
total_awb,
(total_awb/15) avg_daily,

FROM (

  SELECT

waybill_source,
month_pickup,
sender_province_name,
sender_city_name,
sender_district_name,
COUNT(waybill_no) total_awb,
-- AVG(waybill_no) avg_daily,

FROM waybill_data

GROUP BY 1,2,3,4,5
)
GROUP BY 1,2,3,4,5,6
