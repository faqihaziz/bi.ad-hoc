WITH table_pricing AS (
  SELECT 
    sender_location_id,
    recipient_location_id,
    express_type,
    shipping_client_id,
    discount_rate,
    min_sla,
    max_sla

 FROM `grand-sweep-324604.datawarehouse_idexp.standard_shipping_fee` 
 WHERE DATE(end_expire_time, 'Asia/Jakarta') > CURRENT_DATE('Asia/Jakarta')
      AND deleted = '0'

QUALIFY ROW_NUMBER() OVER (PARTITION BY search_code ORDER BY created_at DESC)=1
)

SELECT ww.waybill_no, t2.min_sla, t2.max_sla

FROM `datawarehouse_idexp.waybill_waybill` ww
LEFT JOIN table_pricing t2 -- via cte karena perlu remove duplikat, jadi ada pricing yg dobel tapi ambil yg di create terbaru
              ON t2.shipping_client_id = ww.vip_customer_id 
              AND ww.sender_city_id = t2.sender_location_id 
              AND ww.recipient_district_id  = t2.recipient_location_id 
              AND ww.express_type = t2.express_type



WHERE ww.waybill_source = '116'
    AND DATE(shipping_time,'Asia/Jakarta') = '2023-10-31'

    LIMIT 10
