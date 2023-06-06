SELECT 
ww.waybill_no AS Waybill_No,
ww.order_no AS Order_No,
t9.option_name AS Waybill_Source,
FORMAT_DATE("%b %Y", DATE(ww.shipping_time,'Asia/Jakarta')) AS Month_Shipping,
DATETIME(ww.shipping_time,'Asia/Jakarta') AS Shipping_Time,
ww.item_calculated_weight AS Weight,
ww.total_shipping_fee AS Total_Shipping_Fee,
ww.sender_province_name AS Origin_Province,
ww.sender_city_name AS Origin_City,
ww.sender_district_name AS rigin_District,
ww.pickup_branch_name AS Origin_Branch,
ww.recipient_name AS Recipient_Name,
ww.recipient_address AS Recipient_Address,
ww.recipient_province_name AS Destination_Province,
ww.recipient_city_name AS Destination_City,
ww.recipient_district_name AS Destination,
FORMAT_DATE("%b %Y", DATE(ww.pod_record_time,'Asia/Jakarta')) AS Month_POD,
DATETIME(ww.pod_record_time,'Asia/Jakarta') POD_Time,
ww.pod_photo_url AS POD_Photo,


FROM `datawarehouse_idexp.waybill_waybill` ww
LEFT OUTER JOIN `grand-sweep-324604.datawarehouse_idexp.system_option` t9 on ww.waybill_source  = t9.option_value and t9.type_option = 'waybillSource'

WHERE 
-- DATE(ww.shipping_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -120 DAY))
DATE(ww.shipping_time,'Asia/Jakarta') BETWEEN '2023-01-01' AND '2023-02-28'

AND ww.waybill_source = '117'

-- ORDER BY ww.pod_record_time ASC
ORDER BY ww.shipping_time ASC
