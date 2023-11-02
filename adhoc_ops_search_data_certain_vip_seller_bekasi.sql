SELECT *

FROM (

  SELECT

ww.waybill_no,
DATETIME(ww.shipping_time,'Asia/Jakarta') shipping_time,
ww.pickup_branch_name,
ww.total_shipping_fee,
ww.vip_customer_name,
ww.sender_name,
ww.sender_cellphone,
pt.option_name AS payment_type,
et.option_name AS express_type,
CASE 
    WHEN ww.pod_record_time IS NOT NULL THEN "POD"
    WHEN ww.pod_record_time IS NULL THEN "Not POD"
    END AS pod_status,
-- pd.option_name AS pod_flag,
DATE(ww.pod_record_time,'Asia/Jakarta') pod_date,
sr.option_name AS waybill_source,



-- FROM `warehouse_idexp.ide_waybill_waybill` ww
FROM `datawarehouse_idexp.waybill_waybill` ww
left join `grand-sweep-324604.datawarehouse_idexp.system_option` sr on ww.waybill_source  = sr.option_value and sr.type_option = 'waybillSource'
left join `grand-sweep-324604.datawarehouse_idexp.system_option` pt on ww.payment_type = pt.option_value and pt.type_option = 'paymentType'
left join `grand-sweep-324604.datawarehouse_idexp.system_option` et on ww.express_type = et.option_value and et.type_option = 'expressType'
left join `grand-sweep-324604.datawarehouse_idexp.system_option` pd on ww.pod_flag = pd.option_value and pd.type_option = 'podFlag'

WHERE DATE(ww.shipping_time,'Asia/Jakarta') BETWEEN '2022-12-01' AND '2022-12-31'
-- AND ww.vip_customer_name IN ('drwsskincare01','fashionthrift01','fromoca01','icalshop01','laqulashop01','mobako01','nazeerashop01','nisabagshop01','ptsuryadutainternasional02','riribeauty01','rumahobral01','safinashop01','sgh01','warnagrafika01','yohanaolshop01','zainsnack02','zoyaharapanindah01')
AND ww.vip_customer_name IN ('zboutique01')


QUALIFY ROW_NUMBER() OVER (PARTITION BY ww.waybill_no ORDER BY ww.update_time DESC)=1 --buat remove duplicate
)
ORDER BY shipping_time ASC
