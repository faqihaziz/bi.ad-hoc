SELECT 

waybill_no,
-- sender_cellphone,
input_date,
shipping_date,
update_time,

FROM (

  SELECT

oo.waybill_no,
oo.sender_cellphone,
DATE(oo.input_time,'Asia/Jakarta') input_date,
DATE(ww.shipping_time,'Asia/Jakarta') shipping_date,
ww.pickup_branch_name,
DATE(ww.update_time,'Asia/Jakarta') update_time,
-- ww.item_calculated_weight,

FROM `datawarehouse_idexp.order_order` oo
-- FROM `datawarehouse_idexp.waybill_waybill` ww
LEFT OUTER JOIN `datawarehouse_idexp.waybill_waybill` ww ON oo.waybill_no = ww.waybill_no
-- AND DATE(ww.update_time,'Asia/Jakarta')
LEFT OUTER JOIN `dev_idexp.masterdata_branch_coverage_th` mb ON ww.recipient_district_id = mb.district_id
-- LEFT OUTER JOIN `datawarehouse_idexp.dm_waybill_waybill_line` sc ON ww.waybill_no = sc.waybill_no
left join `grand-sweep-324604.datawarehouse_idexp.system_option` sr on oo.order_source  = sr.option_value and sr.type_option = 'orderSource'
-- AND DATE(sc.record_time,'Asia/Jakarta') BETWEEN '2022-08-01' AND '2023-11-23' -->= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -186 DAY))
-- LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd16 ON rd16.option_value = sc.operation_type AND rd16.type_option = 'operationType'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` t0 ON t0.option_value = ww.waybill_source AND t0.type_option = 'waybillSource'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` t1 ON t1.option_value = ww.waybill_status AND t1.type_option = 'waybillStatus'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` t2 ON t2.option_value = ww.void_flag AND t2.type_option = 'voidFlag'


WHERE DATE(oo.input_time,'Asia/Jakarta') BETWEEN '2024-03-13' AND '2024-03-13' -->= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -186 DAY))
-- WHERE DATE(ww.shipping_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -186 DAY))

AND sr.option_name IN ('Tokopedia')
AND ww.cod_amount >0

QUALIFY ROW_NUMBER() OVER (PARTITION BY oo.waybill_no ORDER BY ww.update_time DESC)=1
)
ORDER BY update_time DESC
