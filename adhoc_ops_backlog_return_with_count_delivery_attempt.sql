WITH waybill_data AS (

SELECT
  ww.waybill_no,
  CASE WHEN ww.waybill_no IS NOT NULL THEN 1 END AS waybill_alias,
  -- FORMAT_DATE("%b %Y", DATE(ww.shipping_time,'Asia/Jakarta')) AS month,
  DATETIME(ww.shipping_time,'Asia/Jakarta') shipping_time,
  sr.option_name AS waybill_source,
  ww.parent_shipping_cleint vip_username,
  ww.sender_name,
  ww.sender_cellphone,
  ww.sender_province_name,
  ww.sender_city_name,
  ww.sender_district_name,
  ww.pickup_branch_name,
  ww.recipient_district_name,
  ww.recipient_city_name,
  ww.recipient_province_name,
  et.option_name AS express_type,
  st.option_name AS service_type,
  kw.kanwil_name AS kanwil_name_regist,

    FROM `datawarehouse_idexp.waybill_waybill` ww
left join `grand-sweep-324604.datawarehouse_idexp.system_option` sr on ww.waybill_source  = sr.option_value and sr.type_option = 'waybillSource'
left join `grand-sweep-324604.datawarehouse_idexp.system_option` et on ww.express_type  = et.option_value and et.type_option = 'expressType'
left join `grand-sweep-324604.datawarehouse_idexp.system_option` st on ww.service_type  = st.option_value and st.type_option = 'serviceType'
LEFT JOIN `datamart_idexp.mapping_kanwil_area` kw ON ww.recipient_province_name = kw.province_name

WHERE DATE(ww.update_time,'Asia/Jakarta') BETWEEN '2023-08-01' AND '2023-10-31' -->= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY))
AND DATE(ww.shipping_time,'Asia/Jakarta') BETWEEN '2023-08-01' AND '2023-10-31'
AND ww.void_flag = '0' AND ww.deleted= '0'

QUALIFY ROW_NUMBER() OVER (PARTITION BY ww.waybill_no ORDER BY ww.update_time DESC)=1
),

return_data AS (

  SELECT
rr.waybill_no,
DATETIME(rr.return_record_time,'Asia/Jakarta') return_regist_time,
rr.return_branch_name return_register_branch,
t5.return_type AS remarks_return,
DATETIME(rr.return_confirm_record_time,'Asia/Jakarta') return_confirm_time,
rc.option_name AS return_confirm_status,
rr.return_shipping_fee,
DATETIME(rr.return_pod_record_time,'Asia/Jakarta') return_pod_record_time,
DATETIME(rr.update_time,'Asia/Jakarta') update_time_rr,

CASE WHEN rr.return_record_time IS NOT NULL THEN 1 ELSE 0 END AS return_regist_alias,
CASE WHEN rr.return_confirm_record_time IS NOT NULL THEN 1 ELSE 0 END AS return_confirm_alias,
CASE WHEN rr.return_pod_record_time IS NOT NULL THEN 1 ELSE 0 END AS return_pod_alias,


  FROM `datawarehouse_idexp.waybill_return_bill` rr
  LEFT OUTER JOIN `datawarehouse_idexp.system_option` rc ON rc.option_value = rr.return_confirm_status AND rc.type_option = 'returnConfirmStatus'
LEFT OUTER JOIN `grand-sweep-324604.datawarehouse_idexp.return_type` t5 ON rr.return_type_id = t5.id AND t5.deleted=0
LEFT OUTER JOIN `datamart_idexp.masterdata_city_mapping_area_island_new` pu3 ON rr.recipient_city_name = pu3.city and rr.recipient_province_name = pu3.province --Return_area_register, 

WHERE DATE(rr.update_time,'Asia/Jakarta') BETWEEN '2023-08-01' AND CURRENT_DATE() -->= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY))
AND DATE(rr.return_record_time,'Asia/Jakarta') BETWEEN '2023-08-01' AND '2023-10-31' -->= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY))

QUALIFY ROW_NUMBER() OVER (PARTITION BY rr.waybill_no ORDER BY rr.update_time DESC)=1
),

count_deliv_attempt AS (

SELECT
waybill_no,
COUNT(scan_type) OVER (PARTITION BY waybill_no) AS count_deliv_attempt,

FROM (

  SELECT

  sc.waybill_no,
  DATE(sc.record_time,'Asia/Jakarta') record_time,
  rd16.option_name AS scan_type,
  rr.return_regist_time,

FROM `datawarehouse_idexp.waybill_waybill_line` sc 
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd16 ON rd16.option_value = sc.operation_type AND rd16.type_option = 'operationType'
LEFT OUTER JOIN return_data rr ON sc.waybill_no = rr.waybill_no

WHERE 
DATE(sc.record_time,'Asia/Jakarta') BETWEEN '2023-08-01' AND CURRENT_DATE() -->= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY))
AND operation_type = "09"
AND DATE(sc.record_time,'Asia/Jakarta') <= DATE(rr.return_regist_time)

)
QUALIFY ROW_NUMBER() OVER (PARTITION BY waybill_no)=1
),

deliv_attempt as(

  SELECT
sc.waybill_no,
sc.deliv_attempt_1,
sc.deliv_attempt_2,
sc.deliv_attempt_3,
sc.deliv_attempt_4,
sc.deliv_attempt_5,


FROM (
  SELECT
        waybill_no,
        MAX(IF(id = 1, DATE(record_time), NULL)) AS deliv_attempt_1,
        MAX(IF(id = 2, DATE(record_time), NULL)) AS deliv_attempt_2,
        MAX(IF(id = 3, DATE(record_time), NULL)) AS deliv_attempt_3,
        MAX(IF(id = 4, DATE(record_time), NULL)) AS deliv_attempt_4,
        MAX(IF(id = 5, DATE(record_time), NULL)) AS deliv_attempt_5,
        MAX(IF(id = 6, DATE(record_time), NULL)) AS deliv_attempt_6,
        MAX(IF(id = 7, DATE(record_time), NULL)) AS deliv_attempt_7,
        FROM (

          SELECT
          waybill_no,
          record_time,
          
              RANK() OVER (PARTITION BY waybill_no ORDER BY DATE(record_time) ASC ) AS id

          FROM(
          
              SELECT sc.waybill_no, 
              DATE(sc.record_time,'Asia/Jakarta') record_time, 
                        
              FROM `datawarehouse_idexp.waybill_waybill_line` sc
              LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON sc.waybill_no = rr.waybill_no
AND DATE(rr.update_time,'Asia/Jakarta') BETWEEN '2023-08-01' AND CURRENT_DATE() -->= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -226 DAY))
              WHERE DATE(sc.record_time,'Asia/Jakarta') BETWEEN '2023-08-01' AND CURRENT_DATE() -->= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -226 DAY))
              AND sc.operation_type IN ('09')
              AND DATE(sc.record_time,'Asia/Jakarta') <= DATE(rr.return_record_time,'Asia/Jakarta')
              GROUP BY 1,2
        ) 
        )
        GROUP BY 1 
) sc
QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.waybill_no)=1
),

last_deliv_attempt AS (

SELECT
sc.waybill_no,
MAX(DATE(sc.record_time,'Asia/Jakarta')) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) AS last_deliv_attempt,
MAX(rd16.option_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) AS scan_type,

FROM `datawarehouse_idexp.waybill_waybill_line` sc 
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd16 ON rd16.option_value = sc.operation_type AND rd16.type_option = 'operationType'
LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON sc.waybill_no = rr.waybill_no
AND DATE(rr.update_time,'Asia/Jakarta') BETWEEN '2023-08-01' AND CURRENT_DATE() -->= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -226 DAY))

WHERE 
DATE(sc.record_time,'Asia/Jakarta') BETWEEN '2023-08-01' AND CURRENT_DATE() -->= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -226 DAY))
AND operation_type = "09"
AND DATE(sc.record_time,'Asia/Jakarta') <= DATE(rr.return_record_time,'Asia/Jakarta')

QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC)=1
),

join_return AS (

SELECT 

*,
DATE_DIFF(CURRENT_DATE(),return_regist_time, DAY) AS aging_backlog_confirm,
DATE_DIFF(CURRENT_DATE(),return_confirm_time, DAY) AS aging_backlog_return,


FROM (

SELECT 

ww.waybill_no,
ww.waybill_alias,
ww.shipping_time,
ww.waybill_source,
ww.vip_username,
ww.sender_name,
ww.sender_cellphone,
ww.sender_province_name,
ww.sender_city_name,
ww.sender_district_name,
ww.pickup_branch_name,
ww.recipient_district_name,
ww.recipient_city_name,
ww.recipient_province_name,
ww.express_type,
ww.service_type,
ww.kanwil_name_regist,

rr.return_regist_time,
rr.return_register_branch,
rr.remarks_return,
rr.return_confirm_time,
rr.return_confirm_status,
rr.return_shipping_fee,
rr.return_pod_record_time,
rr.update_time_rr,

rr.return_regist_alias,
rr.return_confirm_alias,
(rr.return_regist_alias - rr.return_confirm_alias) AS backlog_confirm_alias,
rr.return_pod_alias,

CASE
    WHEN rr.return_regist_time IS NOT NULL AND rr.return_confirm_time IS NULL THEN "Backlog Confirm Return"
    WHEN rr.return_regist_time IS NOT NULL AND rr.return_confirm_time IS NOT NULL AND rr.return_pod_record_time IS NULL THEN "Backlog Return"
    WHEN rr.return_regist_time IS NOT NULL AND rr.return_confirm_time IS NOT NULL AND rr.return_pod_record_time IS NOT NULL THEN "POD Return"
    END AS backlog_return_flag,

cd.count_deliv_attempt,

sc.deliv_attempt_1,
sc.deliv_attempt_2,
sc.deliv_attempt_3,
ld.last_deliv_attempt,



FROM waybill_data ww
LEFT JOIN return_data rr ON ww.waybill_no = rr.waybill_no
LEFT JOIN count_deliv_attempt cd ON ww.waybill_no = cd.waybill_no
LEFT JOIN deliv_attempt sc ON ww.waybill_no = sc.waybill_no
LEFT JOIN last_deliv_attempt ld ON ww.waybill_no = ld.waybill_no

WHERE rr.return_regist_time IS NOT NULL
AND rr.return_pod_record_time IS NULL
)
)

SELECT 
--*
waybill_no,
DATE(shipping_time) shipping_date,
waybill_source,
sender_name,
sender_province_name,
sender_city_name,
sender_district_name,
pickup_branch_name,
recipient_district_name,
recipient_city_name,
recipient_province_name,
count_deliv_attempt,
deliv_attempt_1,
deliv_attempt_2,
deliv_attempt_3,
last_deliv_attempt,

return_regist_time,
return_confirm_time,
backlog_return_flag,


FROM (

  SELECT
*,
CASE 
    WHEN backlog_return_flag IN ('Backlog Confirm Return') AND aging_backlog_confirm <3 THEN "<3 Days"
    WHEN backlog_return_flag IN ('Backlog Confirm Return') AND aging_backlog_confirm BETWEEN 3 AND 7 THEN "3-7 Days"
    WHEN backlog_return_flag IN ('Backlog Confirm Return') AND aging_backlog_confirm BETWEEN 8 AND 14 THEN "8-14 Days"
    WHEN backlog_return_flag IN ('Backlog Confirm Return') AND aging_backlog_confirm BETWEEN 15 AND 30 THEN "15-30 Days"
    WHEN backlog_return_flag IN ('Backlog Confirm Return') AND aging_backlog_confirm >30 THEN ">30 Days"
    END AS backlog_confirm_category,
CASE 
    WHEN backlog_return_flag IN ('Backlog Return') AND aging_backlog_return <3 THEN "<3 Days"
    WHEN backlog_return_flag IN ('Backlog Return') AND aging_backlog_return BETWEEN 3 AND 7 THEN "3-7 Days"
    WHEN backlog_return_flag IN ('Backlog Return') AND aging_backlog_return BETWEEN 8 AND 14 THEN "8-14 Days"
    WHEN backlog_return_flag IN ('Backlog Return') AND aging_backlog_return BETWEEN 15 AND 30 THEN "15-30 Days"
    WHEN backlog_return_flag IN ('Backlog Return') AND aging_backlog_return >30 THEN ">30 Days"
    END AS backlog_return_category,


FROM join_return
)

-- WHERE aging_backlog_return = 8
