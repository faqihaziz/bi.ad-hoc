WITH

                    retur AS (
                    SELECT waybill_no, return_confirm_status, return_waybill_no, DATE(update_time, 'Asia/Jakarta') update_time, DATETIME(return_confirm_record_time, 'Asia/Jakarta') AS return_confirm_time
                    FROM `datawarehouse_idexp.waybill_return_bill`
                    WHERE return_confirm_status = '01'
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY waybill_no ORDER BY update_time DESC)=1
                    )

, waybill AS (
    SELECT

    ww.ecommerce_order_no,
    ww.waybill_no,
    -- code1.mh_name AS mh_origin,
    code2.mh_name AS mh_destination_name,
    ww.pickup_branch_name,
    ww.delivery_branch_name,
    DATETIME(ww.delivery_record_time,'Asia/Jakarta') delivery_time,
    DATETIME(ww.pod_record_time,'Asia/Jakarta') pod_time,
    DATETIME(ww.shipping_time,'Asia/Jakarta') shipping_time,

FROM `datawarehouse_idexp.waybill_waybill` ww
LEFT JOIN `datamart_idexp.a_mh_code_name` code2 on REGEXP_SUBSTR(ww.sorting_code, '[^-]+') = code2.mh_code --mh_dest
-- LEFT JOIN `datawarehouse_idexp.res_area` t1 ON t1.name = ww.sender_district_name AND t1.city_name = ww.sender_city_name
-- LEFT JOIN `datamart_idexp.a_mh_code_name` code1 on REGEXP_SUBSTR(t1.sorting_code, '[^-]+') = code1.mh_code
WHERE DATE(ww.shipping_time,'Asia/Jakarta') BETWEEN '2023-09-01' AND '2023-10-30'
)

, arrival_origin AS (SELECT
    wl.waybill_no,
    wl.operation_branch_name AS mh_origin,
    DATETIME(wl.record_time, 'Asia/Jakarta') AS arrival_origin

FROM `datawarehouse_idexp.waybill_waybill_line` wl

WHERE DATE(record_time,'Asia/Jakarta') BETWEEN '2023-09-01' AND '2023-10-30'
    --   AND operation_type = '05'
      AND SUBSTR(operation_branch_name,1,2) IN ('MH','DC')
      -- AND SUBSTR(previous_branch_name,1,2) = 'TH'

QUALIFY ROW_NUMBER() OVER (PARTITION BY wl.waybill_no ORDER BY wl.record_time ASC)=1
)
,all_arrival AS (SELECT
    wl.waybill_no,
    wl.operation_branch_name AS operation_branch_name,
    DATETIME(wl.record_time, 'Asia/Jakarta') AS arrival_time

FROM `datawarehouse_idexp.waybill_waybill_line` wl

WHERE DATE(record_time,'Asia/Jakarta') BETWEEN '2023-09-01' AND '2023-10-30'
      AND operation_type = '05'
      -- AND SUBSTR(operation_branch_name,1,2) IN ('MH','DC')
      -- AND SUBSTR(previous_branch_name,1,2) = 'TH'

QUALIFY ROW_NUMBER() OVER (PARTITION BY wl.waybill_no,wl.operation_branch_name ORDER BY wl.waybill_no, wl.record_time ASC)=1
)
,sending_th_ori AS (SELECT
    wl.waybill_no,
    wl.operation_branch_name AS operation_branch_name,
    DATETIME(wl.record_time, 'Asia/Jakarta') AS sending_time,

FROM `datawarehouse_idexp.waybill_waybill_line` wl

WHERE DATE(record_time,'Asia/Jakarta') BETWEEN '2023-09-01' AND '2023-10-30'
      AND operation_type = '04'
      AND SUBSTR(next_location_name,1,2) IN ('MH','DC')
      AND SUBSTR(operation_branch_name,1,2) = 'TH'

QUALIFY ROW_NUMBER() OVER (PARTITION BY wl.waybill_no,wl.operation_branch_name ORDER BY wl.waybill_no, wl.record_time ASC)=1
)
, sending_ori AS (SELECT
    wl.waybill_no,
    wl.operation_branch_name AS operation_branch_name,
    DATETIME(wl.record_time, 'Asia/Jakarta') AS sending_time,

FROM `datawarehouse_idexp.waybill_waybill_line` wl

WHERE DATE(record_time,'Asia/Jakarta') BETWEEN '2023-09-01' AND '2023-10-30'
      AND operation_type = '04'
      AND SUBSTR(operation_branch_name,1,2) IN ('MH','DC')
      -- AND SUBSTR(next_location_name,1,2) IN ('MH','DC')
      
  QUALIFY ROW_NUMBER() OVER (PARTITION BY wl.waybill_no ORDER BY wl.waybill_no, wl.record_time ASC)=1
)

, sending_dest AS (SELECT
    wl.waybill_no,
    wl.operation_branch_name AS operation_branch_name,
    DATETIME(wl.record_time, 'Asia/Jakarta') AS sending_time,

FROM `datawarehouse_idexp.waybill_waybill_line` wl

WHERE DATE(record_time,'Asia/Jakarta') BETWEEN '2023-09-01' AND '2023-10-30'
      AND operation_type = '04'
      AND SUBSTR(operation_branch_name,1,2) IN ('MH','DC')
      AND SUBSTR(next_location_name,1,2) IN ('TH','VH','VT')
      
  QUALIFY ROW_NUMBER() OVER (PARTITION BY wl.waybill_no ORDER BY wl.record_time DESC)=1
),

first_deliv_attempt AS (

SELECT
sc.waybill_no,
MIN(DATETIME(sc.record_time,'Asia/Jakarta')) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) AS first_deliv_attempt,
MIN(rd16.option_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) AS scan_type,

FROM `datawarehouse_idexp.waybill_waybill_line` sc 
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd16 ON rd16.option_value = sc.operation_type AND rd16.type_option = 'operationType'

WHERE 
DATE(sc.record_time,'Asia/Jakarta') BETWEEN '2023-09-01' AND '2023-10-30' -->= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))
AND operation_type = "09"

QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC)=1

),

first_pos as(
  SELECT
        ps.waybill_no,
        MIN(ps.problem_reason) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time ASC) first_pos_reason,
        MIN(DATETIME(ps.operation_time,'Asia/Jakarta')) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time ASC) AS first_pos_attempt,
        MIN(prt.option_name) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time ASC) first_pos_type,
        MIN(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) AS first_pos_location,
        MIN(sc.photo_url) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) AS first_pos_photo_url,

              FROM `datawarehouse_idexp.waybill_problem_piece` ps
              LEFT OUTER JOIN `datawarehouse_idexp.waybill_waybill_line` sc ON ps.waybill_no = sc.waybill_no
              AND DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY)) AND sc.operation_type IN ('18') AND sc.problem_type NOT IN ('02')
              
              LEFT join `grand-sweep-324604.datawarehouse_idexp.res_problem_package` t4 on sc.problem_code = t4.code and t4.deleted = '0'
              LEFT OUTER JOIN `datawarehouse_idexp.system_option` t1 ON t1.option_value = sc.problem_type AND t1.type_option = 'problemType'
              LEFT OUTER JOIN `datawarehouse_idexp.system_option` prt ON ps.problem_type  = prt.option_value AND prt.type_option = 'problemType'
              WHERE DATE(ps.operation_time,'Asia/Jakarta') BETWEEN '2023-09-01' AND '2023-10-30' -->= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))

              AND ps.problem_type NOT IN ('02')

              QUALIFY ROW_NUMBER() OVER (PARTITION BY ps.waybill_no ORDER BY sc.record_time ASC)=1
        )


, detail AS (
        SELECT 
              t1.ecommerce_order_no,
              t1.waybill_no,
              t1.pickup_branch_name,
              t1.shipping_time,
            --   t6.sending_time AS sending_th_ori,
              t7.mh_origin,
              t7.arrival_origin AS arrival_mh_ori,
              -- t1.arrival_origin,
            --   t2.sending_time AS sending_mh_ori,
              t1.mh_destination_name,
              t3.arrival_time AS arrival_mh_dest,
            --   IF(t3.arrival_time > t4.sending_time, NULL, t4.sending_time)  AS sending_mh_dest,
              t1.delivery_branch_name,
              t5.arrival_time AS arrival_th_dest,
            --   t1.delivery_time,
            fd.first_deliv_attempt,
            fp.first_pos_attempt,
              t1.pod_time,
            --   IF(t4.sending_time)
                          
        FROM waybill t1
        LEFT JOIN arrival_origin t7 ON t7.waybill_no = t1.waybill_no
        LEFT JOIN sending_th_ori t6 ON t6.waybill_no = t1.waybill_no --AND t1.pickup_branch_name = t6.operation_branch_name
        LEFT JOIN sending_ori t2 ON t2.waybill_no = t1.waybill_no AND t2.operation_branch_name = t7.mh_origin
        LEFT JOIN all_arrival t3 ON t3.waybill_no = t1.waybill_no AND t3.operation_branch_name = t1.mh_destination_name
        LEFT JOIN sending_dest t4 ON t4.waybill_no = t1.waybill_no --AND t4.operation_branch_name = t1.mh_destination_name
        LEFT JOIN all_arrival t5 ON t5.waybill_no = t1.waybill_no AND t5.operation_branch_name = t1.delivery_branch_name
        LEFT JOIN first_deliv_attempt fd ON fd.waybill_no = t1.waybill_no
        LEFT JOIN first_pos fp ON fp.waybill_no = t1.waybill_no

        INNER JOIN `dev_idexp.awb_tokped_ops` ww ON ww.order_no = t1.ecommerce_order_no

)

SELECT 
        *,
        -- ecommerce_order_no,
        -- waybill_no,
        -- pickup_branch_name,
        -- mh_origin,
        -- mh_destination_name,
        -- delivery_branch_name,
        DATETIME_DIFF(arrival_mh_dest,arrival_mh_ori, HOUR) AS mh_ori_to_mh_dest,
        DATETIME_DIFF(arrival_th_dest,arrival_mh_dest, HOUR) AS mh_dest_to_th_dest,
        DATETIME_DIFF(first_pos_attempt,arrival_th_dest, HOUR) AS arr_th_dest_to_pos,
        DATETIME_DIFF(pod_time,arrival_th_dest, HOUR) AS th_dest_to_pod,

FROM detail
