-- Query untuk dev >> dummy_scan_record_vm  
-- misahin waybill yg return dan nggak return, kalau yg return hanya munculin scan sebelum return_confirm_record_time

WITH
scan_record_main AS (

SELECT
            currenttab.vehicle_tag_no,
            currenttab.waybill_no,
            currenttab.bag_no,
            option.option_name AS operation_type,
            currenttab.operation_branch_name AS operation_branch_name,
            DATETIME(currenttab.record_time,'Asia/Jakarta') AS record_time,
            LAG(currenttab.operation_branch_name,1) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS previous_branch_name,
            LEAD(currenttab.operation_branch_name) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_location_name,
            LEAD(currenttab.operation_branch_name,1) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_location_name_2,
            LEAD(currenttab.operation_branch_name,2) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_location_name_3,
            LEAD(currenttab.operation_branch_name,3) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_location_name_4,
            LEAD(currenttab.operation_branch_name,4) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_location_name_5,
            
            -- FROM `dev_idexp.dummy_count_awb_and_weight_from_vm` sc
            -- LEFT JOIN 
            FROM
                `datawarehouse_idexp.dm_waybill_waybill_line` AS currenttab --ON sc.vehicle_tag_no = currenttab.vehicle_tag_no

                -- AND DATE(currenttab.record_time,'Asia/Jakarta') BETWEEN '2023-10-01' AND '2024-02-20' 

                LEFT JOIN `datawarehouse_idexp.system_option` AS option ON currenttab.operation_type = option.option_value AND option.type_option = 'operationType'
                -- LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON currenttab.waybill_no = rr.waybill_no
                -- LEFT OUTER JOIN waybill_to_return rr ON currenttab.waybill_no = rr.waybill_no
                                                
            WHERE DATE(currenttab.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -65 DAY))
            -- WHERE DATE(currenttab.record_time,'Asia/Jakarta') BETWEEN '2024-01-01' AND '2024-02-20' 
            -- AND DATE(currenttab.record_time,'Asia/Jakarta') < DATE(rr.return_confirm_record_time)
            AND currenttab.deleted = '0'

            -- AND currenttab.vehicle_tag_no IN (
--  "VF1130038744"
               AND currenttab.waybill_no IN (
"IDS900334113382"
-- "IDS900549017234"
)

            ORDER BY record_time DESC
),

return_data AS (

SELECT
    waybill_no,
    DATETIME(return_confirm_record_time,'Asia/Jakarta') return_confirm_record_time,

FROM `datawarehouse_idexp.waybill_return_bill` rr
WHERE DATE(rr.return_confirm_record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -65 DAY))
),

scan_record_fwd AS (

    SELECT * FROM (

        SELECT
    
    sc.waybill_no,
    sc.vehicle_tag_no,
    sc.bag_no,
    sc.operation_type,
    sc.operation_branch_name,
    sc.record_time,
    sc.previous_branch_name,
    sc.next_location_name,
    sc.next_location_name_2,
    sc.next_location_name_3,
    sc.next_location_name_4,
    sc.next_location_name_5,
    CASE WHEN rr.waybill_no IS NULL THEN "return waybill" 
    WHEN rr.waybill_no IS NOT NULL THEN "non-return"
    END AS return_or_not,
    
    FROM scan_record_main sc
    -- LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON sc.waybill_no = rr.waybill_no
    LEFT JOIN return_data rr ON sc.waybill_no = rr.waybill_no
    -- WHERE record_time <= rr.return_confirm_record_time
    AND rr.return_confirm_record_time IS NULL
)
-- WHERE return_or_not = "non-return"
),

scan_record_return AS (

    SELECT * FROM (

        SELECT
    
    sc.waybill_no,
    sc.vehicle_tag_no,
    sc.bag_no,
    sc.operation_type,
    sc.operation_branch_name,
    sc.record_time,
    sc.previous_branch_name,
    sc.next_location_name,
    sc.next_location_name_2,
    sc.next_location_name_3,
    sc.next_location_name_4,
    sc.next_location_name_5,
    CASE WHEN rr.waybill_no IS NULL THEN "return waybill" 
    WHEN rr.waybill_no IS NOT NULL THEN "non-return"
    END AS return_or_not,

    
    FROM scan_record_main sc
    -- LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON sc.waybill_no = rr.waybill_no
    LEFT JOIN return_data rr ON sc.waybill_no = rr.waybill_no
        
    -- WHERE DATE(record_time) < DATE(rr.return_confirm_record_time)
    WHERE record_time < rr.return_confirm_record_time
    -- WHERE DATE(rr.return_confirm_record_time) > DATE(record_time)
    AND rr.return_confirm_record_time IS NOT NULL

)
-- WHERE return_or_not = "return waybill" 
),

join_scan_record AS (

SELECT * FROM (

SELECT * FROM scan_record_fwd UNION ALL
SELECT * FROM scan_record_return
)
-- QUALIFY ROW_NUMBER() OVER(PARTITION BY record_time)=1
)

SELECT * FROM join_scan_record
-- SELECT * FROM scan_record_main
-- WHERE vehicle_tag_no IS NOT NULL
-- WHERE vehicle_tag_no IN (

-- )

ORDER BY record_time DESC
